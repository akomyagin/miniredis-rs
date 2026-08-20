//! In-memory key/value store with TTL and O(1) LRU eviction.
//!
//! Built incrementally across stages:
//!   - Этап 2: plain `HashMap<Vec<u8>, Entry>` behind a `Mutex`, GET/SET/DEL.
//!   - Этап 3: per-key expiry (`expires_at`), lazy expiration on access + a background
//!     sweeper thread that reaps expired keys.
//!   - Этап 4: O(1) LRU eviction — a doubly-linked list over a slab arena (`Vec<Node>` +
//!     `usize` indices, no `unsafe`, no raw pointers) threaded through the map's entries,
//!     so both `get` (touch) and `evict` (drop the least-recently-used) are O(1).
//!     Decision (slab arena over the `lru` crate) recorded in docs/TECHNICAL_PLAN.md, Этап 4.

use std::collections::HashMap;
use std::time::Instant;

/// Sentinel meaning "no node" instead of `Option<usize>` in the hot link fields —
/// `usize::MAX` is never a valid arena index.
const NIL: usize = usize::MAX;

/// One slot in the LRU arena. `prev`/`next` are indices into `LruList::nodes`, or `NIL`.
#[derive(Debug)]
struct Node {
    /// Duplicated key so that eviction can remove the entry from the `HashMap`.
    key: Vec<u8>,
    prev: usize,
    next: usize,
}

/// Doubly-linked list over a Vec arena: O(1) touch (move-to-front) and O(1) eviction
/// (pop the tail = least-recently-used), without `unsafe` or raw pointers — indices
/// instead of references.
#[derive(Debug)]
struct LruList {
    nodes: Vec<Node>,
    /// Most-recently-used; `NIL` if empty.
    head: usize,
    /// Least-recently-used; `NIL` if empty.
    tail: usize,
    /// Reusable slot indices.
    free: Vec<usize>,
}

impl Default for LruList {
    fn default() -> Self {
        Self::new()
    }
}

impl LruList {
    fn new() -> Self {
        Self {
            nodes: Vec::new(),
            head: NIL,
            tail: NIL,
            free: Vec::new(),
        }
    }

    /// Inserts a new node for `key` at the head (most-recently-used). Returns the slot
    /// index — store it in the `Entry` for O(1) future touch/remove.
    fn push_front(&mut self, key: Vec<u8>) -> usize {
        let idx = match self.free.pop() {
            Some(i) => {
                self.nodes[i] = Node {
                    key,
                    prev: NIL,
                    next: self.head,
                };
                i
            }
            None => {
                self.nodes.push(Node {
                    key,
                    prev: NIL,
                    next: self.head,
                });
                self.nodes.len() - 1
            }
        };
        if self.head != NIL {
            self.nodes[self.head].prev = idx;
        }
        self.head = idx;
        if self.tail == NIL {
            self.tail = idx;
        }
        idx
    }

    /// Unlinks `idx` from wherever it is and relinks it at the head. O(1).
    fn move_to_front(&mut self, idx: usize) {
        if self.head == idx {
            return; // already MRU
        }
        self.unlink(idx);
        self.nodes[idx].prev = NIL;
        self.nodes[idx].next = self.head;
        if self.head != NIL {
            self.nodes[self.head].prev = idx;
        }
        self.head = idx;
        if self.tail == NIL {
            self.tail = idx;
        }
    }

    /// Removes `idx` from the list and releases the slot for reuse. O(1).
    fn remove(&mut self, idx: usize) {
        self.unlink(idx);
        self.nodes[idx].key.clear();
        self.free.push(idx);
    }

    /// Evicts and returns the tail key (least-recently-used), if any. O(1).
    fn evict(&mut self) -> Option<Vec<u8>> {
        if self.tail == NIL {
            return None;
        }
        let idx = self.tail;
        let key = std::mem::take(&mut self.nodes[idx].key);
        self.unlink(idx);
        self.free.push(idx);
        Some(key)
    }

    /// Internal: rewires the neighbours' prev/next to bypass `idx`, updating head/tail
    /// if `idx` was at either end. Does not touch `idx`'s own prev/next and does not
    /// release the slot.
    fn unlink(&mut self, idx: usize) {
        let (prev, next) = (self.nodes[idx].prev, self.nodes[idx].next);
        match prev {
            NIL => self.head = next,
            p => self.nodes[p].next = next,
        }
        match next {
            NIL => self.tail = prev,
            n => self.nodes[n].prev = prev,
        }
    }

    /// Test-only observability: number of live (non-free) slots in the arena.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.nodes.len() - self.free.len()
    }

    /// Test-only observability: walks the list from `head` to `NIL` and returns the keys
    /// in MRU→LRU order. Panics on broken links or cycles (walk longer than the arena),
    /// which is exactly what the consistency tests want to catch.
    #[cfg(test)]
    fn keys_mru_to_lru(&self) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        let mut idx = self.head;
        let mut prev = NIL;
        while idx != NIL {
            assert!(keys.len() <= self.nodes.len(), "cycle detected in LRU list");
            assert_eq!(self.nodes[idx].prev, prev, "broken back-link at node {idx}");
            keys.push(self.nodes[idx].key.clone());
            prev = idx;
            idx = self.nodes[idx].next;
        }
        assert_eq!(self.tail, prev, "tail does not match end of forward walk");
        keys
    }
}

/// A single stored value plus its metadata.
#[derive(Debug, Clone)]
pub struct Entry {
    pub value: Vec<u8>,
    /// Absolute expiry instant; `None` means the key never expires.
    pub expires_at: Option<Instant>,
    /// Index into `Store::lru.nodes` — private, not part of the public API.
    lru_idx: usize,
}

/// The shared KV store. Wrapped in `Arc<Mutex<Store>>` (or finer-grained locking, TBD in
/// Этап 5) and shared across connection-handler threads.
#[derive(Default)]
pub struct Store {
    map: HashMap<Vec<u8>, Entry>,
    lru: LruList,
    /// `None` = unlimited (default, backwards compatible with Этапы 2-3).
    capacity: Option<usize>,
}

impl Store {
    pub fn new() -> Self {
        Self::default()
    }

    /// Store with a bounded capacity; when full, the least-recently-used key is evicted
    /// before a new key is inserted.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity: Some(capacity),
            ..Self::default()
        }
    }

    /// GET — returns the value if present and not expired. An expired key is treated as
    /// absent and physically removed on access (lazy expiration). A successful GET is an
    /// LRU touch: the key becomes most-recently-used.
    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if self.is_expired(key) {
            self.remove_internal(key);
            return None;
        }
        let idx = self.map.get(key)?.lru_idx;
        self.lru.move_to_front(idx);
        self.map.get(key).map(|e| e.value.clone())
    }

    /// SET — insert or overwrite a key, optionally with a TTL. Counts as an LRU touch.
    /// When at capacity, inserting a *new* key evicts the least-recently-used one first.
    ///
    /// Deliberate design decision (docs/TECHNICAL_PLAN.md, Этап 4): `set` does NOT lazily
    /// expire other keys before counting occupancy — an expired-but-unswept key still
    /// takes a slot and is evicted by LRU order, not TTL order. TTL cleanup is the job of
    /// the sweeper and of lazy expiration on `get`, not of `set`.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>, expires_at: Option<Instant>) {
        if let Some(existing) = self.map.get_mut(&key) {
            existing.value = value;
            existing.expires_at = expires_at;
            let idx = existing.lru_idx;
            self.lru.move_to_front(idx);
            return;
        }

        if let Some(cap) = self.capacity {
            if self.map.len() >= cap {
                self.evict_one();
            }
        }

        let idx = self.lru.push_front(key.clone());
        self.map.insert(
            key,
            Entry {
                value,
                expires_at,
                lru_idx: idx,
            },
        );
    }

    /// DEL — remove a key, returning whether it existed. An expired key counts as absent
    /// (lazy expiration): it is dropped, but `false` is returned.
    pub fn del(&mut self, key: &[u8]) -> bool {
        if self.is_expired(key) {
            self.remove_internal(key);
            return false;
        }
        self.remove_internal(key)
    }

    /// Removes a key from *both* the map and the LRU arena, keeping them in sync.
    /// Every physical removal must go through here — dropping a map entry without
    /// unlinking its arena node would desync the free list from the map.
    fn remove_internal(&mut self, key: &[u8]) -> bool {
        match self.map.remove(key) {
            Some(entry) => {
                self.lru.remove(entry.lru_idx);
                true
            }
            None => false,
        }
    }

    /// Drops the least-recently-used key from both structures. O(1).
    fn evict_one(&mut self) {
        if let Some(key) = self.lru.evict() {
            self.map.remove(&key);
        }
    }

    /// TTL support: seconds until expiry, Redis semantics.
    ///   -2 => no such key (or already expired, treated as absent)
    ///   -1 => key exists but has no TTL set
    ///    n => seconds remaining (rounded up, like Redis — never under-reports: a key
    ///         400ms from expiry reports 1, not 0)
    pub fn ttl_secs(&mut self, key: &[u8]) -> i64 {
        if self.is_expired(key) {
            self.remove_internal(key);
            return -2;
        }
        match self.map.get(key) {
            None => -2,
            Some(Entry {
                expires_at: None, ..
            }) => -1,
            Some(Entry {
                expires_at: Some(at),
                ..
            }) => {
                let now = Instant::now();
                if *at <= now {
                    -2 // should not happen (is_expired caught it above); defensive
                } else {
                    let remaining = at.duration_since(now);
                    let secs = remaining.as_secs() as i64;
                    let has_subsecond = remaining.subsec_nanos() > 0;
                    if has_subsecond {
                        secs + 1
                    } else {
                        secs.max(1)
                    }
                }
            }
        }
    }

    /// EXPIRE — set/overwrite the TTL of an existing key. Returns `false` if the key does
    /// not exist (or has already expired).
    pub fn expire(&mut self, key: &[u8], expires_at: Instant) -> bool {
        if self.is_expired(key) {
            self.remove_internal(key);
            return false;
        }
        match self.map.get_mut(key) {
            Some(entry) => {
                entry.expires_at = Some(expires_at);
                true
            }
            None => false,
        }
    }

    fn is_expired(&self, key: &[u8]) -> bool {
        match self.map.get(key) {
            Some(Entry {
                expires_at: Some(at),
                ..
            }) => *at <= Instant::now(),
            _ => false,
        }
    }

    /// Reap all currently-expired keys. Called by the background sweeper thread.
    ///
    /// Этап 4 note: the Этап 3 implementation used `self.map.retain(...)`, but now every
    /// removal must also unlink the key's LRU arena node, and `retain` cannot borrow
    /// `self.lru` inside its closure while `self.map` is borrowed. So expired keys are
    /// collected into an intermediate `Vec` first, then removed via `remove_internal`,
    /// which cleans both structures consistently. Still O(n) per sweep, which is fine —
    /// a sweep walks the whole store by construction.
    pub fn sweep_expired(&mut self) {
        let now = Instant::now();
        let expired_keys: Vec<Vec<u8>> = self
            .map
            .iter()
            .filter(|(_, e)| matches!(e.expires_at, Some(at) if at <= now))
            .map(|(k, _)| k.clone())
            .collect();
        for k in expired_keys {
            self.remove_internal(&k);
        }
    }

    /// Test-only observability: number of physically stored entries, expired or not.
    /// Lets tests prove that `sweep_expired()` itself removed a key, as opposed to a
    /// later lazy-expiring access.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.map.len()
    }

    /// Test-only observability: number of live LRU arena nodes. The core Этап 4
    /// invariant is `self.len() == self.lru_len()` at all times.
    #[cfg(test)]
    fn lru_len(&self) -> usize {
        self.lru.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_missing_key_returns_none() {
        let mut store = Store::new();
        assert_eq!(store.get(b"absent"), None);
    }

    #[test]
    fn set_then_get_round_trip() {
        let mut store = Store::new();
        store.set(b"foo".to_vec(), b"bar".to_vec(), None);
        assert_eq!(store.get(b"foo"), Some(b"bar".to_vec()));
    }

    #[test]
    fn set_overwrites_existing_key() {
        let mut store = Store::new();
        store.set(b"foo".to_vec(), b"old".to_vec(), None);
        store.set(b"foo".to_vec(), b"new".to_vec(), None);
        assert_eq!(store.get(b"foo"), Some(b"new".to_vec()));
    }

    #[test]
    fn del_existing_and_missing() {
        let mut store = Store::new();
        store.set(b"foo".to_vec(), b"bar".to_vec(), None);
        assert!(store.del(b"foo"));
        assert!(!store.del(b"foo"));
        assert_eq!(store.get(b"foo"), None);
    }

    #[test]
    fn binary_keys_and_values_pass_through() {
        let mut store = Store::new();
        let key = vec![0x00, 0xff, 0x80];
        let value = vec![0xde, 0xad, 0x00, 0xbe, 0xef];
        store.set(key.clone(), value.clone(), None);
        assert_eq!(store.get(&key), Some(value));
    }

    // --- TTL (Этап 3) ---

    use std::time::Duration;

    #[test]
    fn ttl_of_key_without_expiry_is_minus_one() {
        let mut store = Store::new();
        store.set(b"foo".to_vec(), b"bar".to_vec(), None);
        assert_eq!(store.ttl_secs(b"foo"), -1);
    }

    #[test]
    fn ttl_of_missing_key_is_minus_two() {
        let mut store = Store::new();
        assert_eq!(store.ttl_secs(b"absent"), -2);
    }

    #[test]
    fn get_with_future_expiry_returns_value() {
        let mut store = Store::new();
        store.set(
            b"foo".to_vec(),
            b"bar".to_vec(),
            Some(Instant::now() + Duration::from_secs(60)),
        );
        assert_eq!(store.get(b"foo"), Some(b"bar".to_vec()));
    }

    #[test]
    fn get_expired_key_returns_none_and_removes_it() {
        let mut store = Store::new();
        // `Instant::now()` as the expiry plus a short sleep — never subtract from an
        // Instant (underflow panics are platform-dependent).
        store.set(b"foo".to_vec(), b"bar".to_vec(), Some(Instant::now()));
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(store.get(b"foo"), None);
        assert_eq!(store.len(), 0, "expired key must be physically removed");
        assert_eq!(store.lru_len(), 0, "arena node must be unlinked too");
    }

    #[test]
    fn del_expired_key_reports_absent() {
        let mut store = Store::new();
        store.set(b"foo".to_vec(), b"bar".to_vec(), Some(Instant::now()));
        std::thread::sleep(Duration::from_millis(5));
        assert!(!store.del(b"foo"));
        assert_eq!(store.len(), 0);
        assert_eq!(store.lru_len(), 0);
    }

    #[test]
    fn expire_on_existing_key_returns_true_and_sets_ttl() {
        let mut store = Store::new();
        store.set(b"foo".to_vec(), b"bar".to_vec(), None);
        assert!(store.expire(b"foo", Instant::now() + Duration::from_secs(100)));
        let ttl = store.ttl_secs(b"foo");
        assert!((1..=100).contains(&ttl), "ttl was {ttl}");
    }

    #[test]
    fn expire_on_missing_key_returns_false() {
        let mut store = Store::new();
        assert!(!store.expire(b"absent", Instant::now() + Duration::from_secs(100)));
    }

    #[test]
    fn expire_on_already_expired_key_returns_false() {
        let mut store = Store::new();
        store.set(b"foo".to_vec(), b"bar".to_vec(), Some(Instant::now()));
        std::thread::sleep(Duration::from_millis(5));
        assert!(!store.expire(b"foo", Instant::now() + Duration::from_secs(100)));
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn ttl_of_expired_key_is_minus_two_and_removes_it() {
        let mut store = Store::new();
        store.set(b"foo".to_vec(), b"bar".to_vec(), Some(Instant::now()));
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(store.ttl_secs(b"foo"), -2);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn sweep_expired_removes_only_expired_keys() {
        let mut store = Store::new();
        store.set(b"no-ttl".to_vec(), b"a".to_vec(), None);
        store.set(
            b"future".to_vec(),
            b"b".to_vec(),
            Some(Instant::now() + Duration::from_secs(60)),
        );
        store.set(b"past".to_vec(), b"c".to_vec(), Some(Instant::now()));
        std::thread::sleep(Duration::from_millis(5));

        store.sweep_expired();

        assert_eq!(store.len(), 2, "only the expired key must be swept");
        assert_eq!(store.get(b"no-ttl"), Some(b"a".to_vec()));
        assert_eq!(store.get(b"future"), Some(b"b".to_vec()));
        assert_eq!(store.get(b"past"), None);
    }

    // Timing-dependent: the key expires and is reaped by sweep_expired() alone — no GET
    // ever touches it, so lazy expiration cannot be what removed it. Generous margin
    // (100ms lifetime, 400ms sleep) keeps this stable in CI.
    #[test]
    fn sweep_expired_reaps_key_without_any_access() {
        let mut store = Store::new();
        store.set(
            b"doomed".to_vec(),
            b"x".to_vec(),
            Some(Instant::now() + Duration::from_millis(100)),
        );
        assert_eq!(store.len(), 1);
        std::thread::sleep(Duration::from_millis(400));
        store.sweep_expired();
        assert_eq!(
            store.len(),
            0,
            "sweeper must reap the expired key by itself"
        );
    }

    #[test]
    fn ttl_secs_rounds_up() {
        let mut store = Store::new();
        store.set(
            b"foo".to_vec(),
            b"bar".to_vec(),
            Some(Instant::now() + Duration::from_millis(1500)),
        );
        // 1.5s remaining must round up to 2, never down to 1.
        assert_eq!(store.ttl_secs(b"foo"), 2);
    }

    // --- LruList in isolation (Этап 4) ---

    #[test]
    fn lru_list_evicts_in_lru_order() {
        let mut list = LruList::new();
        list.push_front(b"a".to_vec());
        list.push_front(b"b".to_vec());
        list.push_front(b"c".to_vec());
        // Insertion order a, b, c => a is the oldest (tail), evicted first.
        assert_eq!(list.evict(), Some(b"a".to_vec()));
        assert_eq!(list.evict(), Some(b"b".to_vec()));
        assert_eq!(list.evict(), Some(b"c".to_vec()));
        assert_eq!(list.evict(), None);
    }

    #[test]
    fn lru_list_move_to_front_changes_eviction_candidate() {
        let mut list = LruList::new();
        let a = list.push_front(b"a".to_vec());
        list.push_front(b"b".to_vec());
        list.push_front(b"c".to_vec());
        // Touch "a": it becomes MRU, so "b" is now the eviction candidate.
        list.move_to_front(a);
        assert_eq!(
            list.keys_mru_to_lru(),
            vec![b"a".to_vec(), b"c".to_vec(), b"b".to_vec()]
        );
        assert_eq!(list.evict(), Some(b"b".to_vec()));
        assert_eq!(list.evict(), Some(b"c".to_vec()));
        assert_eq!(list.evict(), Some(b"a".to_vec()));
    }

    #[test]
    fn lru_list_move_to_front_on_head_is_noop() {
        let mut list = LruList::new();
        list.push_front(b"a".to_vec());
        let b = list.push_front(b"b".to_vec());
        list.move_to_front(b);
        assert_eq!(list.keys_mru_to_lru(), vec![b"b".to_vec(), b"a".to_vec()]);
        assert_eq!(list.evict(), Some(b"a".to_vec()));
    }

    #[test]
    fn lru_list_remove_middle_relinks_neighbours() {
        let mut list = LruList::new();
        list.push_front(b"a".to_vec());
        let b = list.push_front(b"b".to_vec());
        list.push_front(b"c".to_vec());
        list.remove(b);
        // Walk validates both forward and back links around the removed node.
        assert_eq!(list.keys_mru_to_lru(), vec![b"c".to_vec(), b"a".to_vec()]);
        assert_eq!(list.evict(), Some(b"a".to_vec()));
        assert_eq!(list.evict(), Some(b"c".to_vec()));
        assert_eq!(list.evict(), None);
    }

    #[test]
    fn lru_list_reuses_freed_slot() {
        let mut list = LruList::new();
        let a = list.push_front(b"a".to_vec());
        list.push_front(b"b".to_vec());
        list.remove(a);
        // The freed slot must be reused: the arena must not grow.
        let c = list.push_front(b"c".to_vec());
        assert_eq!(c, a, "freed slot must be recycled");
        assert_eq!(list.nodes.len(), 2, "arena must not grow past its peak");
        assert_eq!(list.keys_mru_to_lru(), vec![b"c".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn lru_list_evict_on_empty_returns_none() {
        let mut list = LruList::new();
        assert_eq!(list.evict(), None);
        // Also after filling and draining.
        list.push_front(b"a".to_vec());
        assert_eq!(list.evict(), Some(b"a".to_vec()));
        assert_eq!(list.evict(), None);
    }

    // --- Store with bounded capacity (Этап 4) ---

    #[test]
    fn overflow_evicts_least_recently_used_key() {
        let mut store = Store::with_capacity(2);
        store.set(b"a".to_vec(), b"1".to_vec(), None);
        store.set(b"b".to_vec(), b"2".to_vec(), None);
        store.set(b"c".to_vec(), b"3".to_vec(), None); // evicts "a" (oldest, untouched)
        assert_eq!(store.get(b"a"), None, "LRU key must be evicted");
        assert_eq!(store.get(b"b"), Some(b"2".to_vec()));
        assert_eq!(store.get(b"c"), Some(b"3".to_vec()));
        assert_eq!(store.len(), 2);
        assert_eq!(store.lru_len(), 2);
    }

    #[test]
    fn get_is_a_touch_and_protects_from_eviction() {
        let mut store = Store::with_capacity(2);
        store.set(b"a".to_vec(), b"1".to_vec(), None);
        store.set(b"b".to_vec(), b"2".to_vec(), None);
        assert_eq!(store.get(b"a"), Some(b"1".to_vec())); // touch: "a" becomes MRU
        store.set(b"c".to_vec(), b"3".to_vec(), None); // evicts "b", not "a"
        assert_eq!(store.get(b"b"), None, "untouched LRU key must be evicted");
        assert_eq!(
            store.get(b"a"),
            Some(b"1".to_vec()),
            "touched key must survive"
        );
        assert_eq!(store.get(b"c"), Some(b"3".to_vec()));
    }

    #[test]
    fn set_on_existing_key_is_a_touch() {
        let mut store = Store::with_capacity(2);
        store.set(b"a".to_vec(), b"1".to_vec(), None);
        store.set(b"b".to_vec(), b"2".to_vec(), None);
        store.set(b"a".to_vec(), b"1'".to_vec(), None); // overwrite = touch: "a" MRU
        store.set(b"c".to_vec(), b"3".to_vec(), None); // evicts "b", not "a"
        assert_eq!(store.get(b"b"), None);
        assert_eq!(store.get(b"a"), Some(b"1'".to_vec()));
        assert_eq!(store.get(b"c"), Some(b"3".to_vec()));
    }

    #[test]
    fn overwrite_does_not_consume_capacity_or_evict() {
        let mut store = Store::with_capacity(2);
        store.set(b"a".to_vec(), b"1".to_vec(), None);
        store.set(b"b".to_vec(), b"2".to_vec(), None);
        // Overwriting at full capacity must not evict anything.
        store.set(b"b".to_vec(), b"2'".to_vec(), None);
        assert_eq!(store.len(), 2);
        assert_eq!(store.lru_len(), 2);
        assert_eq!(store.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(store.get(b"b"), Some(b"2'".to_vec()));
    }

    #[test]
    fn del_frees_a_capacity_slot() {
        let mut store = Store::with_capacity(2);
        store.set(b"a".to_vec(), b"1".to_vec(), None);
        store.set(b"b".to_vec(), b"2".to_vec(), None);
        assert!(store.del(b"a"));
        store.set(b"c".to_vec(), b"3".to_vec(), None); // fits in the freed slot: no eviction
        assert_eq!(
            store.get(b"b"),
            Some(b"2".to_vec()),
            "no eviction after del freed a slot"
        );
        assert_eq!(store.get(b"c"), Some(b"3".to_vec()));
        assert_eq!(store.len(), 2);
        assert_eq!(store.lru_len(), 2);
    }

    #[test]
    fn unlimited_store_never_evicts() {
        let mut store = Store::new();
        for i in 0..1000u32 {
            store.set(i.to_be_bytes().to_vec(), b"v".to_vec(), None);
        }
        assert_eq!(store.len(), 1000, "unbounded store must never evict");
        assert_eq!(store.lru_len(), 1000);
        for i in 0..1000u32 {
            assert_eq!(store.get(&i.to_be_bytes()), Some(b"v".to_vec()));
        }
    }

    // Documented behavior (docs/TECHNICAL_PLAN.md, Этап 4), not a side effect: `set` does
    // NOT lazily expire other keys before counting occupancy. An expired-but-unswept key
    // still occupies its slot and is removed by LRU eviction on the next insert, not by
    // TTL logic. TTL cleanup belongs to the sweeper and to lazy expiration on `get`.
    #[test]
    fn expired_key_at_capacity_is_evicted_by_lru_not_ttl() {
        let mut store = Store::with_capacity(1);
        store.set(b"old".to_vec(), b"x".to_vec(), Some(Instant::now()));
        std::thread::sleep(Duration::from_millis(5));
        // "old" has expired but was never swept nor read: it still holds the only slot.
        assert_eq!(store.len(), 1);
        store.set(b"new".to_vec(), b"y".to_vec(), None);
        // The insert evicted "old" via the LRU path (it was the tail), not via TTL.
        assert_eq!(store.len(), 1);
        assert_eq!(store.lru_len(), 1);
        assert_eq!(store.get(b"new"), Some(b"y".to_vec()));
        assert_eq!(store.get(b"old"), None);
    }

    // The trap the plan warns about: sweep_expired must unlink the arena node, not just
    // drop the map entry — otherwise the arena's free list desyncs from the map. Verified
    // as a multi-step set/sweep/set chain with intermediate asserts, not just a final one.
    #[test]
    fn sweep_expired_unlinks_lru_arena_nodes() {
        let mut store = Store::with_capacity(2);
        store.set(b"stay".to_vec(), b"s".to_vec(), None);
        store.set(b"die".to_vec(), b"d".to_vec(), Some(Instant::now()));
        assert_eq!(store.len(), 2);
        assert_eq!(store.lru_len(), 2);
        std::thread::sleep(Duration::from_millis(5));

        store.sweep_expired();
        assert_eq!(store.len(), 1, "sweep must drop the expired map entry");
        assert_eq!(store.lru_len(), 1, "sweep must also unlink the arena node");

        // The freed slot must be reusable and eviction order must stay coherent.
        store.set(b"next".to_vec(), b"n".to_vec(), None);
        assert_eq!(store.len(), 2);
        assert_eq!(store.lru_len(), 2);
        assert_eq!(store.lru.nodes.len(), 2, "arena must reuse the swept slot");

        // Capacity is 2 and both live keys are present; inserting a third evicts the
        // LRU one ("stay"), proving the list links survived the sweep intact.
        store.set(b"third".to_vec(), b"t".to_vec(), None);
        assert_eq!(store.get(b"stay"), None);
        assert_eq!(store.get(b"next"), Some(b"n".to_vec()));
        assert_eq!(store.get(b"third"), Some(b"t".to_vec()));
        assert_eq!(store.len(), 2);
        assert_eq!(store.lru_len(), 2);
    }

    // Property/consistency test: a deterministic few-hundred-operation sequence over a
    // small key space on a capacity-5 store. After every operation the map and the arena
    // must agree, and walking the list head→NIL must visit exactly the live keys (the
    // walk itself asserts link integrity and catches cycles).
    #[test]
    fn random_ops_keep_map_and_arena_consistent() {
        let mut store = Store::with_capacity(5);
        // Deterministic LCG, same generator as the Этап 1 fragmentation tests:
        // dev-dependencies stay empty.
        let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            (seed >> 33) as usize
        };

        for step in 0..500 {
            let key = format!("k{}", next() % 10).into_bytes();
            match next() % 3 {
                0 => store.set(key, format!("v{step}").into_bytes(), None),
                1 => {
                    store.get(&key);
                }
                _ => {
                    store.del(&key);
                }
            }

            // Invariant: map and arena agree on the live population.
            assert_eq!(
                store.len(),
                store.lru_len(),
                "map/arena desync at step {step}"
            );
            assert!(store.len() <= 5, "capacity exceeded at step {step}");

            // The list walk must visit exactly the map's keys, each exactly once
            // (the walk panics on broken links or cycles).
            let listed = store.lru.keys_mru_to_lru();
            assert_eq!(
                listed.len(),
                store.len(),
                "walk length mismatch at step {step}"
            );
            for k in &listed {
                assert!(
                    store.map.contains_key(k),
                    "arena lists a key absent from the map at step {step}"
                );
            }
        }
    }
}
