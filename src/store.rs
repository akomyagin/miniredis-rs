//! In-memory key/value store with TTL and O(1) LRU eviction.
//!
//! Built incrementally across stages:
//!   - Этап 2: plain `HashMap<Vec<u8>, Entry>` behind a `Mutex`, GET/SET/DEL.
//!   - Этап 3: per-key expiry (`expires_at`), lazy expiration on access + a background
//!     sweeper thread that reaps expired keys.
//!   - Этап 4: O(1) LRU eviction — an intrusive doubly-linked list threaded through the
//!     map's entries (or a well-justified crate; decision recorded in
//!     docs/TECHNICAL_PLAN.md, Этап 4) so both `get` (touch) and `evict`
//!     (drop the least-recently-used) are O(1), not just insertion.

use std::collections::HashMap;
use std::time::Instant;

/// A single stored value plus its metadata.
#[derive(Debug, Clone)]
pub struct Entry {
    pub value: Vec<u8>,
    /// Absolute expiry instant; `None` means the key never expires.
    pub expires_at: Option<Instant>,
    // TODO(Этап 4): intrusive LRU links (prev/next node handles) live here.
}

/// The shared KV store. Wrapped in `Arc<Mutex<Store>>` (or finer-grained locking, TBD in
/// Этап 5) and shared across connection-handler threads.
#[derive(Default)]
pub struct Store {
    map: HashMap<Vec<u8>, Entry>,
    // TODO(Этап 4): lru list head/tail + capacity for eviction.
}

impl Store {
    pub fn new() -> Self {
        Self::default()
    }

    /// GET — returns the value if present and not expired. An expired key is treated as
    /// absent and physically removed on access (lazy expiration).
    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        // TODO(Этап 4): move the touched key to the LRU front.
        if self.is_expired(key) {
            self.map.remove(key);
            return None;
        }
        self.map.get(key).map(|e| e.value.clone())
    }

    /// SET — insert or overwrite a key, optionally with a TTL.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>, expires_at: Option<Instant>) {
        // TODO(Этап 4): push to the LRU front and evict if over capacity.
        self.map.insert(key, Entry { value, expires_at });
    }

    /// DEL — remove a key, returning whether it existed. An expired key counts as absent
    /// (lazy expiration): it is dropped, but `false` is returned.
    pub fn del(&mut self, key: &[u8]) -> bool {
        // TODO(Этап 4): unlink from the LRU list.
        if self.is_expired(key) {
            self.map.remove(key);
            return false;
        }
        self.map.remove(key).is_some()
    }

    /// TTL support: seconds until expiry, Redis semantics.
    ///   -2 => no such key (or already expired, treated as absent)
    ///   -1 => key exists but has no TTL set
    ///    n => seconds remaining (rounded up, like Redis — never under-reports: a key
    ///         400ms from expiry reports 1, not 0)
    pub fn ttl_secs(&mut self, key: &[u8]) -> i64 {
        if self.is_expired(key) {
            self.map.remove(key);
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
            self.map.remove(key);
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
    pub fn sweep_expired(&mut self) {
        let now = Instant::now();
        self.map.retain(|_, entry| match entry.expires_at {
            Some(at) => at > now,
            None => true,
        });
    }

    /// Test-only observability: number of physically stored entries, expired or not.
    /// Lets tests prove that `sweep_expired()` itself removed a key, as opposed to a
    /// later lazy-expiring access.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.map.len()
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
    }

    #[test]
    fn del_expired_key_reports_absent() {
        let mut store = Store::new();
        store.set(b"foo".to_vec(), b"bar".to_vec(), Some(Instant::now()));
        std::thread::sleep(Duration::from_millis(5));
        assert!(!store.del(b"foo"));
        assert_eq!(store.len(), 0);
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
}
