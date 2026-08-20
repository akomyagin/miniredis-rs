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
    // TODO(Этап 3): remove the allow once expiry logic reads this field.
    #[allow(dead_code)]
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

    /// GET — returns the value if present and not expired.
    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        // TODO(Этап 3): treat an expired key as absent (lazy expiration).
        // TODO(Этап 4): move the touched key to the LRU front.
        self.map.get(key).map(|e| e.value.clone())
    }

    /// SET — insert or overwrite a key, optionally with a TTL.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>, expires_at: Option<Instant>) {
        // TODO(Этап 4): push to the LRU front and evict if over capacity.
        self.map.insert(key, Entry { value, expires_at });
    }

    /// DEL — remove a key, returning whether it existed.
    pub fn del(&mut self, key: &[u8]) -> bool {
        // TODO(Этап 4): unlink from the LRU list.
        self.map.remove(key).is_some()
    }

    /// Reap all currently-expired keys. Called by the background sweeper thread.
    // TODO(Этап 3): remove the allow once the sweeper thread calls this.
    #[allow(dead_code)]
    pub fn sweep_expired(&mut self) {
        // TODO(Этап 3): iterate and drop entries whose expires_at is in the past.
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
}
