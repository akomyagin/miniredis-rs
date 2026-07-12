//! In-memory key/value store with TTL and O(1) LRU eviction.
//!
//! Built incrementally across stages:
//!   - Этап 2: plain `HashMap<Vec<u8>, Entry>` behind a `Mutex`, GET/SET/DEL.
//!   - Этап 3: per-key expiry (`expires_at`), lazy expiration on access + a background
//!             sweeper thread that reaps expired keys.
//!   - Этап 4: O(1) LRU eviction — an intrusive doubly-linked list threaded through the
//!             map's entries (or a well-justified crate; decision recorded in
//!             docs/TECHNICAL_PLAN.md, Этап 4) so both `get` (touch) and `evict`
//!             (drop the least-recently-used) are O(1), not just insertion.

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
    // TODO(Этап 2): map: HashMap<Vec<u8>, Entry>,
    // TODO(Этап 4): lru list head/tail + capacity for eviction.
}

impl Store {
    pub fn new() -> Self {
        Self::default()
    }

    /// GET — returns the value if present and not expired.
    pub fn get(&mut self, _key: &[u8]) -> Option<Vec<u8>> {
        // TODO(Этап 2): map lookup.
        // TODO(Этап 3): treat an expired key as absent (lazy expiration).
        // TODO(Этап 4): move the touched key to the LRU front.
        unimplemented!("TODO(Этап 2): implement GET")
    }

    /// SET — insert or overwrite a key, optionally with a TTL.
    pub fn set(&mut self, _key: Vec<u8>, _value: Vec<u8>, _expires_at: Option<Instant>) {
        // TODO(Этап 2): insert into the map.
        // TODO(Этап 4): push to the LRU front and evict if over capacity.
        unimplemented!("TODO(Этап 2): implement SET")
    }

    /// DEL — remove a key, returning whether it existed.
    pub fn del(&mut self, _key: &[u8]) -> bool {
        // TODO(Этап 2): remove from the map.
        // TODO(Этап 4): unlink from the LRU list.
        unimplemented!("TODO(Этап 2): implement DEL")
    }

    /// Reap all currently-expired keys. Called by the background sweeper thread.
    pub fn sweep_expired(&mut self) {
        // TODO(Этап 3): iterate and drop entries whose expires_at is in the past.
    }
}
