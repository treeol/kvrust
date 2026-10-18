//! # kvr — sharded in-memory key-value store
//!
//! A lightweight, sharded, concurrent in-memory key-value store written in Rust.
//! Keys are `String` (UTF-8), values are `Vec<u8>` (arbitrary bytes). The store
//! is divided into 16 shards, each protected by a `parking_lot::RwLock`, with
//! keys distributed via `DefaultHasher` + bitmask for low contention.
//!
//! ## Features
//!
//! - **Thread-safe sharding** — 16 `RwLock`-protected `HashMap` shards; concurrent
//!   reads to different keys proceed without blocking.
//! - **Optional memory bounds** — enforce a max entry count via
//!   [`ShardedKV::with_max_entries`]; atomic CAS ensures concurrent inserts
//!   never exceed the limit.
//! - **TTL with strict read-time filtering** — entries can have an optional
//!   TTL set via [`ShardedKV::set_with_ttl`]. Contract: no key- or
//!   value-returning operation (`get`/`contains`/`del`/`ttl`/`mget`/`scan`,
//!   plus `len_active` and `collect_for_snapshot`) ever returns, counts, or
//!   persists an entry whose absolute expiry deadline (UNIX epoch ms) is at
//!   or before the time sampled by that operation — regardless of whether
//!   the background sweeper has run. Expiry is inclusive: at
//!   `now == expires_at` the entry is already expired. Physical removal
//!   (lazy purge on access, or [`ShardedKV::sweep_expired`]) is a
//!   space-reclamation side effect, never a precondition for expiry.
//!   Physical-count exceptions: `len()` and `is_empty()` include
//!   expired-but-unremoved entries, `scan` with `limit == 0` returns
//!   early without inspecting any entry, and a full store may reject
//!   new keys (`STORE_FULL`) while expired entries still hold slots.
//! - **Snapshot persistence** — save/load the entire store to a binary file with
//!   CRC32 integrity, atomic rename, and expired-entry filtering on load.
//!
//! ## Quick start
//!
//! ```
//! use kvr::ShardedKV;
//!
//! let store = ShardedKV::new();
//!
//! // Plain SET — permanent entry, no TTL.
//! assert!(store.set("key", b"value".to_vec()));
//! assert_eq!(store.get("key"), Some(b"value".to_vec()));
//!
//! // SETX — entry with a 1-hour TTL (3600000 ms).
//! assert!(store.set_with_ttl("temp", b"ephemeral".to_vec(), 3_600_000));
//! assert_eq!(store.get("temp"), Some(b"ephemeral".to_vec()));
//! ```
//!
//! See the [`ShardedKV`] type for the full API, and the repository README for
//! the binary wire protocol, configuration, and server usage.
//!
//! [`ShardedKV::with_max_entries`]: ShardedKV::with_max_entries
//! [`ShardedKV::set_with_ttl`]: ShardedKV::set_with_ttl
//! [`ShardedKV::sweep_expired`]: ShardedKV::sweep_expired

#![warn(clippy::all)]

pub mod protocol;

use parking_lot::RwLock;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const SHARDS: usize = 16;
const PER_SHARD_CAPACITY: usize = 8192;

// SHARDS must be a power of two for the bitmask to work correctly.
const _: () = assert!(SHARDS.is_power_of_two());

// ─── Expiry contract and clock (test-only override) ─────────────────────
//
// CONTRACT A — strict read-time TTL filtering.
//
// An entry is *logically absent* once `now >= expires_at` (inclusive
// boundary, millisecond resolution). Every key- or value-returning
// operation — `get`, `contains`, `del`, `ttl`, `mget`, `scan`, plus
// `len_active` and `collect_for_snapshot` — samples the current time
// itself and treats such entries exactly like missing keys, independent
// of the background sweeper (`sweep_expired` is space reclamation only,
// never a precondition for expiry).
//
// Explicitly outside the contract (physical counts, not logical reads):
// - `len()` and `is_empty()` count expired-but-unremoved entries.
// - `scan(prefix, 0, _)` returns early without sampling time or purging.
// - A store at `max_entries` may reject new keys (`STORE_FULL`) while
//   expired entries still physically occupy slots, until they are
//   removed by a read, the sweeper, or a restart-filtered restore.
//
// Wall-clock caveat: deadlines are compared against `SystemTime`
// (required for snapshot portability across restarts), so a system
// clock rollback can make an expired-but-unremoved entry appear live
// again. The contract holds relative to the operation's sampled time,
// not to irreversible real-world elapsed time.

/// The single definition of "expired" for the whole crate (see the
/// expiry-contract comment above). An entry with `expires_at == Some(exp)`
/// is expired once `now >= exp` — inclusive at the deadline, millisecond
/// resolution. `None` means permanent (never expired).
pub fn is_expired_at(expires_at: Option<u64>, now: u64) -> bool {
    expires_at.is_some_and(|exp| now >= exp)
}

fn hash_key(key: &str) -> usize {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    h.finish() as usize & (SHARDS - 1)
}

/// Returns the current time as UNIX epoch milliseconds.
///
/// Uses `SystemTime` (wall clock) rather than a monotonic clock. Wall-clock
/// semantics are accepted here because TTL expiry timestamps may need to be
/// serialized and restored across restarts (snapshot persistence), so they
/// must be comparable to real-world time.
///
/// Returns 0 if the system clock is before the UNIX epoch (extremely rare,
/// but avoids a panic in server threads that don't have `catch_unwind`).
///
/// # Tests
///
/// In the library's own unit tests (`cargo test --lib`), the time source can
/// be overridden per thread via the `cfg(test)`-only `test_support` module
/// (`freeze_clock_at`) to make TTL boundary behavior deterministic. The
/// override is `#[cfg(test)]`-only and never present in production builds,
/// integration tests, or dependent binaries.
pub fn now_ms() -> u64 {
    #[cfg(test)]
    {
        // MSRV 1.70: use LocalKey::with, not the LocalKey::set/get
        // convenience methods (stabilized later, in Rust 1.73).
        let frozen = test_support::TEST_CLOCK.with(|c| c.get());
        if let Some(ts) = frozen {
            return ts;
        }
    }
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Test-only deterministic clock for this crate's unit tests.
///
/// `freeze_clock_at(ts)` installs a thread-local override returned by
/// [`now_ms`] on the *calling thread only*. The returned guard restores the
/// previous override (default: none) on drop, including during unwinding.
/// Correct nesting requires LIFO guard destruction: dropping an outer
/// guard while an inner one is still alive can install a stale override —
/// `#[must_use]` plus the conventional `let _g = ...` binding prevents the
/// worst case (immediate destruction, which would un-freeze at once).
/// Spawned threads never inherit the override — frozen-clock tests must
/// stay single-threaded.
///
/// `Option<u64>` is used (rather than treating 0 as "unset") so that an
/// override at timestamp 0 remains representable.
#[cfg(test)]
pub(crate) mod test_support {
    use std::cell::Cell;

    thread_local! {
        pub(crate) static TEST_CLOCK: Cell<Option<u64>> = const { Cell::new(None) };
    }

    /// RAII guard: restores the previous clock override when dropped
    /// (including on panic). The override is a property of the thread that
    /// installed it, and `PhantomData<*const ()>` makes the guard `!Send`:
    /// if it could be moved, dropping it on another thread would corrupt
    /// that thread's override while leaving the original thread frozen.
    #[must_use]
    pub(crate) struct ClockGuard {
        prev: Option<u64>,
        _not_send: std::marker::PhantomData<*const ()>,
    }

    impl Drop for ClockGuard {
        fn drop(&mut self) {
            TEST_CLOCK.with(|c| c.set(self.prev));
        }
    }

    /// Freeze this thread's `now_ms()` to `ts`.
    pub(crate) fn freeze_clock_at(ts: u64) -> ClockGuard {
        ClockGuard {
            prev: TEST_CLOCK.with(|c| c.replace(Some(ts))),
            _not_send: std::marker::PhantomData,
        }
    }

    /// Read the current override (test assertions only).
    pub(crate) fn frozen_now() -> Option<u64> {
        TEST_CLOCK.with(|c| c.get())
    }
}

/// A stored entry: a value with an optional expiry timestamp.
///
/// `expires_at` is `None` for entries with no TTL (permanent until deleted).
/// When `Some(ts)`, `ts` is a UNIX epoch millisecond timestamp; the entry is
/// considered expired once `now_ms() >= ts`.
#[derive(Clone, Debug)]
pub struct Entry {
    pub value: Vec<u8>,
    pub expires_at: Option<u64>,
}

impl Entry {
    /// Create a permanent entry (no TTL).
    pub fn new(value: Vec<u8>) -> Self {
        Entry {
            value,
            expires_at: None,
        }
    }

    /// Create an entry with a TTL. `expires_at` is an absolute UNIX epoch
    /// millisecond timestamp (already computed by the caller).
    pub fn with_expiry(value: Vec<u8>, expires_at: u64) -> Self {
        Entry {
            value,
            expires_at: Some(expires_at),
        }
    }

    /// Returns true if this entry has expired as of `now` — the single
    /// shared expiry predicate (see [`is_expired_at`]; contract comment at
    /// the top of this file). Inclusive: at `now == expires_at` the entry
    /// is expired. Millisecond resolution.
    pub fn is_expired(&self, now: u64) -> bool {
        is_expired_at(self.expires_at, now)
    }
}

/// TTL information returned by [`ShardedKV::ttl`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TtlInfo {
    /// The key exists with no expiry (permanent).
    Permanent,
    /// The key has a TTL. The value is the remaining milliseconds until expiry.
    RemainingMs(u64),
}

/// A sharded, concurrent in-memory key-value store.
///
/// Keys are `String` (UTF-8 required), values are `Vec<u8>` (arbitrary bytes).
/// The store is divided into 16 shards, each protected by a `parking_lot::RwLock`.
/// Keys are distributed across shards using `DefaultHasher` + bitmask.
///
/// # Thread safety
///
/// All operations are thread-safe. `Arc<ShardedKV>` can be shared across threads.
/// Sharding reduces contention: concurrent reads to different keys in different
/// shards proceed without blocking each other.
///
/// # Memory bounds
///
/// By default, the store has no entry limit (`new()`). Use `with_max_entries(n)`
/// to enforce a maximum key count. When full, `set()` for new keys returns `false`.
/// The limit is enforced atomically using CAS — concurrent inserts never exceed it.
///
/// # TTL and expiry
///
/// Entries can have an optional TTL set via `set_with_ttl()`. Plain `set()` creates
/// permanent entries and overwrites any existing TTL on that key. Expired entries
/// are removed lazily on access (`get`/`contains`/`del`/`mget`/`scan`/`ttl`) and
/// actively by a background sweeper (`sweep_expired()`).
///
/// # Example
///
/// ```
/// use kvr::ShardedKV;
///
/// let store = ShardedKV::new();
///
/// // Plain SET — permanent entry, no TTL.
/// assert!(store.set("key", b"value".to_vec()));
/// assert_eq!(store.get("key"), Some(b"value".to_vec()));
///
/// // SETX — entry with a 1-hour TTL (3600000 ms).
/// assert!(store.set_with_ttl("temp", b"ephemeral".to_vec(), 3_600_000));
/// assert_eq!(store.get("temp"), Some(b"ephemeral".to_vec()));
/// ```
pub struct ShardedKV {
    shards: [RwLock<HashMap<String, Entry>>; SHARDS],
    max_entries: usize,
    entry_count: AtomicU64,
}

/// RAII guard that decrements `entry_count` on drop unless `commit()` is called.
/// Closes the panic window between reserving a slot and inserting into the map.
struct EntryReservation<'a> {
    count: &'a AtomicU64,
    committed: bool,
}

impl EntryReservation<'_> {
    /// Mark the reservation as committed — `Drop` will no longer decrement.
    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for EntryReservation<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.count.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl ShardedKV {
    /// Create a new ShardedKV with no entry limit (unlimited).
    pub fn new() -> Self {
        ShardedKV {
            shards: std::array::from_fn(|_| {
                RwLock::new(HashMap::with_capacity(PER_SHARD_CAPACITY))
            }),
            max_entries: 0, // 0 = unlimited
            entry_count: AtomicU64::new(0),
        }
    }

    /// Create a ShardedKV with a maximum entry count.
    /// When the limit is reached, new SET calls for non-existing keys return false.
    /// 0 means unlimited.
    pub fn with_max_entries(max_entries: usize) -> Self {
        let mut kv = Self::new();
        kv.max_entries = max_entries;
        kv
    }

    fn shard(&self, key: &str) -> &RwLock<HashMap<String, Entry>> {
        &self.shards[hash_key(key)]
    }

    /// Retrieve the value for a key. Returns `None` if the key doesn't exist
    /// or has expired (expired entries are removed on access).
    ///
    /// Contract A: an entry whose deadline has elapsed at this call's sampled
    /// time is never returned, regardless of sweep timing.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let now = now_ms();
        let shard = self.shard(key);

        // Fast path: read lock, check for non-expired entry.
        {
            let guard = shard.read();
            let entry = guard.get(key)?;
            if !entry.is_expired(now) {
                return Some(entry.value.clone());
            }
            // Entry exists but is expired — fall through to removal.
        }

        // Slow path: entry is expired, acquire write lock to remove it.
        // Re-check under write lock: another thread may have already removed it.
        {
            let mut guard = shard.write();
            if let Some(entry) = guard.get(key) {
                if entry.is_expired(now) {
                    guard.remove(key);
                    self.entry_count.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
        None
    }

    /// Set a key-value pair with no TTL. Returns false if the store is at max
    /// capacity and the key doesn't already exist (i.e., this would be a new entry).
    ///
    /// Plain SET overwrites any existing TTL on that key — the entry becomes
    /// permanent (no expiry).
    pub fn set(&self, key: &str, value: Vec<u8>) -> bool {
        let shard = self.shard(key);
        let mut guard = shard.write();
        use std::collections::hash_map::Entry as MapEntry;
        match guard.entry(key.to_string()) {
            MapEntry::Occupied(mut e) => {
                // Overwrite — no count change, clear any existing TTL.
                e.insert(Entry::new(value));
                true
            }
            MapEntry::Vacant(e) => {
                // New key — atomically reserve a slot.
                let reservation = match self.try_reserve_entry_guard() {
                    Some(r) => r,
                    None => return false, // store full
                };
                e.insert(Entry::new(value));
                reservation.commit();
                true
            }
        }
    }

    /// Set a key-value pair with a TTL. `ttl_ms` is relative milliseconds from
    /// the server's current time. Returns false if the store is at max capacity
    /// and the key doesn't already exist. Returns false if `ttl_ms` is 0.
    ///
    /// If the key already exists, its TTL is updated (overwrite, no count change).
    pub fn set_with_ttl(&self, key: &str, value: Vec<u8>, ttl_ms: u64) -> bool {
        if ttl_ms == 0 {
            return false;
        }
        let expires_at = now_ms().saturating_add(ttl_ms);
        let shard = self.shard(key);
        let mut guard = shard.write();
        use std::collections::hash_map::Entry as MapEntry;
        match guard.entry(key.to_string()) {
            MapEntry::Occupied(mut e) => {
                // Overwrite — no count change, set new TTL.
                e.insert(Entry::with_expiry(value, expires_at));
                true
            }
            MapEntry::Vacant(e) => {
                // New key — atomically reserve a slot.
                let reservation = match self.try_reserve_entry_guard() {
                    Some(r) => r,
                    None => return false, // store full
                };
                e.insert(Entry::with_expiry(value, expires_at));
                reservation.commit();
                true
            }
        }
    }

    /// Atomically reserve an entry slot using CAS.
    /// Returns a guard that decrements the count on drop unless committed.
    /// Returns `None` if the store is at max capacity.
    fn try_reserve_entry_guard(&self) -> Option<EntryReservation<'_>> {
        if self.max_entries == 0 {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
            return Some(EntryReservation {
                count: &self.entry_count,
                committed: false,
            });
        }
        let max = self.max_entries as u64;
        let mut current = self.entry_count.load(Ordering::Relaxed);
        loop {
            if current >= max {
                return None;
            }
            match self.entry_count.compare_exchange_weak(
                current,
                current + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(EntryReservation {
                        count: &self.entry_count,
                        committed: false,
                    })
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Delete a key. Returns the removed value, or `None` if the key didn't exist
    /// or had expired (expired entries are removed on access).
    ///
    /// Contract A: an entry whose deadline has elapsed at this call's sampled
    /// time is treated as already absent — DEL returns `None`, never a value.
    pub fn del(&self, key: &str) -> Option<Vec<u8>> {
        let now = now_ms();
        let shard = self.shard(key);
        let mut guard = shard.write();
        match guard.remove(key) {
            Some(entry) if entry.is_expired(now) => {
                // Expired — already removed, decrement count, return None.
                self.entry_count.fetch_sub(1, Ordering::Relaxed);
                None
            }
            Some(entry) => {
                // Live entry — already removed, decrement count.
                self.entry_count.fetch_sub(1, Ordering::Relaxed);
                Some(entry.value)
            }
            None => None,
        }
    }

    /// Query the TTL of a key. Returns `None` if the key doesn't exist or has
    /// expired (expired entries are removed on access, same as `get`).
    ///
    /// Returns `Some(TtlInfo::Permanent)` for keys with no TTL, or
    /// `Some(TtlInfo::RemainingMs(ms))` for keys with a TTL, where `ms` is the
    /// remaining milliseconds until expiry.
    ///
    /// Contract A: an entry whose deadline has elapsed at this call's sampled
    /// time returns `None`, regardless of sweep timing. Because expiry is
    /// inclusive (`now >= expires_at`), `RemainingMs(0)` is unreachable.
    pub fn ttl(&self, key: &str) -> Option<TtlInfo> {
        let now = now_ms();
        let shard = self.shard(key);

        // Fast path: read lock, check for non-expired entry.
        {
            let guard = shard.read();
            let entry = guard.get(key)?;
            if !entry.is_expired(now) {
                return Some(match entry.expires_at {
                    None => TtlInfo::Permanent,
                    Some(exp) => TtlInfo::RemainingMs(exp.saturating_sub(now)),
                });
            }
            // Entry exists but is expired — fall through to removal.
        }

        // Slow path: entry is expired, acquire write lock to remove it.
        // Re-check under write lock: another thread may have already removed it.
        {
            let mut guard = shard.write();
            if let Some(entry) = guard.get(key) {
                if entry.is_expired(now) {
                    guard.remove(key);
                    self.entry_count.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
        None
    }

    /// Check if a key exists without retrieving its value.
    /// Expired entries are treated as absent and removed on access.
    ///
    /// Contract A: an entry whose deadline has elapsed at this call's sampled
    /// time is never reported as existing, regardless of sweep timing.
    pub fn contains(&self, key: &str) -> bool {
        let now = now_ms();
        let shard = self.shard(key);

        // Fast path: read lock, check for non-expired entry.
        {
            let guard = shard.read();
            let entry = guard.get(key);
            if entry.is_none() {
                return false;
            }
            let entry = entry.unwrap();
            if !entry.is_expired(now) {
                return true;
            }
            // Entry exists but is expired — fall through to removal.
        }

        // Slow path: entry is expired, acquire write lock to remove it.
        {
            let mut guard = shard.write();
            if let Some(entry) = guard.get(key) {
                if entry.is_expired(now) {
                    guard.remove(key);
                    self.entry_count.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
        false
    }

    /// Sweep all shards and remove expired entries. Returns the number of
    /// entries removed. Computes `now_ms()` once at the start for a coherent
    /// "as-of" timestamp across all shards.
    ///
    /// Acquires a write lock on each shard individually (never globally).
    ///
    /// # Complexity
    ///
    /// O(n) where n is the total number of entries across all shards. No
    /// expiry index (e.g. min-heap) is used; the sweep scans all entries.
    /// At the designed scale (≤100K entries) this is acceptable. The
    /// background sweeper runs every `KVR_SWEEP_INTERVAL_SECS` seconds
    /// (default 30) to bound stale entry lifetime.
    pub fn sweep_expired(&self) -> usize {
        let now = now_ms();
        let mut removed = 0;
        for shard in &self.shards {
            let mut guard = shard.write();
            let expired_keys: Vec<String> = guard
                .iter()
                .filter(|(_, entry)| entry.is_expired(now))
                .map(|(k, _)| k.clone())
                .collect();
            for key in &expired_keys {
                guard.remove(key);
                removed += 1;
            }
        }
        if removed > 0 {
            self.entry_count
                .fetch_sub(removed as u64, Ordering::Relaxed);
        }
        removed
    }

    /// Scan for keys matching a prefix, up to `limit`, starting after `cursor`.
    ///
    /// Contract A: an entry whose deadline has elapsed at this call's sampled
    /// time is never included in the result (or the `more` page count),
    /// regardless of sweep timing. Exception: `limit == 0` returns early
    /// without sampling time or inspecting entries.
    ///
    /// Returns matching keys (not values) in lexicographic order, plus a
    /// `more` flag indicating whether additional pages exist.
    ///
    /// - Empty prefix matches all keys.
    /// - Empty cursor starts from the beginning.
    /// - Expired keys are never returned. Expired entries encountered during
    ///   scan are removed (lazy purge, same as `get`/`contains`/`del`).
    ///   All expired entries in every shard are eligible for purge, regardless
    ///   of prefix or cursor match.
    /// - `limit` is the maximum number of keys returned. `limit == 0` returns
    ///   an empty result with `more = false` and does not purge.
    ///
    /// Implementation: collects per-shard matches then merge-sorts.
    /// At ≤100K entries O(n) per SCAN is acceptable — no ordered index.
    pub fn scan(&self, prefix: &str, limit: usize, cursor: &str) -> (Vec<String>, bool) {
        if limit == 0 {
            return (vec![], false);
        }

        let now = now_ms();

        // Collect matching keys from all shards, and purge expired entries.
        let mut all_keys: Vec<String> = Vec::new();
        for shard in &self.shards {
            // Pass 1: read lock — collect matching non-expired keys and
            // expired keys to purge.
            let expired_keys: Vec<String> = {
                let guard = shard.read();
                let mut expired = Vec::new();
                for (key, entry) in guard.iter() {
                    if entry.is_expired(now) {
                        expired.push(key.clone());
                        continue;
                    }
                    if !prefix.is_empty() && !key.starts_with(prefix) {
                        continue;
                    }
                    if !cursor.is_empty() && key.as_str() <= cursor {
                        continue;
                    }
                    all_keys.push(key.clone());
                }
                expired
            };
            // Read guard is dropped here.

            // Pass 2: write lock — re-check and remove expired entries.
            // Re-check under write lock prevents double-decrement vs concurrent
            // get/del/sweep/mget.
            if !expired_keys.is_empty() {
                let mut guard = shard.write();
                for key in &expired_keys {
                    if let Some(entry) = guard.get(key) {
                        if entry.is_expired(now) {
                            guard.remove(key);
                            self.entry_count.fetch_sub(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        }

        // Sort lexicographically.
        all_keys.sort();

        // Take up to `limit` keys.
        let more = all_keys.len() > limit;
        all_keys.truncate(limit);

        (all_keys, more)
    }

    /// Batch retrieve values for multiple keys, preserving request order.
    ///
    /// Contract A: for each requested key whose deadline has elapsed at this
    /// call's sampled time, the result slot is `None`, regardless of sweep
    /// timing. (Batch reads are not a transactional snapshot — each key's
    /// filtering uses the single `now` sampled at call start.)
    ///
    /// Returns `Some(value)` for found (non-expired) keys, `None` for missing
    /// or expired keys. Expired keys encountered during MGET are removed
    /// (lazy expiry, same as GET).
    pub fn mget(&self, keys: &[String]) -> Vec<Option<Vec<u8>>> {
        let now = now_ms();
        let mut results = Vec::with_capacity(keys.len());

        for key in keys {
            let shard = self.shard(key);

            // Fast path: read lock — distinguish three states:
            // - found: entry exists and is live → no write lock needed.
            // - needs_removal: entry exists but is expired → write lock to remove.
            // - absent: entry doesn't exist → no write lock needed.
            let mut found = None;
            let mut needs_removal = false;
            {
                let guard = shard.read();
                if let Some(entry) = guard.get(key) {
                    if entry.is_expired(now) {
                        needs_removal = true;
                    } else {
                        found = Some(entry.value.clone());
                    }
                }
            }

            // Slow path: only for expired entries (not plain misses).
            // Re-check under write lock: another thread may have already
            // removed it or overwritten it with a live value.
            if needs_removal {
                let mut guard = shard.write();
                if let Some(entry) = guard.get(key) {
                    if entry.is_expired(now) {
                        guard.remove(key);
                        self.entry_count.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }

            results.push(found);
        }

        results
    }

    /// Collect all entries for a point-in-time snapshot.
    ///
    /// Acquires read locks on ALL 16 shards simultaneously in shard-index
    /// order (0→15), clones non-expired entries, then releases all locks.
    /// This gives a true point-in-time view — no write can land between shard
    /// collections. Serialization and file I/O happen outside any shard lock.
    ///
    /// Expired entries are excluded at save time. Load-time filtering is still
    /// applied as defense-in-depth (entries can expire between save and load).
    ///
    /// Returns `(key, value, expires_at)` tuples for non-expired entries only.
    pub fn collect_for_snapshot(&self) -> Vec<(String, Vec<u8>, Option<u64>)> {
        // Acquire all shard read locks simultaneously in index order.
        let guards: Vec<_> = self.shards.iter().map(|s| s.read()).collect();

        let now = now_ms();
        let mut entries = Vec::new();
        for guard in &guards {
            for (key, entry) in guard.iter() {
                if entry.is_expired(now) {
                    continue;
                }
                entries.push((key.clone(), entry.value.clone(), entry.expires_at));
            }
        }

        // Guards drop here, releasing all shard read locks.
        entries
    }

    /// Load a single entry during snapshot restore. **Restore-only API: it
    /// deliberately bypasses the max-entries CAS guard, so it must NOT be
    /// used for normal writes** — doing so lets the store exceed the
    /// `with_max_entries` bound. It exists solely so a persisted snapshot can
    /// be reloaded even if it was taken under a larger (or unlimited) bound.
    /// The entry_count is incremented directly. If the key already exists,
    /// the value is overwritten without a count change.
    pub fn load_entry(&self, key: String, value: Vec<u8>, expires_at: Option<u64>) {
        let shard = self.shard(&key);
        let mut guard = shard.write();
        use std::collections::hash_map::Entry as MapEntry;
        match guard.entry(key) {
            MapEntry::Occupied(mut e) => {
                // Key already exists (duplicate in snapshot) — overwrite, no count change.
                e.insert(crate::Entry { value, expires_at });
            }
            MapEntry::Vacant(e) => {
                e.insert(crate::Entry { value, expires_at });
                self.entry_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Returns the current number of entries in the store.
    ///
    /// This is an approximate physical count under concurrent mutation.
    /// It includes expired-but-not-yet-removed entries, and may briefly
    /// include reserved-but-not-yet-inserted entries. Expired entries are
    /// removed (and the count decremented) when accessed via
    /// `get`/`contains`/`del`/`mget`/`scan`/`ttl` or when the sweeper runs.
    /// See also [`len_active`](Self::len_active) for the logical non-expired count.
    pub fn len(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Returns the number of non-expired entries in the store.
    ///
    /// This is an O(n) operation that iterates all shards under read locks
    /// (one at a time). Unlike [`len`](Self::len), it excludes expired-but-not-yet-removed
    /// entries. Does not purge expired entries or free capacity.
    ///
    /// Under concurrent writes, this is a best-effort as-of-one-`now` count,
    /// not a transactional snapshot.
    pub fn len_active(&self) -> u64 {
        let now = now_ms();
        let mut count = 0u64;
        for shard in &self.shards {
            let guard = shard.read();
            count += guard.iter().filter(|(_, e)| !e.is_expired(now)).count() as u64;
        }
        count
    }

    /// Returns true if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for ShardedKV {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_set_get() {
        let store = ShardedKV::new();
        store.set("hello", b"world".to_vec());
        assert_eq!(store.get("hello"), Some(b"world".to_vec()));
    }

    #[test]
    fn test_del() {
        let store = ShardedKV::new();
        store.set("key", b"value".to_vec());
        assert_eq!(store.del("key"), Some(b"value".to_vec()));
        assert_eq!(store.get("key"), None);
    }

    #[test]
    fn test_overwrite() {
        let store = ShardedKV::new();
        store.set("k", b"v1".to_vec());
        store.set("k", b"v2".to_vec());
        assert_eq!(store.get("k"), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_delete_then_get() {
        let store = ShardedKV::new();
        store.set("x", b"y".to_vec());
        store.del("x");
        assert_eq!(store.get("x"), None);
    }

    #[test]
    fn test_del_nonexistent() {
        let store = ShardedKV::new();
        assert_eq!(store.del("nope"), None);
    }

    #[test]
    fn test_get_nonexistent() {
        let store = ShardedKV::new();
        assert_eq!(store.get("nope"), None);
    }

    #[test]
    fn test_contains() {
        let store = ShardedKV::new();
        store.set("k", b"v".to_vec());
        assert!(store.contains("k"));
        assert!(!store.contains("missing"));
    }

    #[test]
    fn test_empty_value() {
        let store = ShardedKV::new();
        store.set("empty", vec![]);
        assert_eq!(store.get("empty"), Some(vec![]));
        assert!(store.contains("empty"));
    }

    #[test]
    fn test_large_value() {
        let store = ShardedKV::new();
        let val: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        store.set("big", val.clone());
        assert_eq!(store.get("big"), Some(val));
    }

    #[test]
    fn test_concurrent_access() {
        let store = Arc::new(ShardedKV::new());
        let mut handles = vec![];

        for t in 0..8 {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0..500 {
                    let key = format!("t{}_{}", t, i);
                    let val = vec![(t + i) as u8];
                    s.set(&key, val.clone());
                    let retrieved = s.get(&key);
                    assert_eq!(retrieved, Some(val));
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_concurrent_same_key() {
        // Exercise concurrent writes/reads to the SAME key — verifies
        // sharded locking actually protects against data races.
        let store = Arc::new(ShardedKV::new());
        let mut handles = vec![];

        for t in 0..8 {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0..500 {
                    let val = vec![(t + i) as u8];
                    s.set("contention-key", val.clone());
                    let retrieved = s.get("contention-key");
                    // The last writer wins; we just assert consistency:
                    // whatever we read back must be non-empty and match
                    // some writer's value.
                    if let Some(ref r) = retrieved {
                        assert!(!r.is_empty());
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Final state: some thread's value must be stored.
        let final_val = store.get("contention-key");
        assert!(final_val.is_some());
        assert!(!final_val.unwrap().is_empty());
    }

    #[test]
    fn test_max_entries() {
        let store = ShardedKV::with_max_entries(3);
        assert!(store.set("a", b"1".to_vec()));
        assert!(store.set("b", b"2".to_vec()));
        assert!(store.set("c", b"3".to_vec()));
        // Store is full — new key should be rejected.
        assert!(!store.set("d", b"4".to_vec()));
        // Overwriting existing key should still work.
        assert!(store.set("a", b"updated".to_vec()));
        assert_eq!(store.get("a"), Some(b"updated".to_vec()));
        // After deleting one, we can add a new key.
        store.del("b");
        assert!(store.set("d", b"4".to_vec()));
        assert_eq!(store.get("d"), Some(b"4".to_vec()));
    }

    #[test]
    fn test_entry_count() {
        let store = ShardedKV::new();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
        store.set("a", b"1".to_vec());
        store.set("b", b"2".to_vec());
        assert_eq!(store.len(), 2);
        store.set("a", b"updated".to_vec()); // overwrite, no count change
        assert_eq!(store.len(), 2);
        store.del("a");
        assert_eq!(store.len(), 1);
        store.del("nonexistent"); // no count change
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_unlimited_entries() {
        let store = ShardedKV::new(); // max_entries = 0 = unlimited
        for i in 0..1000 {
            assert!(store.set(&format!("k{i}"), b"v".to_vec()));
        }
        assert_eq!(store.len(), 1000);
    }

    #[test]
    fn test_max_entries_concurrent() {
        // Verify that concurrent inserts never exceed max_entries.
        let store = Arc::new(ShardedKV::with_max_entries(50));
        let mut handles = vec![];

        for t in 0..8 {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                let mut inserted = 0;
                for i in 0..100 {
                    let key = format!("t{}_{}", t, i);
                    if s.set(&key, b"v".to_vec()) {
                        inserted += 1;
                    }
                }
                inserted
            }));
        }

        let total_inserted: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        // Total successful inserts must not exceed the max.
        assert!(
            total_inserted <= 50,
            "inserted {total_inserted} entries but max is 50"
        );
        assert_eq!(store.len(), total_inserted);
        assert!(store.len() <= 50, "len() = {} exceeds max 50", store.len());
    }

    // ─── TTL tests ───────────────────────────────────────────────────────

    #[test]
    fn test_setx_roundtrip() {
        let store = ShardedKV::new();
        assert!(store.set_with_ttl("temp", b"ephemeral".to_vec(), 3_600_000));
        assert_eq!(store.get("temp"), Some(b"ephemeral".to_vec()));
        assert!(store.contains("temp"));
    }

    #[test]
    fn test_expiry_via_lazy_get() {
        let store = ShardedKV::new();
        // 50ms TTL — short enough for a fast test, long enough to not race.
        assert!(store.set_with_ttl("temp", b"v".to_vec(), 50));
        assert_eq!(store.get("temp"), Some(b"v".to_vec()));
        assert_eq!(store.len(), 1);

        // Wait for expiry.
        thread::sleep(Duration::from_millis(80));

        // GET on expired entry → None, entry removed, count decremented.
        assert_eq!(store.get("temp"), None);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_expiry_via_lazy_contains() {
        let store = ShardedKV::new();
        assert!(store.set_with_ttl("temp", b"v".to_vec(), 50));
        assert!(store.contains("temp"));

        thread::sleep(Duration::from_millis(80));

        assert!(!store.contains("temp"));
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_expiry_via_lazy_del() {
        let store = ShardedKV::new();
        assert!(store.set_with_ttl("temp", b"v".to_vec(), 50));
        assert_eq!(store.len(), 1);

        thread::sleep(Duration::from_millis(80));

        // DEL on expired entry → None (treated as absent), count decremented.
        assert_eq!(store.del("temp"), None);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_expiry_via_sweeper() {
        let store = ShardedKV::new();
        assert!(store.set_with_ttl("a", b"1".to_vec(), 50));
        assert!(store.set_with_ttl("b", b"2".to_vec(), 50));
        // Permanent entry — should NOT be swept.
        store.set("perm", b"forever".to_vec());
        assert_eq!(store.len(), 3);

        thread::sleep(Duration::from_millis(80));

        let removed = store.sweep_expired();
        assert_eq!(removed, 2);
        assert_eq!(store.len(), 1);
        assert_eq!(store.get("perm"), Some(b"forever".to_vec()));
        assert_eq!(store.get("a"), None);
        assert_eq!(store.get("b"), None);
    }

    #[test]
    fn test_set_after_setx_clears_ttl() {
        let store = ShardedKV::new();
        // SETX with short TTL.
        assert!(store.set_with_ttl("k", b"v1".to_vec(), 50));
        assert_eq!(store.len(), 1);

        // Plain SET overwrites — entry becomes permanent (no TTL).
        assert!(store.set("k", b"v2".to_vec()));
        assert_eq!(store.len(), 1);

        // Wait past the original TTL — entry should still be present.
        thread::sleep(Duration::from_millis(80));
        assert_eq!(store.get("k"), Some(b"v2".to_vec()));
        assert!(store.contains("k"));
    }

    #[test]
    fn test_setx_overwrite_existing_ttl() {
        let store = ShardedKV::new();
        // First SETX with 50ms TTL.
        assert!(store.set_with_ttl("k", b"v1".to_vec(), 50));

        // Second SETX with long TTL — overwrites value and TTL, no count change.
        assert!(store.set_with_ttl("k", b"v2".to_vec(), 3_600_000));
        assert_eq!(store.len(), 1);

        // Wait past the first TTL — entry should still be present (second TTL).
        thread::sleep(Duration::from_millis(80));
        assert_eq!(store.get("k"), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_setx_overwrite_existing_permanent() {
        let store = ShardedKV::new();
        // Plain SET first (permanent).
        store.set("k", b"v1".to_vec());
        assert_eq!(store.len(), 1);

        // SETX overwrites with a short TTL, no count change.
        assert!(store.set_with_ttl("k", b"v2".to_vec(), 50));
        assert_eq!(store.len(), 1);

        thread::sleep(Duration::from_millis(80));
        assert_eq!(store.get("k"), None);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_capacity_freed_after_expiry() {
        // Fill store to capacity with short-TTL keys, wait for expiry,
        // verify new SETs succeed (capacity was freed).
        let store = ShardedKV::with_max_entries(5);
        for i in 0..5 {
            assert!(store.set_with_ttl(&format!("k{i}"), b"v".to_vec(), 50));
        }
        assert_eq!(store.len(), 5);
        // Store is full.
        assert!(!store.set("new", b"v".to_vec()));

        // Wait for expiry, then sweep.
        thread::sleep(Duration::from_millis(80));
        let removed = store.sweep_expired();
        assert_eq!(removed, 5);
        assert_eq!(store.len(), 0);

        // New SETs should succeed now.
        assert!(store.set("new", b"v".to_vec()));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_sweep_expired_no_entries() {
        let store = ShardedKV::new();
        assert_eq!(store.sweep_expired(), 0);
    }

    #[test]
    fn test_sweep_expired_only_permanent() {
        let store = ShardedKV::new();
        store.set("a", b"1".to_vec());
        store.set("b", b"2".to_vec());
        assert_eq!(store.sweep_expired(), 0);
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn test_setx_ttl_zero_rejected() {
        let store = ShardedKV::new();
        assert!(!store.set_with_ttl("k", b"v".to_vec(), 0));
        assert_eq!(store.get("k"), None);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_capacity_freed_via_lazy_get() {
        // Fill store to capacity with short-TTL keys, wait for expiry,
        // then use GET (lazy removal) to free capacity — no sweep needed.
        let store = ShardedKV::with_max_entries(3);
        for i in 0..3 {
            assert!(store.set_with_ttl(&format!("k{i}"), b"v".to_vec(), 50));
        }
        assert_eq!(store.len(), 3);
        // Store is full.
        assert!(!store.set("new", b"v".to_vec()));

        // Wait for expiry.
        thread::sleep(Duration::from_millis(80));

        // GET each expired key — lazy removal frees capacity.
        for i in 0..3 {
            assert_eq!(store.get(&format!("k{i}")), None);
        }
        assert_eq!(store.len(), 0);

        // New SET should succeed now (capacity freed via lazy path only).
        assert!(store.set("new", b"v".to_vec()));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_concurrent_lazy_get_vs_sweep() {
        // Concurrent lazy GET on expired keys + sweeper on the same keys.
        // Must not double-decrement (underflow) or leak entries.
        let store = Arc::new(ShardedKV::new());

        // Insert 100 short-TTL keys.
        for i in 0..100 {
            store.set_with_ttl(&format!("k{i}"), b"v".to_vec(), 50);
        }
        assert_eq!(store.len(), 100);

        // Wait for expiry.
        thread::sleep(Duration::from_millis(80));

        let mut handles = vec![];

        // Thread 1: lazy GET on all keys.
        {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let _ = s.get(&format!("k{i}"));
                }
            }));
        }

        // Thread 2: sweep concurrently.
        {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                s.sweep_expired();
            }));
        }

        // Thread 3: more lazy GETs.
        {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let _ = s.get(&format!("k{i}"));
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Count must be exactly 0 — no underflow (which would wrap to a huge u64).
        assert_eq!(
            store.len(),
            0,
            "entry count should be 0, got {} (possible underflow)",
            store.len()
        );
    }

    #[test]
    fn test_concurrent_ttl_and_sweeper() {
        // Threads inserting short-TTL keys while the sweeper runs.
        // Final count must be exact, never negative, never leaked.
        let store = Arc::new(ShardedKV::with_max_entries(0));
        let mut handles = vec![];

        // 4 threads each inserting 200 short-TTL keys (50ms TTL).
        for t in 0..4 {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0..200 {
                    let key = format!("t{}_{}", t, i);
                    s.set_with_ttl(&key, b"v".to_vec(), 50);
                }
            }));
        }

        // Run sweeper concurrently while inserts are happening.
        let s = Arc::clone(&store);
        let sweeper = thread::spawn(move || {
            for _ in 0..10 {
                thread::sleep(Duration::from_millis(20));
                s.sweep_expired();
            }
        });

        for h in handles {
            h.join().unwrap();
        }
        sweeper.join().unwrap();

        // Wait for all remaining TTLs to expire, then do a final sweep.
        thread::sleep(Duration::from_millis(100));
        store.sweep_expired();

        // All entries should be gone — count must be exactly 0.
        assert_eq!(
            store.len(),
            0,
            "entry count should be 0 after all TTLs expired and swept"
        );
    }

    // ─── SCAN tests ─────────────────────────────────────────────────────

    #[test]
    fn test_scan_basic() {
        let store = ShardedKV::new();
        store.set("apple", b"1".to_vec());
        store.set("app2", b"2".to_vec());
        store.set("banana", b"3".to_vec());

        let (keys, more) = store.scan("app", 100, "");
        assert_eq!(keys, vec!["app2", "apple"]); // lexicographic order
        assert!(!more);
    }

    #[test]
    fn test_scan_empty_prefix() {
        let store = ShardedKV::new();
        store.set("b", b"1".to_vec());
        store.set("a", b"2".to_vec());
        store.set("c", b"3".to_vec());

        let (keys, more) = store.scan("", 100, "");
        assert_eq!(keys, vec!["a", "b", "c"]); // sorted
        assert!(!more);
    }

    #[test]
    fn test_scan_pagination() {
        let store = ShardedKV::new();
        for i in 0..10 {
            store.set(&format!("k{i:02}"), b"v".to_vec());
        }

        // Page 1: limit=3.
        let (keys, more) = store.scan("", 3, "");
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], "k00");
        assert_eq!(keys[2], "k02");
        assert!(more);

        // Page 2: cursor = last key of page 1.
        let (keys, more) = store.scan("", 3, "k02");
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], "k03");
        assert_eq!(keys[2], "k05");
        assert!(more);

        // Page 3.
        let (keys, more) = store.scan("", 3, "k05");
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], "k06");
        assert_eq!(keys[2], "k08");
        assert!(more);

        // Page 4: 1 key left.
        let (keys, more) = store.scan("", 3, "k08");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], "k09");
        assert!(!more);
    }

    #[test]
    fn test_scan_excludes_expired() {
        let store = ShardedKV::new();
        store.set("perm", b"1".to_vec());
        store.set_with_ttl("temp", b"2".to_vec(), 50);

        thread::sleep(Duration::from_millis(80));

        let (keys, more) = store.scan("", 100, "");
        assert_eq!(keys, vec!["perm"]);
        assert!(!more);
    }

    #[test]
    fn test_scan_empty_store() {
        let store = ShardedKV::new();
        let (keys, more) = store.scan("", 100, "");
        assert!(keys.is_empty());
        assert!(!more);
    }

    #[test]
    fn test_scan_limit_zero() {
        let store = ShardedKV::new();
        store.set("a", b"1".to_vec());
        let (keys, more) = store.scan("", 0, "");
        assert!(keys.is_empty());
        assert!(!more);
    }

    // ─── MGET tests ─────────────────────────────────────────────────────

    #[test]
    fn test_mget_mixed() {
        let store = ShardedKV::new();
        store.set("a", b"val_a".to_vec());
        store.set("c", b"val_c".to_vec());

        let results = store.mget(&["a".to_string(), "b".to_string(), "c".to_string()]);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Some(b"val_a".to_vec()));
        assert_eq!(results[1], None);
        assert_eq!(results[2], Some(b"val_c".to_vec()));
    }

    #[test]
    fn test_mget_preserves_order() {
        let store = ShardedKV::new();
        store.set("x", b"1".to_vec());
        store.set("y", b"2".to_vec());
        store.set("z", b"3".to_vec());

        let results = store.mget(&["z".to_string(), "x".to_string(), "y".to_string()]);
        assert_eq!(results[0], Some(b"3".to_vec()));
        assert_eq!(results[1], Some(b"1".to_vec()));
        assert_eq!(results[2], Some(b"2".to_vec()));
    }

    #[test]
    fn test_mget_all_missing() {
        let store = ShardedKV::new();
        let results = store.mget(&["a".to_string(), "b".to_string()]);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], None);
        assert_eq!(results[1], None);
    }

    #[test]
    fn test_mget_empty() {
        let store = ShardedKV::new();
        let results = store.mget(&[]);
        assert!(results.is_empty());
    }

    #[test]
    fn test_mget_excludes_expired() {
        let store = ShardedKV::new();
        store.set("perm", b"1".to_vec());
        store.set_with_ttl("temp", b"2".to_vec(), 50);

        thread::sleep(Duration::from_millis(80));

        let results = store.mget(&["perm".to_string(), "temp".to_string()]);
        assert_eq!(results[0], Some(b"1".to_vec()));
        assert_eq!(results[1], None); // expired
        assert_eq!(store.len(), 1); // expired entry was lazily removed
    }

    // ─── TTL tests ──────────────────────────────────────────────────────

    #[test]
    fn test_ttl_permanent() {
        let store = ShardedKV::new();
        store.set("k", b"v".to_vec());
        assert_eq!(store.ttl("k"), Some(TtlInfo::Permanent));
    }

    #[test]
    fn test_ttl_with_expiry() {
        let store = ShardedKV::new();
        store.set_with_ttl("temp", b"v".to_vec(), 3_600_000);
        match store.ttl("temp") {
            Some(TtlInfo::RemainingMs(ms)) => {
                assert!(ms > 3_500_000 && ms <= 3_600_000);
            }
            other => panic!("expected RemainingMs, got {other:?}"),
        }
    }

    #[test]
    fn test_ttl_missing() {
        let store = ShardedKV::new();
        assert_eq!(store.ttl("missing"), None);
    }

    #[test]
    fn test_ttl_expired_purges() {
        let store = ShardedKV::new();
        store.set_with_ttl("temp", b"v".to_vec(), 50);
        assert_eq!(store.len(), 1);

        thread::sleep(Duration::from_millis(80));

        assert_eq!(store.ttl("temp"), None);
        assert_eq!(store.len(), 0); // expired entry was lazily removed
    }

    #[test]
    fn test_ttl_expired_frees_capacity() {
        let store = ShardedKV::with_max_entries(1);
        assert!(store.set_with_ttl("temp", b"v".to_vec(), 50));
        assert!(!store.set("new", b"v".to_vec())); // full

        thread::sleep(Duration::from_millis(80));

        // TTL on expired key purges it, freeing capacity.
        assert_eq!(store.ttl("temp"), None);
        assert!(store.set("new", b"v".to_vec())); // now succeeds
    }

    // ─── Scan purge tests ───────────────────────────────────────────────

    #[test]
    fn test_scan_purges_expired() {
        let store = ShardedKV::new();
        store.set("perm", b"1".to_vec());
        store.set_with_ttl("temp", b"2".to_vec(), 50);
        assert_eq!(store.len(), 2);

        thread::sleep(Duration::from_millis(80));

        let (keys, _) = store.scan("", 100, "");
        assert_eq!(keys.len(), 1); // only "perm"
        assert_eq!(store.len(), 1); // expired entry was purged by scan
    }

    #[test]
    fn test_scan_purges_expired_frees_capacity() {
        let store = ShardedKV::with_max_entries(1);
        assert!(store.set_with_ttl("temp", b"v".to_vec(), 50));
        assert!(!store.set("new", b"v".to_vec())); // full

        thread::sleep(Duration::from_millis(80));

        // Scan purges the expired entry, freeing capacity.
        let _ = store.scan("", 100, "");
        assert!(store.set("new", b"v".to_vec())); // now succeeds
    }

    #[test]
    fn test_scan_limit_zero_does_not_purge() {
        let store = ShardedKV::new();
        store.set_with_ttl("temp", b"v".to_vec(), 50);
        assert_eq!(store.len(), 1);

        thread::sleep(Duration::from_millis(80));

        // limit=0 returns early without iterating, so no purge.
        let (keys, more) = store.scan("", 0, "");
        assert!(keys.is_empty());
        assert!(!more);
        assert_eq!(store.len(), 1); // expired entry still present (not purged)
    }

    #[test]
    fn test_scan_purges_all_expired_not_just_matching_prefix() {
        let store = ShardedKV::new();
        store.set_with_ttl("prefix_a", b"1".to_vec(), 50);
        store.set_with_ttl("prefix_b", b"2".to_vec(), 50);
        store.set("other", b"3".to_vec());
        assert_eq!(store.len(), 3);

        thread::sleep(Duration::from_millis(80));

        // Scan with prefix "prefix_" — should purge ALL expired entries
        // in the shard, not just those matching the prefix.
        let (keys, _) = store.scan("prefix_", 100, "");
        assert_eq!(keys.len(), 0); // both expired, neither returned
        assert_eq!(store.len(), 1); // both expired entries purged, "other" remains
    }

    // ─── len_active tests ───────────────────────────────────────────────

    #[test]
    fn test_len_active_excludes_expired() {
        let store = ShardedKV::new();
        store.set("perm", b"1".to_vec());
        store.set_with_ttl("temp", b"2".to_vec(), 50);

        assert_eq!(store.len(), 2);
        assert_eq!(store.len_active(), 2);

        thread::sleep(Duration::from_millis(80));

        // len() still counts the expired-but-not-removed entry.
        assert_eq!(store.len(), 2);
        // len_active() excludes it.
        assert_eq!(store.len_active(), 1);
    }

    // ─── Card 1 new tests ───────────────────────────────────────────────

    #[test]
    fn test_set_full_store_rejects_new_key() {
        // Verify the entry()-based refactor still rejects new keys when full.
        let store = ShardedKV::with_max_entries(2);
        assert!(store.set("a", b"1".to_vec()));
        assert!(store.set("b", b"2".to_vec()));
        // Store is full — new key rejected.
        assert!(!store.set("c", b"3".to_vec()));
        // Overwriting existing key still works.
        assert!(store.set("a", b"updated".to_vec()));
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn test_set_overwrite_expired_entry() {
        // Overwriting an expired-but-unswept key should NOT change the count.
        // The entry() API treats it as Occupied (overwrite, no count change).
        let store = ShardedKV::with_max_entries(5);
        assert!(store.set_with_ttl("k", b"v1".to_vec(), 50));
        assert_eq!(store.len(), 1);

        // Wait for expiry but don't sweep — the key is still in the map.
        thread::sleep(Duration::from_millis(80));

        // Overwrite with a permanent entry. The key exists in the map
        // (expired but not removed), so entry() sees Occupied.
        // Count should NOT change (no new entry added).
        assert!(store.set("k", b"v2".to_vec()));
        assert_eq!(store.len(), 1);
        assert_eq!(store.get("k"), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_mget_plain_miss_no_write_lock() {
        // mget on absent keys should not inflate count or cause issues.
        // This is a functional regression guard for the tri-state fix.
        let store = ShardedKV::new();
        store.set("present", b"val".to_vec());
        assert_eq!(store.len(), 1);

        // mget with a mix of present and absent keys.
        let results = store.mget(&[
            "present".to_string(),
            "absent1".to_string(),
            "absent2".to_string(),
            "absent3".to_string(),
        ]);

        assert_eq!(results[0], Some(b"val".to_vec()));
        assert_eq!(results[1], None);
        assert_eq!(results[2], None);
        assert_eq!(results[3], None);

        // Count must be unchanged — absent keys should not have triggered
        // any write-lock path that could corrupt state.
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_del_single_remove_semantics() {
        // Verify the refactored del() (single remove) preserves semantics.
        let store = ShardedKV::new();
        store.set("live", b"val".to_vec());
        assert_eq!(store.del("live"), Some(b"val".to_vec()));
        assert_eq!(store.len(), 0);
        assert_eq!(store.get("live"), None);

        // del on missing key.
        assert_eq!(store.del("missing"), None);
        assert_eq!(store.len(), 0);

        // del on expired key returns None, decrements count.
        store.set_with_ttl("temp", b"v".to_vec(), 50);
        assert_eq!(store.len(), 1);
        thread::sleep(Duration::from_millis(80));
        assert_eq!(store.del("temp"), None);
        assert_eq!(store.len(), 0);
    }

    // ─── Deterministic TTL contract tests (Contract A) ──────────────────
    //
    // All tests in this section use the test-only frozen clock
    // (`test_support::freeze_clock_at`) — no sleeping, no sweeper thread
    // (none exists in lib tests), fully deterministic boundaries.
    //
    // ANTI-VACUOUS RULE: every assertion uses a freshly planted entry
    // (via `load_entry`, which bypasses expiry filtering) with physical
    // presence (`len()`) asserted BEFORE the read, so each read method is
    // tested against its own physically-present expired entry, not an
    // already-purged key.

    mod contract {
        use super::super::test_support::{freeze_clock_at, frozen_now};
        use super::*;
        use crate::{is_expired_at, Entry, TtlInfo};

        const T0: u64 = 1_000_000; // arbitrary frozen "now"
        const DEADLINE: u64 = T0 + 10_000; // entry deadline under test

        /// Plant an entry directly, bypassing expiry filtering, then assert
        /// the expected total physical count. This guarantees the subsequent
        /// read is testing expiry *filtering*, not an ordinary missing key.
        fn plant(store: &ShardedKV, key: &str, expires_at: Option<u64>) {
            let before = store.len();
            store.load_entry(key.to_string(), b"v".to_vec(), expires_at);
            assert_eq!(
                store.len(),
                before + 1,
                "{key} must be physically present pre-read"
            );
        }

        // ── Clock guard mechanics ─────────────────────────────────────

        #[test]
        fn clock_override_represents_timestamp_zero() {
            // Option<u64> means an override at timestamp 0 is representable
            // (0 is not treated as "unset").
            let _g = freeze_clock_at(0);
            assert_eq!(frozen_now(), Some(0));
            assert_eq!(now_ms(), 0);
        }

        #[test]
        fn clock_guard_restores_previous_state() {
            let _g1 = freeze_clock_at(111);
            assert_eq!(frozen_now(), Some(111));
            {
                let _g2 = freeze_clock_at(222);
                assert_eq!(frozen_now(), Some(222));
            }
            // Nested guard restored the outer override.
            assert_eq!(frozen_now(), Some(111));
        }

        #[test]
        fn clock_override_affects_now_ms() {
            let _g = freeze_clock_at(123_456);
            assert_eq!(now_ms(), 123_456);
        }

        #[test]
        fn spawned_threads_do_not_inherit_frozen_clock() {
            let _g = freeze_clock_at(123_456);
            let child = thread::spawn(now_ms);
            let child_now = child.join().unwrap();
            // The child must see the REAL clock: far from our frozen value,
            // and (on any machine running these tests) strictly positive.
            assert_ne!(child_now, 123_456);
            assert!(child_now > 1_000_000_000_000);
        }

        // ── Boundary triple: deadline-1 / deadline / deadline+1 ───────

        #[test]
        fn boundary_get_live_before_deadline() {
            let _g = freeze_clock_at(T0);
            let store = ShardedKV::new();
            store.set_with_ttl("k", b"v".to_vec(), DEADLINE - T0); // deadline DEADLINE
            let _g2 = freeze_clock_at(DEADLINE - 1);
            assert_eq!(store.get("k"), Some(b"v".to_vec()));
        }

        #[test]
        fn boundary_get_expired_exactly_at_deadline() {
            let _g = freeze_clock_at(DEADLINE);
            let store = ShardedKV::new();
            plant(&store, "k", Some(DEADLINE));
            assert_eq!(store.get("k"), None);
        }

        #[test]
        fn boundary_get_expired_after_deadline() {
            let _g = freeze_clock_at(DEADLINE + 1);
            let store = ShardedKV::new();
            plant(&store, "k", Some(DEADLINE));
            assert_eq!(store.get("k"), None);
        }

        #[test]
        fn boundary_contains_live_before_deadline() {
            let _g = freeze_clock_at(DEADLINE - 1);
            let store = ShardedKV::new();
            plant(&store, "k", Some(DEADLINE));
            assert!(store.contains("k"));
        }

        #[test]
        fn boundary_contains_expired_exactly_at_deadline() {
            let _g = freeze_clock_at(DEADLINE);
            let store = ShardedKV::new();
            plant(&store, "k", Some(DEADLINE));
            assert!(!store.contains("k"));
        }

        #[test]
        fn boundary_contains_expired_after_deadline() {
            let _g = freeze_clock_at(DEADLINE + 1);
            let store = ShardedKV::new();
            plant(&store, "k", Some(DEADLINE));
            assert!(!store.contains("k"));
        }

        #[test]
        fn boundary_del_returns_value_live_before_deadline() {
            let _g = freeze_clock_at(DEADLINE - 1);
            let store = ShardedKV::new();
            plant(&store, "k", Some(DEADLINE));
            assert_eq!(store.del("k"), Some(b"v".to_vec()));
        }

        #[test]
        fn boundary_del_none_exactly_at_deadline() {
            let _g = freeze_clock_at(DEADLINE);
            let store = ShardedKV::new();
            plant(&store, "k", Some(DEADLINE));
            assert_eq!(store.del("k"), None);
        }

        #[test]
        fn boundary_ttl_remaining_one_before_deadline() {
            // Most precise boundary proof: at deadline-1 the entry reports
            // exactly 1ms remaining; RemainingMs(0) is unreachable because
            // at the deadline the key is already absent.
            let _g = freeze_clock_at(DEADLINE - 1);
            let store = ShardedKV::new();
            plant(&store, "k", Some(DEADLINE));
            assert_eq!(store.ttl("k"), Some(TtlInfo::RemainingMs(1)));
        }

        #[test]
        fn boundary_ttl_none_exactly_at_deadline() {
            let _g = freeze_clock_at(DEADLINE);
            let store = ShardedKV::new();
            plant(&store, "k", Some(DEADLINE));
            assert_eq!(store.ttl("k"), None);
        }

        #[test]
        fn boundary_mget_live_before_deadline() {
            let _g = freeze_clock_at(DEADLINE - 1);
            let store = ShardedKV::new();
            plant(&store, "k", Some(DEADLINE));
            let r = store.mget(&["k".to_string()]);
            assert_eq!(r[0], Some(b"v".to_vec()));
        }

        #[test]
        fn boundary_mget_not_found_exactly_at_deadline() {
            let _g = freeze_clock_at(DEADLINE);
            let store = ShardedKV::new();
            plant(&store, "k", Some(DEADLINE));
            let r = store.mget(&["k".to_string()]);
            assert_eq!(r[0], None);
        }

        #[test]
        fn boundary_mget_not_found_after_deadline() {
            let _g = freeze_clock_at(DEADLINE + 1);
            let store = ShardedKV::new();
            plant(&store, "k", Some(DEADLINE));
            let r = store.mget(&["k".to_string()]);
            assert_eq!(r[0], None);
        }

        #[test]
        fn boundary_scan_includes_live_before_deadline() {
            let _g = freeze_clock_at(DEADLINE - 1);
            let store = ShardedKV::new();
            plant(&store, "k", Some(DEADLINE));
            let (keys, more) = store.scan("", 100, "");
            assert_eq!(keys, vec!["k"]);
            assert!(!more);
        }

        #[test]
        fn boundary_scan_omits_expired_exactly_at_deadline() {
            let _g = freeze_clock_at(DEADLINE);
            let store = ShardedKV::new();
            plant(&store, "k", Some(DEADLINE));
            let (keys, more) = store.scan("", 100, "");
            assert!(keys.is_empty());
            assert!(!more);
        }

        #[test]
        fn boundary_scan_omits_expired_after_deadline() {
            let _g = freeze_clock_at(DEADLINE + 1);
            let store = ShardedKV::new();
            plant(&store, "k", Some(DEADLINE));
            let (keys, _) = store.scan("", 100, "");
            assert!(keys.is_empty());
        }

        #[test]
        fn scan_pagination_with_expired_entries_interspersed() {
            // Pages of 2 over a..e where c is expired: pagination must skip
            // "c" and the `more` flag must reflect the LIVE key count only.
            let _g = freeze_clock_at(DEADLINE);
            let store = ShardedKV::new();
            plant(&store, "a", None);
            plant(&store, "b", Some(DEADLINE + 60_000));
            plant(&store, "c", Some(DEADLINE)); // expired at sampled now
            plant(&store, "d", None);
            plant(&store, "e", None);

            let (page1, more) = store.scan("", 2, "");
            assert_eq!(page1, vec!["a", "b"]);
            assert!(more);
            let (page2, more) = store.scan("", 2, "b");
            assert_eq!(page2, vec!["d", "e"]); // "c" skipped
            assert!(!more);
        }

        #[test]
        fn boundary_len_active_includes_live_excludes_expired() {
            let _g = freeze_clock_at(DEADLINE);
            let store = ShardedKV::new();
            plant(&store, "dead", Some(DEADLINE));
            plant(&store, "perm", None);
            assert_eq!(store.len(), 2); // physical: expired entry still there
            assert_eq!(store.len_active(), 1); // logical: only "perm"
        }

        #[test]
        fn boundary_snapshot_excludes_expired_preserves_live_deadline() {
            let _g = freeze_clock_at(DEADLINE);
            let store = ShardedKV::new();
            plant(&store, "dead", Some(DEADLINE));
            plant(&store, "live", Some(DEADLINE + 60_000));
            let snap = store.collect_for_snapshot();
            // No purge: both entries remain physically present.
            assert_eq!(store.len(), 2);
            assert_eq!(snap.len(), 1);
            assert_eq!(snap[0].0, "live");
            // Absolute deadline survives, unmodified.
            assert_eq!(snap[0].2, Some(DEADLINE + 60_000));
        }

        // ── Non-expiring and live-TTL keys unchanged ───────────────────

        #[test]
        fn permanent_entry_behaves_identically_with_frozen_clock() {
            let _g = freeze_clock_at(T0);
            let store = ShardedKV::new();
            store.set("perm", b"v".to_vec());
            assert_eq!(store.get("perm"), Some(b"v".to_vec()));
            assert!(store.contains("perm"));
            assert_eq!(store.ttl("perm"), Some(TtlInfo::Permanent));
            let (keys, _) = store.scan("", 100, "");
            assert_eq!(keys, vec!["perm"]);
        }

        // ── Overwrite / TTL-refresh semantics ──────────────────────────

        #[test]
        fn setx_overwrite_recomputes_deadline_from_overwrite_time() {
            let store = ShardedKV::new();
            let _g = freeze_clock_at(T0);
            assert!(store.set_with_ttl("k", b"v1".to_vec(), 5_000)); // deadline T0+5000
            let _g2 = freeze_clock_at(T0 + 4_000);
            assert!(store.set_with_ttl("k", b"v2".to_vec(), 5_000)); // new deadline T0+9000
                                                                     // Old deadline passed, new one not yet.
            let _g3 = freeze_clock_at(T0 + 6_000);
            assert_eq!(store.get("k"), Some(b"v2".to_vec()));
            match store.ttl("k") {
                Some(TtlInfo::RemainingMs(ms)) => assert_eq!(ms, 3_000),
                other => panic!("expected RemainingMs, got {other:?}"),
            }
        }

        #[test]
        fn setx_revives_expired_but_unremoved_entry_no_count_change() {
            // Overwriting an expired-but-physically-present key takes the
            // Occupied branch: value and deadline replaced, count unchanged.
            let _g = freeze_clock_at(DEADLINE);
            let store = ShardedKV::new();
            plant(&store, "k", Some(DEADLINE)); // expired at frozen now
            let _g2 = freeze_clock_at(DEADLINE + 100);
            assert!(store.set_with_ttl("k", b"v2".to_vec(), 5_000));
            assert_eq!(store.len(), 1); // revived in place, no new entry
            assert_eq!(store.get("k"), Some(b"v2".to_vec()));
        }

        #[test]
        fn set_clears_ttl_entry_survives_old_deadline() {
            let _g = freeze_clock_at(T0);
            let store = ShardedKV::new();
            assert!(store.set_with_ttl("k", b"v1".to_vec(), 5_000)); // deadline T0+5000
            let _g2 = freeze_clock_at(T0 + 2_000);
            assert!(store.set("k", b"v2".to_vec())); // clears TTL
            let _g3 = freeze_clock_at(T0 + 10_000); // past the old deadline
            assert_eq!(store.get("k"), Some(b"v2".to_vec()));
            assert_eq!(store.ttl("k"), Some(TtlInfo::Permanent));
        }

        #[test]
        fn setx_zero_ttl_rejected_without_mutating_existing_entry() {
            let _g = freeze_clock_at(T0);
            let store = ShardedKV::new();
            assert!(store.set_with_ttl("k", b"v1".to_vec(), 5_000));
            let _g2 = freeze_clock_at(T0 + 1_000);
            // Zero TTL rejected even on overwrite — existing entry untouched.
            assert!(!store.set_with_ttl("k", b"v2".to_vec(), 0));
            assert_eq!(store.len(), 1);
            assert_eq!(store.get("k"), Some(b"v1".to_vec()));
            match store.ttl("k") {
                Some(TtlInfo::RemainingMs(ms)) => assert_eq!(ms, 4_000), // deadline unchanged
                other => panic!("expected RemainingMs, got {other:?}"),
            }
        }

        #[test]
        fn setx_max_ttl_saturates_lives_to_end_of_representable_time() {
            let _g = freeze_clock_at(T0);
            let store = ShardedKV::new();
            assert!(store.set_with_ttl("k", b"v".to_vec(), u64::MAX));
            // expires_at saturates to u64::MAX; the entry lives at every
            // representable time below that, and expires AT it (inclusive).
            let _g2 = freeze_clock_at(u64::MAX - 1);
            assert_eq!(store.get("k"), Some(b"v".to_vec()));
            let _g3 = freeze_clock_at(u64::MAX);
            assert_eq!(store.get("k"), None);
        }

        // ── Sweep independence (Contract A with sweeper disabled) ──────
        //
        // No sweeper thread exists in these tests — but the point is the
        // reads filter on their own sampled time even when NOTHING ever
        // sweeps. Physical removal happens lazily as a side effect.

        #[test]
        fn expired_entry_never_served_with_sweeper_never_run() {
            let _g = freeze_clock_at(DEADLINE);
            let store = ShardedKV::new();
            plant(&store, "k", Some(DEADLINE)); // expired, never swept
            assert_eq!(store.get("k"), None);
            assert_eq!(store.len(), 0); // lazily removed as side effect
        }

        // ── Shared predicate sanity ────────────────────────────────────

        #[test]
        fn is_expired_at_shared_predicate_boundaries() {
            assert!(!is_expired_at(None, u64::MAX)); // permanent never expires
            assert!(!is_expired_at(Some(100), 99)); // strictly before
            assert!(is_expired_at(Some(100), 100)); // exactly at (inclusive)
            assert!(is_expired_at(Some(100), 101)); // after
            assert!(!Entry::new(vec![]).is_expired(u64::MAX));
            assert!(Entry::with_expiry(vec![], 100).is_expired(100));
        }

        // ── Capacity occupied by expired-but-unswept entries ───────────

        #[test]
        fn expired_entries_hold_capacity_until_removed() {
            // With no sweep ever running, expired entries still occupy
            // capacity slots — documented contract exclusion, tested here
            // with the frozen clock (no sleeps).
            let _g = freeze_clock_at(T0);
            let store = ShardedKV::with_max_entries(1);
            assert!(store.set_with_ttl("k", b"v".to_vec(), 5_000));
            // Expire it, no sweep, no read.
            let _g2 = freeze_clock_at(T0 + 10_000);
            assert!(!store.set("new", b"v".to_vec())); // STORE_FULL: slot still held
            assert_eq!(store.len(), 1);
            // A read frees the slot (lazy removal).
            assert_eq!(store.get("k"), None);
            assert!(store.set("new", b"v".to_vec())); // capacity reclaimed
            assert_eq!(store.len(), 1);
        }
    }
}
