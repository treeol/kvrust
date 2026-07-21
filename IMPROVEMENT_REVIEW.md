# kvr Improvement Review

A comprehensive review of the `kvr` codebase (v0.1.0) for correctness,
performance, maintainability, and CI improvements. Findings are categorized by
severity and verified against the actual code and runtime behavior.

**Verification basis:** source read, `cargo test` (141 pass, 2 doctest
sandbox-artifact failures), shell checks for Docker/grep/dash behavior, and
Mashūra oracle review (2 panels).

---

## Summary

| Category | Confirmed issues |
|---|---|
| Correctness bugs | 4 |
| Performance | 3 |
| Code quality / maintainability | 3 |
| CI gaps | 5 |
| Documentation | 2 |

The codebase is well-structured for a v0.1.0 — clean sharding, thorough tests,
atomic snapshot saves, careful TTL expiry. The issues below are improvement
opportunities, not signs of systemic problems.

---

## 1. Correctness Bugs

### 1.1 Docker healthcheck is broken in three independent ways
**Severity: High** — the documented "health check using PING over UDS" does not
work.

The Dockerfile healthcheck (line 38-39):
```dockerfile
HEALTHCHECK --interval=30s --timeout=5s --start-period=2s --retries=3 \
    CMD ["/bin/sh", "-c", "printf '\\x00\\x00\\x00\\x01\\x03' | nc -U $KVR_SOCKET_PATH | head -c 5 | grep -q '\\x10'"]
```

Three independent failures, any one of which makes the check non-functional:

1. **`nc` is not installed.** The runtime image (`debian:bookworm-slim`)
   installs only `ca-certificates`. Netcat is not included by default.

2. **`printf '\xHH'` doesn't work in dash.** Debian bookworm's `/bin/sh` is
   `dash`, whose `printf` does not support `\xHH` hex escapes. Verified:
   `/bin/sh -c "printf '\x00'"` outputs the literal ASCII characters
   `\x00` (bytes `5c 78 30 30`), not the null byte. The server would receive
   20 bytes of ASCII text instead of a 5-byte binary frame.

3. **`grep -q '\x10'` is locale-dependent.** Even if the correct bytes reached
   `nc` and came back, GNU grep's interpretation of `\x10` as byte `0x10`
   depends on locale settings. In the `C` locale it matches the byte; in UTF-8
   locales behavior varies.

**Fix:** Replace the shell-based healthcheck with a tiny `--ping` client mode
in the server binary itself (e.g., `kvr-server --ping $KVR_SOCKET_PATH`), which
avoids all three issues and requires no extra packages. Alternatively, install
`netcat-openbsd` and use octal escapes (`printf '\000\000\000\001\003'`), but
the Rust binary approach is more robust and testable.

**Acceptance criterion:** CI should build the Docker image, run it, and verify
`docker inspect --format '{{.State.Health.Status}}'` reports `healthy`.

### 1.2 `entry_count` reserve-before-insert window (count leak on panic)
**Severity: Medium** — permanently inflates the count on bounded stores if a
panic occurs in a narrow window.

In `set()` (lib.rs:237-241) and `set_with_ttl()` (lib.rs:262-266):
```rust
if !self.try_reserve_entry() {
    return false; // store full
}
guard.insert(key.to_string(), Entry::new(value));
true
```

If a panic occurs between `try_reserve_entry()` (which increments
`entry_count`) and `guard.insert()`, the count is permanently inflated. The
server's `catch_unwind` in accept loops catches the panic, but the leaked count
silently persists — on a bounded store, this permanently reduces capacity.

**Realistic risk is low** today: the only code in the window is
`key.to_string()` and `HashMap::insert`, and Rust's default allocation failure
behavior is abort (not unwind). But the design lacks rollback safety, and the
upcoming `entry()` refactor (§2.1) is the natural place to fix it.

**Fix:** Use an RAII reservation guard that decrements on `Drop` unless
explicitly committed:
```rust
struct Reservation<'a> {
    count: &'a AtomicU64,
    committed: bool,
}
impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.count.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
```

### 1.3 Snapshot save silently truncates oversized key/value lengths
**Severity: Medium** — produces corrupt snapshots for library API users.

In `SnapshotManager::save()` (server.rs:800-807):
```rust
let key_len = key.len() as u16;   // silently truncates if > 65535
let val_len = value.len() as u32; // silently truncates if > 4GiB
```

The wire protocol limits keys to u16 and frames to 1 MiB, but the library API
(`ShardedKV::set`) accepts arbitrary `String` keys and `Vec<u8>` values. A
direct library user — or a snapshot loaded with mismatched config — could
insert keys longer than 65,535 bytes. Saving would silently truncate the length
field, producing a corrupt snapshot file.

**Fix:** Return an error if any key or value exceeds the format limits:
```rust
if key.len() > u16::MAX as usize {
    return Err(io::Error::new(InvalidData, "key too long for snapshot format"));
}
```

### 1.4 GET response doesn't enforce `MAX_FRAME_SIZE`
**Severity: Low** — only exploitable via library API or snapshot load.

MGET (server.rs:458) and SCAN (server.rs:395) check response size against
`MAX_FRAME_SIZE` and return `RESP_ERROR` if exceeded. GET (server.rs:239-248)
does not:
```rust
match store.get(&key_str) {
    Some(val) => {
        let mut resp = Vec::with_capacity(1 + 4 + val.len());
        resp.push(RESP_OK);
        resp.extend_from_slice(&(val.len() as u32).to_be_bytes());
        resp.extend_from_slice(&val);
        resp  // no size check
    }
    ...
}
```

Normally SET requests are capped by the request frame size limit (1 MiB), so
values can't exceed ~1 MiB via the wire protocol. But values can enter through
the library API or snapshot load. If a value > 1 MiB exists, GET emits an
oversized response frame.

**Fix:** Add a size check before building the response, or document that
`MAX_FRAME_SIZE` is request-only.

---

## 2. Performance

### 2.1 `set()` / `set_with_ttl()` do 2 hash lookups (should be 1)
**Impact: Medium** — the hot write path does redundant work.

Current (lib.rs:231-241):
```rust
if guard.contains_key(key) {      // lookup 1
    guard.insert(key.to_string(), Entry::new(value));
    return true;
}
// ...
guard.insert(key.to_string(), Entry::new(value)); // lookup 2
```

`HashMap::entry` does a single lookup and returns a view that can insert
directly. This also naturally closes the panic-window (§1.2) when combined
with a reservation guard:
```rust
match guard.entry(key.to_string()) {
    MapEntry::Occupied(mut e) => { e.insert(Entry::new(value)); true }
    MapEntry::Vacant(e) => {
        let res = self.try_reserve_entry_guard()?;
        e.insert(Entry::new(value));
        res.commit();
        true
    }
}
```

### 2.2 `del()` does 2 lookups (should be 1)
**Impact: Low-Medium**

Current (lib.rs:300-312):
```rust
if let Some(entry) = guard.get(key) {    // lookup 1 (expiry check)
    if entry.is_expired(now) {
        guard.remove(key);                 // lookup 2
        ...
    }
}
let result = guard.remove(key);            // lookup 3 (or 2 if not expired)
```

A single `remove()` returns the `Entry`, then branch on `is_expired`:
```rust
match guard.remove(key) {
    Some(entry) if entry.is_expired(now) => {
        self.entry_count.fetch_sub(1, Ordering::Relaxed);
        None
    }
    Some(entry) => {
        self.entry_count.fetch_sub(1, Ordering::Relaxed);
        Some(entry.value)
    }
    None => None,
}
```

### 2.3 `mget()` takes a write lock on every plain miss
**Impact: Medium-High under miss-heavy workloads** — unnecessary write-lock
contention.

Current (lib.rs:517-526):
```rust
if found.is_none() {
    // Slow path: entry is expired or missing. If expired, remove it.
    let mut guard = shard.write();
    if let Some(entry) = guard.get(key) {
        if entry.is_expired(now) {
            guard.remove(key);
            ...
        }
    }
}
```

`found.is_none()` is true for both **missing** keys (entry doesn't exist) and
**expired** keys (entry exists but is expired). For missing keys — which is the
common case under miss-heavy load — the write lock is unnecessary. Only expired
entries need the write lock for removal.

`get()` handles this correctly (early `?` return on missing). `mget` should
distinguish "absent" from "expired" in the read pass:
```rust
let mut found = None;
let mut needs_removal = false;
{
    let guard = shard.read();
    if let Some(entry) = guard.get(key) {
        if entry.is_expired(now) {
            needs_removal = true;  // expired — needs write lock
        } else {
            found = Some(entry.value.clone());
        }
    }
    // if entry is None, found stays None, needs_removal stays false — no write lock
}
if needs_removal {
    let mut guard = shard.write();
    // re-check and remove...
}
```

---

## 3. Code Quality / Maintainability

### 3.1 `bench_wire.rs` duplicates server dispatch with divergent behavior
**Impact: Medium** — the benchmark server's protocol handling has silently
diverged from the real server.

`bench_wire.rs` reimplements the dispatch logic (SET/GET/PING) inline rather
than calling the server's `dispatch()`. Divergences:

| Behavior | Real server | bench_wire |
|---|---|---|
| Invalid UTF-8 key | Returns `RESP_ERROR` | Maps to `""` via `unwrap_or("")` |
| Frame size limit | `MAX_FRAME_SIZE` (1 MiB) | None |
| Connection cap | 256 + semaphore | None |
| Read timeout | 30s | None |
| Opcode coverage | All 10 | Only SET/GET/PING |
| Server cleanup | `drop(server_thread)` detaches | N/A — process exits |

**Fix:** Move `dispatch()`, `read_frame()`, `write_frame()`, and the protocol
constants into a shared module (e.g., `src/protocol.rs` or a `pub mod` in
`lib.rs`). Both `server.rs` and `bench_wire.rs` call the same implementation.
Note: `dispatch()` currently takes `Option<Arc<SnapshotManager>>`, so this
requires decoupling snapshot handling from the protocol layer.

### 3.2 `len()` documentation is stronger than implementation under concurrency
**Impact: Low** — misleading docs, not a bug.

`len()` (lib.rs:590) loads `entry_count` with `Acquire`, while all mutations
use `Relaxed`. The `Acquire` load buys nothing without `Release` stores — it's
cosmetically inconsistent. More importantly, because inserts increment
`entry_count` *before* map insertion (via `try_reserve_entry`), `len()` can
briefly include reserved-but-not-yet-inserted entries under concurrent
mutation.

The doc says "Returns the current number of entries in the store" — too strong
for concurrent observation. Should say "approximate physical count" or similar.

### 3.3 `now_ms()` can panic on pre-epoch system clock
**Impact: Very Low** — extremely rare, but a panic path.

`now_ms()` (lib.rs:71-76):
```rust
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX epoch")
        .as_millis() as u64
}
```

If the system clock is set before 1970, this panics. The server's
`catch_unwind` in client threads catches it, but the sweeper and snapshot
threads do not have `catch_unwind`. Acceptable for a single-container trusted
environment, but worth noting.

---

## 4. CI Gaps

### 4.1 Clippy doesn't lint tests/benches
**Current:** `cargo clippy -- -D warnings`
**Should be:** `cargo clippy --all-targets -- -D warnings`

Without `--all-targets`, clippy only lints lib + bins, skipping the 1,888-line
test file and benchmark code. Test/bench code can accumulate lint violations
that CI won't catch.

### 4.2 No `cargo audit`
Only 2 dependencies (`parking_lot`, `signal-hook`), but advisories can still
appear. A `cargo audit` step is cheap insurance.

### 4.3 No Docker build/test in CI
The healthcheck bug (§1.1) would have been caught by a CI step that builds
the image and verifies the healthcheck works.

### 4.4 No MSRV declared
`Cargo.toml` has no `rust-version` field. CI tests on `stable` only. Should
either declare an MSRV (e.g., `rust-version = "1.70"`) and test against it, or
explicitly document "stable only."

### 4.5 No release build in CI
Tests run in debug mode only. A `cargo build --release --bins` step catches
optimization-specific issues (e.g., overflow checks differ between debug and
release).

---

## 5. Documentation

### 5.1 Docker socket permissions contradiction
README says other containers can connect via shared volume:
```
# Other containers can connect via the shared volume:
# docker run --rm -v kvr-socket:/run/kvr ... connect to /run/kvr/kvr.sock
```

But the Dockerfile sets the socket directory to `0700` and the socket file to
`0600`, owned by `kvr:kvr`. Other containers running as a different UID cannot
read or connect to the socket. The README should document the UID requirement
or provide a group-based access pattern.

### 5.2 README claims healthcheck works
README: "A health check using PING over UDS runs every 30 seconds." — but it's
broken (§1.1). Must be updated when fixed.

---

## Priority Order

1. **Fix Docker healthcheck** (§1.1) — broken in production, 3 independent bugs
2. **Fix `mget()` write-lock-on-miss** (§2.3) — real contention under miss-heavy load
3. **Refactor `set()`/`set_with_ttl()` to `entry()`** (§2.1) — also closes §1.2
4. **Refactor `del()` to single `remove()`** (§2.2)
5. **Fix snapshot save truncation** (§1.3) — silent data corruption
6. **Add GET response size check** (§1.4)
7. **Share protocol dispatch between server and bench_wire** (§3.1)
8. **Add `--all-targets` to clippy, `cargo audit`, Docker build to CI** (§4)
9. **Fix Docker permission docs** (§5.1)
10. **Minor: `len()` docs, `now_ms()` panic, `Acquire`/`Relaxed` consistency** (§3.2, §3.3)
