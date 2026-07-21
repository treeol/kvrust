# kvr — Implementation Plan (v2, Mashūra-reviewed)

Implements all improvements from `IMPROVEMENT_REVIEW.md`. Organized into 5
cards (waves), each a self-contained commit following the Trello card workflow
(verify → plan → Mashūra review → implement → Mashūra review → commit).

## Mashūra review feedback folded in (gpt-5.5 + claude-5-fable)

1. `now_ms()` pre-epoch panic (review §3.3) was missing — added to Card 1.
2. mget write-lock re-check must be an explicit acceptance criterion.
3. Protocol boundary: use a `SnapshotSaver` trait, don't move `SnapshotManager`
   to lib. Keep `READ_TIMEOUT_SECS` in server.rs.
4. Card 3↔4 dependency: `--ping` uses frame I/O that Card 4 moves. Reorder.
5. CI healthcheck must poll+assert, not just print. Use `rustsec/audit-check`
   action instead of slow `cargo install cargo-audit`.
6. MSRV 1.70 verified by API usage (`is_some_and` = 1.70 is the highest). Only
   stable toolchain available locally; can't empirically check 1.70. Note this.
7. Add explicit tests for edge cases (overwrite-expired, full-store rejection,
   mget miss contention, oversized snapshot key, GET response size).
8. `entry(key.to_string())` allocates before full-store rejection — noted as
   acceptable tradeoff (tiny allocation on rejection path).

---

## Card 1: Core lib.rs performance + correctness fixes

**Files:** `src/lib.rs`

### 1a. Add `EntryReservation` RAII guard (closes review §1.2)
- New private struct that increments `entry_count` on creation, decrements on
  `Drop` unless `commit()` was called.
- Replaces `try_reserve_entry()` with `try_reserve_entry_guard() -> Option<EntryReservation>`.
- The guard handles BOTH paths: unlimited (`fetch_add`) and bounded (CAS loop).
- Closes the panic window between reserve and insert (for unwinding panics).

```rust
struct EntryReservation<'a> {
    count: &'a AtomicU64,
    committed: bool,
}
impl EntryReservation<'_> {
    fn commit(mut self) { self.committed = true; }
}
impl Drop for EntryReservation<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.count.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
```

### 1b. Refactor `set()` to `HashMap::entry` + reservation guard (§2.1, §1.2)
- Current: `contains_key()` + `insert()` = 2 hash lookups.
- New: `entry(key.to_string())` = 1 lookup. `Occupied` → overwrite (clears TTL),
  no count change. `Vacant` → reserve via guard, insert, commit.
- Preserves exact semantics. Note: allocates `String` before full-store
  rejection (acceptable — tiny cost on rejection path).

### 1c. Refactor `set_with_ttl()` to `HashMap::entry` + reservation guard
- Same pattern as 1b, but inserts `Entry::with_expiry()`.
- Preserves: `ttl_ms == 0` returns false early (before any lock or allocation).

### 1d. Refactor `del()` to single `remove()` (§2.2)
- Current: `get()` (expiry check) + `remove()` = 2-3 lookups.
- New: single `guard.remove(key)`, then branch on `entry.is_expired(now)`.
- Expired → decrement count, return `None`.
- Live → decrement count, return `Some(entry.value)`.
- Missing → return `None`, no count change.

### 1e. Fix `mget()` write-lock-on-miss (§2.3)
- Current: `found.is_none()` triggers write lock for both missing AND expired.
- New: tri-state read pass: `found = Some(value)` / `needs_removal = true` /
  absent (neither set). Only acquire write lock when `needs_removal`.
- **Acceptance criterion: the write-lock re-check under the write lock is
  mandatory** — without it, concurrent removal+insert would double-decrement.

### 1f. Fix `len()` doc + `Acquire`/`Relaxed` consistency (§3.2)
- Change `len()` load from `Acquire` to `Relaxed` (no Release stores exist).
- Update doc: note it's an approximate physical count under concurrent
  mutation (reserved-but-uninserted entries may be briefly counted).

### 1g. Fix `now_ms()` pre-epoch panic (§3.3)
- Current: `.expect("system clock is before UNIX epoch")` panics.
- New: `.map(|d| d.as_millis() as u64).unwrap_or(0)` — returns 0 on pre-epoch.
- This affects sweeper and snapshot threads (no `catch_unwind` there).

### 1h. New tests
- `test_set_full_store_rejects_new_key` — verify false return after refactor.
- `test_set_overwrite_expired_entry` — overwrite an expired-but-unswept key,
  verify count unchanged (edge case from Mashūra).
- `test_mget_plain_miss_no_write_lock` — mget on absent keys should not
  inflate count or block (functional regression guard).

**Verification:** `cargo test` (all 51 lib tests + new tests pass).

---

## Card 2: Server protocol guard fixes

**Files:** `src/bin/server.rs`

### 2a. Fix snapshot save truncation (§1.3)
- In `SnapshotManager::save()`, before serializing each entry:
  - `key.len() > u16::MAX as usize` → return `Err(InvalidData)`.
  - `value.len() > u32::MAX as usize` → return `Err(InvalidData)` (defensive).
- Check against format limits (u16/u32), not `MAX_FRAME_SIZE`.

### 2b. Add GET response size check (§1.4)
- In `OP_GET` dispatch, after retrieving value, use checked math:
  `1 + 4 + val.len() > MAX_FRAME_SIZE` → return `RESP_ERROR`.
- Matches existing MGET (line 458) and SCAN (line 395) patterns.

### 2c. New tests
- `test_snapshot_save_oversized_key` — insert key > 65,535 bytes via library
  API, call `SnapshotManager::save()`, expect `Err`.
- `test_get_oversized_value` — insert large value via library API, call
  `dispatch()` with GET, expect `RESP_ERROR`.

**Verification:** `cargo test` (server tests + new tests pass).

---

## Card 3: Docker healthcheck fix + socket permission docs

**Files:** `src/bin/server.rs`, `Dockerfile`, `README.md`

### 3a. Add `--ping` client mode to server binary (§1.1)
- Parse `argv` at the top of `main()`: if first arg is `--ping`, treat second
  arg as socket path.
- Connect via `UnixStream`, send PING frame (4-byte len=1 + `[OP_PING]`), read
  framed response.
- Set a read timeout (2s) on the stream so the check can't hang.
- Exit 0 on `[RESP_OK]`, exit 1 on any error (connect/write/read/protocol).
- Uses the same `read_frame`/`write_frame` from server.rs (these move to
  `protocol.rs` in Card 4; the --ping code will follow).

### 3b. Update Dockerfile healthcheck (§1.1)
- Use shell form (expands `$KVR_SOCKET_PATH`):
```dockerfile
HEALTHCHECK --interval=30s --timeout=5s --start-period=2s --retries=3 \
    CMD kvr-server --ping "$KVR_SOCKET_PATH"
```

### 3c. Fix README docs (§5.1, §5.2)
- Socket permissions: document that directory is `0700`, socket is `0600`,
  owned by `kvr:kvr`. Other containers must run as same UID or adjust perms.
- Healthcheck: update to reflect `--ping` mode (not `nc`).

### 3d. Add `--ping` integration test
- Start a minimal UDS server (reuse test helpers), spawn `kvr-server --ping`,
  assert exit code 0.

**Verification:** `cargo build --release --bin server`, test `--ping` manually.
`cargo test` for the integration test.

---

## Card 4: Protocol dispatch dedup

**Files:** New `src/protocol.rs`, `src/lib.rs`, `src/bin/server.rs`,
`src/bin/bench_wire.rs`

### 4a. Extract protocol layer to `src/protocol.rs`
- Move from `server.rs` to a new `pub mod protocol` (file: `src/protocol.rs`,
  declared in `lib.rs`):
  - Constants: `OP_*`, `RESP_*`, `MAX_FRAME_SIZE`, `SCAN_LIMIT_CAP`,
    `MGET_LIMIT_CAP`.
  - Functions: `read_frame()`, `write_frame()`, `dispatch()`.
- **Keep `READ_TIMEOUT_SECS` in `server.rs`** — it's server connection policy,
  not wire protocol.
- `dispatch()` signature uses a trait for snapshot:
```rust
pub trait SnapshotSaver {
    fn save(&self, store: &ShardedKV) -> std::io::Result<()>;
}
pub fn dispatch(frame: &[u8], store: &ShardedKV, snapshot: Option<&dyn SnapshotSaver>) -> Vec<u8>;
```
- `SnapshotManager` stays in `server.rs`, implements `SnapshotSaver`.
- Move the GET size check (Card 2b) and snapshot truncation check (Card 2a)
  logic along with dispatch/snapshot code to the protocol module — they move
  as part of the extraction.

### 4b. Refactor `bench_wire.rs` to use shared dispatch
- Remove inline dispatch logic (lines 99-175).
- Import and call `kvr::protocol::dispatch(frame, &store, None)`.
- Import `kvr::protocol::{read_frame, write_frame}` for frame I/O.
- Fixes all divergences: invalid UTF-8 returns error, frame size enforced.

### 4c. Update `--ping` code to use `protocol::read_frame`/`write_frame`
- Card 3's `--ping` used server-local frame I/O. After Card 4, update to use
  `kvr::protocol::{read_frame, write_frame}`.

**Verification:** `cargo build --bins`, `cargo test`. Run `bench_wire` to confirm
it still works. Note: benchmark numbers may differ after shared dispatch (was
missing frame size checks).

---

## Card 5: CI pipeline improvements

**Files:** `.github/workflows/ci.yml`, `Cargo.toml`

### 5a. Add `--all-targets` to clippy (§4.1)
```yaml
- name: Clippy
  run: cargo clippy --all-targets -- -D warnings
```
Note: this newly lints the ~1,888-line test file and bench code. Fix any
surfaced lints before this lands.

### 5b. Add `cargo audit` via `rustsec/audit-check` action (§4.2)
```yaml
- name: Security audit
  uses: rustsec/audit-check@v2.0.0
```
Avoids slow `cargo install cargo-audit` (it has a large dependency tree).

### 5c. Add Docker build + healthcheck test (§4.3)
- Depends on Card 3 (healthcheck fix).
- Must poll and assert `healthy` (not just print):
```yaml
docker:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - name: Build Docker image
      run: docker build -t kvr .
    - name: Run and verify healthcheck
      run: |
        docker run -d --name kvr-test \
          --health-interval=2s --health-timeout=3s --health-retries=10 \
          kvr
        for i in $(seq 1 30); do
          status=$(docker inspect --format '{{.State.Health.Status}}' kvr-test)
          if [ "$status" = "healthy" ]; then exit 0; fi
          if [ "$status" = "unhealthy" ]; then docker logs kvr-test; exit 1; fi
          sleep 1
        done
        docker logs kvr-test
        exit 1
      finally:
        docker rm -f kvr-test 2>/dev/null || true
```

### 5d. Declare MSRV (§4.4)
- Add `rust-version = "1.70"` to `Cargo.toml`.
- Highest API used: `Option::is_some_and` (stabilized 1.70.0).
- `array::from_fn` is 1.63. `Duration::ZERO` is 1.0. `saturating_add` is 1.0.
- Only stable toolchain available locally; MSRV not empirically verified
  against `cargo +1.70 check`. Note in CI.
- Add MSRV check to CI (check only, not clippy — lint sets differ by version):
```yaml
msrv:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - uses: dtolnay/rust-toolchain@1.70.0
    - name: Check MSRV
      run: cargo check --all-targets
```

### 5e. Add release build (§4.5)
```yaml
- name: Release build
  run: cargo build --release --bins
```

**Verification:** `cargo clippy --all-targets` and `cargo build --release --bins`
locally. CI validates the rest.

---

## Execution order (revised)

```
Card 1 (lib.rs core)     ← no dependencies
Card 2 (server guards)   ← no dependencies (independent of Card 1)
Card 3 (Docker health)   ← no dependencies (uses server-local frame I/O)
Card 4 (protocol dedup)  ← depends on Card 1 (lib.rs), Card 2 (guards), Card 3 (--ping)
Card 5 (CI)              ← depends on Card 3 (Docker healthcheck) + Card 4 (bench builds)
```

Implementation is sequential (one commit per card). Card 4 must come after
Cards 1-3 because it restructures the files they touch (moves dispatch to
protocol.rs, updates --ping to use protocol::frame I/O, moves guards).
