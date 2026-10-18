# AGENTS.md

Guidance for AI coding agents (and humans) working in this repository.

## Language

- Rust (edition 2021). MSRV 1.70 — new code must compile with `cargo +1.70.0 check --all-targets` (no newer std APIs at runtime; test-only code may use `LocalKey::with`-style MSRV-safe workarounds).

## Build / Test / Lint

```sh
cargo build --bins            # builds server, bench, bench_wire
cargo test                    # full suite: lib + server-bin + doctests (CI runs exactly this)
cargo clippy --all-targets -- -D warnings
cargo fmt --check
cargo +1.70.0 check --all-targets   # MSRV gate (uses rustup toolchain 1.70.0)
```

`cargo test` must stay fully green — CI (`.github/workflows/ci.yml`) enforces: fmt check, clippy `-D warnings`, tests, release build, MSRV check, `cargo audit`, and a Docker build + healthcheck run. Match all of these locally before committing; ask before running anything broader than `cargo test` (e.g. release-mode full suites).

## Architecture

Single crate `kvr` (~3000 lines, 2 runtime deps: `parking_lot`, `signal-hook`). Synchronous std threads throughout — no async runtime; do not introduce one.

- `src/lib.rs` — core store. `ShardedKV`: 16 shards, each a `parking_lot::RwLock<HashMap<String, Entry>>`. TTL via absolute UNIX-epoch-ms deadlines. 100K-entry capacity cap (default) enforced by atomic counter + RAII `EntryReservation`. Includes a `#[cfg(test)]`-only thread-local frozen clock (`freeze_clock_at`) used for deterministic TTL tests.
- `src/protocol.rs` — wire protocol: frame read/write (1 MiB cap), 10 opcodes (SET, GET, DEL, PING, EXISTS, SETX, SCAN, MGET, SAVE, TTL). Includes dispatch and wire tests.
- `src/bin/server.rs` — thread-per-connection UDS + optional TCP server, background TTL sweeper, signal handling, `KVR_*` env-var config, snapshot save/restore. Server tests live in `src/bin/server/tests.rs` (module include) and run as part of `cargo test`.
- `src/bin/bench.rs`, `src/bin/bench_wire.rs` — in-process and wire-level benchmarks (manual; not part of CI).

### TTL expiry contract (load-bearing — do not weaken)

**Strict read-time filtering (Contract A):** no key- or value-returning operation (`get`, `contains`, `del`, `ttl`, `mget`, `scan`, `len_active`, `collect_for_snapshot`, and the restart restore filter) ever serves an entry whose deadline is **at or before** (inclusive) the time sampled by that operation — regardless of sweeper timing. The sweep is space reclamation only, never the correctness mechanism. Known exclusions, documented in `src/lib.rs`: `len`/`is_empty` count physical entries; `scan(limit=0)` returns no keys but can hold a capacity slot; wall-clock rollback can resurrect entries. TTL tests must use the frozen clock (`freeze_clock_at`) or plant expired entries directly — **never sleep-based tests** (they prove nothing about sweep independence). Deadlines are absolute and survive snapshot round-trips; `restore_snapshot_at` filters expired records at restart.

### Snapshot format

Hand-rolled binary: `KVR1` magic + CRC32, written to a tmp file then fsync + atomic rename. `src/bin/server.rs` documents the layout in a `//` doc comment (note: that comment has pre-existing unclosed `<key>`/`<val>` HTML tags that trip `rustdoc -D warnings` — not a CI gate).

## Server configuration

`KVR_SOCKET_PATH`, `KVR_TCP_ADDR`, `KVR_MAX_CONNECTIONS`, `KVR_MAX_ENTRIES`, `KVR_SWEEP_INTERVAL_SECS` (default 30), `KVR_SNAPSHOT_PATH`, `KVR_SNAPSHOT_ON_SHUTDOWN`, `KVR_SNAPSHOT_INTERVAL_SECS`. Numeric vars reject invalid values at startup (refuse to start); string vars are lenient.

## Conventions

- **Docs are part of the contract.** Behavioral guarantees (esp. TTL semantics) are stated in crate docs, per-method docs, and README — keep all three in sync when changing behavior. After changes, check `README.md`, `CHANGELOG.md`, and doc comments that the change makes stale.
- **CHANGELOG.md** gets an entry for user-visible changes.
- Commits follow `type: subject` prefixes (`feat:`, `fix:`, `docs:`, `test:`, `chore:`, and compounds like `docs,test:`).
- Enhancement-only mindset: the store is designed for ≤100K entries, trusted co-container IPC (auth = 0600 socket perms), no replication/clustering. Don't add heavyweight machinery.
- Don't commit local scratch files that aren't part of the task; check `git status --short` before staging.

## Infrastructure

- Docker (`Dockerfile`): single-container image, healthcheck exercises the socket.
- GitHub Actions CI: `.github/workflows/ci.yml` (jobs: check, msrv, audit, docker).
