# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `TTL` protocol command (opcode `0x09`) — query remaining TTL for a key.
  Returns permanent/remaining-ms, or NOT_FOUND for missing/expired keys.
- `TtlInfo` enum and `ShardedKV::ttl()` library method.
- `ShardedKV::len_active()` method — O(n) count of non-expired entries.
- `--ping` client mode in the `server` binary (`kvr-server --ping <socket>`),
  used by the Docker HEALTHCHECK to replace the broken nc/printf/grep chain.
- Shared protocol module `src/protocol.rs` (`dispatch`, `read_frame`,
  `write_frame`, opcode/response constants, `SnapshotSaver` trait) so the
  server and `bench_wire` can no longer diverge on protocol behavior.
- CI: clippy `--all-targets`, MSRV check (Rust 1.70), `cargo audit`, Docker
  build + healthcheck verification, and a `--release` build.
- `rust-version = "1.70"` MSRV declaration in `Cargo.toml`.

### Changed

- Hot-path performance: `set()`/`set_with_ttl()` now use `HashMap::entry`
  (one lookup instead of two) and `del()` uses a single `remove()`.
- `mget()` no longer takes a write lock on plain misses — only expired
  entries trigger the write-lock removal path.
- Correctness: an `EntryReservation` RAII guard closes the panic window
  between reserving an entry slot and inserting it; snapshot save now rejects
  oversized keys/values instead of silently truncating them; GET responses
  enforce `MAX_FRAME_SIZE`; protocol dispatch uses checked arithmetic against
  overflow; `now_ms()` returns 0 instead of panicking on a pre-epoch clock.
- Docker healthcheck switched from `nc`/`printf`/`grep` to the `--ping` client
  mode (the old approach was broken three ways: `nc` not installed, dash
  `printf` lacking `\xHH`, locale-dependent `grep`).
- Snapshot durability: fsync errors are propagated, the `.tmp` file is written
  with `0600` permissions, and the parent directory is fsynced after rename.
- Configuration is fail-fast: malformed `KVR_*` values refuse startup instead
  of silently falling back to defaults.
- Signal handling uses the safe `signal_hook::flag::register` API.
- `bench_wire` now calls the shared `protocol::dispatch` instead of a divergent
  inline reimplementation.
- `SCAN` purges expired entries during shard iteration, and
  `collect_for_snapshot()` filters expired entries at save time.
- Simplified the `ShardedKV::get` shard read path (`?` operator instead of
  `is_none()`/`unwrap()`); updated crate-level rustdoc and `scan()`/
  `sweep_expired()`/`len()` docs for the new lazy-purge behavior.
- `len()` documentation corrected to "approximate physical count" (the load
  was also changed from `Acquire` to `Relaxed`).

## [0.1.0] - 2026-07-14

### Added

- Sharded in-memory key-value store (`ShardedKV`) with 16 `RwLock`-protected
  `HashMap` shards.
- Unix Domain Socket (UDS) server with length-prefixed framed binary protocol.
- Wire protocol opcodes: SET, GET, DEL, PING, EXISTS, SETX, SCAN, MGET, SAVE.
- TTL with hybrid expiry: lazy eviction on access + active background sweeper.
- Snapshot persistence with CRC32 integrity, atomic rename, and
  expired-entry filtering on load.
- Configurable memory bounds with atomic CAS enforcement.
- Signal handling (SIGTERM/SIGINT) for graceful shutdown.
- Optional TCP listener for debugging.
- Docker support with non-root user and health check.
- CI pipeline (fmt, clippy `-D warnings`, test).
- 41 library tests, 81 server tests, 1 doctest.
