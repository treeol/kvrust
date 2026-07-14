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

### Changed

- `SCAN` now purges expired entries during shard iteration (lazy purge),
  closing the scan-only lazy hole where expired entries consumed memory and
  capacity until the sweeper or a direct key access removed them.
- `collect_for_snapshot()` now filters expired entries at save time, reducing
  snapshot file size. Load-time filtering remains as defense-in-depth.
- Simplified shard read path: replaced `is_none()`/`unwrap()` with `?` operator
  in `ShardedKV::get`.
- Updated crate-level rustdoc, `scan()`, `sweep_expired()`, and `len()` docs
  to reflect new lazy-purge methods and O(n) sweeper complexity.

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
