# kvr

A lightweight, sharded, concurrent in-memory key-value store in Rust with a
Unix Domain Socket (UDS) server for IPC in a single sandbox container.

[![CI](https://github.com/treeol/kvrust/actions/workflows/ci.yml/badge.svg)](https://github.com/treeol/kvrust/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

## Architecture

- **ShardedKV** — 16 shards, each a `HashMap` protected by a `parking_lot::RwLock`. Keys are hashed to a shard via Rust's default `Hasher` + bitmask (power-of-two shard count). Supports configurable max entry count with atomic CAS-based enforcement.
- **UDS server** — listens on `/tmp/kvr.sock` (configurable) with a length-prefixed framed binary protocol. Thread-per-connection with panic-safe permit release. Optional secondary TCP listener for debugging.
- **Signal handling** — SIGTERM/SIGINT trigger graceful shutdown with socket file cleanup.

## Protocol

Frames are length-prefixed (4-byte big-endian length + payload).

### Request opcodes

| Opcode | Name | Frame format |
|--------|------|-------------|
| `0x00` | SET | `<2B key-len:BE><key><4B val-len:BE><val>` |
| `0x01` | GET | `<2B key-len:BE><key>` |
| `0x02` | DEL | `<2B key-len:BE><key>` |
| `0x03` | PING | (no payload) |
| `0x04` | EXISTS | `<2B key-len:BE><key>` |
| `0x05` | SETX | `<2B key-len:BE><key><4B val-len:BE><val><8B ttl-ms:BE>` |
| `0x06` | SCAN | `<2B prefix-len:BE><prefix><2B limit:BE><2B cursor-len:BE><cursor>` |
| `0x07` | MGET | `<2B count:BE>` then count × `<2B key-len:BE><key>` |
| `0x08` | SAVE | (no payload) |
| `0x09` | TTL | `<2B key-len:BE><key>` |

### Response bytes

| Byte | Meaning |
|------|---------|
| `0x10` | OK (SET success, GET found, PING, EXISTS found) |
| `0x11` | DELETED (DEL removed a key) |
| `0x12` | NOT_FOUND (GET/DEL/EXISTS on missing key) |
| `0x13` | STORE_FULL (SET rejected — max entry count reached) |
| `0xFF` | ERROR (bad frame, unknown opcode, invalid UTF-8, trailing bytes, SETX with ttl=0, MGET response overflow, SAVE failure) |

GET responses include a 4-byte big-endian value length after the status byte.

### SCAN response

SCAN returns `0x10` followed by `<2B count:BE>` then `count` × `<2B key-len:BE><key>`,
then a `<1B more-flag>` (`0x01` if more pages exist, `0x00` otherwise). Keys are
returned (not values) in lexicographic order. The cursor is the last key of the
previous page (opaque to the client; empty cursor = start). The server caps
`limit` at 1024. Empty prefix scans all keys. Expired keys are never returned.
`limit=0` returns an empty result with `more=0x00`.

### MGET response

MGET returns `0x10` then per requested key **in request order**: `<1B found-flag>`
(`0x01` = found, `0x00` = not found) followed by `<4B val-len:BE><val>` when found
(val-len/val omitted when not found). Maximum 256 keys per request. If the
assembled response would exceed 1 MiB, the server returns `0xFF` (use smaller batches).

### TTL response

TTL (0x09) queries the TTL of a key. Request format is identical to GET:
`<2B key-len:BE><key>`. Response is `0x12` (NOT_FOUND) if the key is missing or
expired, or `0x10` (OK) followed by:

- `<1B ttl-type>` — `0x00` for permanent (no TTL), `0x01` for keys with a TTL.
- If ttl-type is `0x01`: `<8B remaining-ms:BE>` — remaining milliseconds until
  expiry (computed server-side, so no client/server clock skew).

Expired keys encountered during TTL are lazily removed (same as GET).

### Snapshot persistence

SAVE (0x08) triggers a synchronous snapshot. Response is `0x10` on success,
`0xFF` on failure (path not configured, IO error). The snapshot file format
is hand-rolled binary (no serde/bincode):

```
Magic: b"KVR1" (4 bytes)
Entry count: <8B count:BE>
Entries: count × (<2B key-len:BE><key><4B val-len:BE><val><8B expires-at:BE>)
  expires-at is 0 for no TTL (None), otherwise UNIX epoch milliseconds.
CRC32: <4B CRC32:BE> of everything after the magic.
```

Save procedure: acquire read locks on ALL 16 shards simultaneously (point-in-time
view), serialize, release locks, write to `<path>.tmp`, fsync, atomic rename to
`<path>`, fsync parent directory. A crash mid-save never corrupts or removes an
existing valid snapshot.

Load on startup: if `KVR_SNAPSHOT_PATH` is set and the file exists, entries are
loaded, **skipping entries whose `expires_at` is already past**. If the file is
corrupted (bad magic, truncation, CRC mismatch), the server refuses to load,
logs the error, and starts empty. Never half-loads.

When a snapshot contains more entries than `KVR_MAX_ENTRIES` (config changed
between runs, or snapshot taken under 0=unlimited loaded under a bounded config),
all entries are loaded anyway — never silently drop persisted data. `entry_count`
reflects the true loaded count, which may exceed `max_entries`. New SETs for
non-existing keys return `STORE_FULL` until expiry/deletes bring the count under max.

### TTL and expiry

SETX (0x05) sets a key-value pair with a relative TTL. The server computes
`expires_at = now_ms + ttl_ms` (UNIX epoch milliseconds). `ttl_ms` must be > 0;
a value of 0 returns `0xFF`. Response semantics are identical to SET (`0x10`
or `0x13` STORE_FULL).

Plain SET (0x00) creates a permanent entry (no TTL) and **overwrites any
existing TTL** on that key — the entry becomes permanent.

Expired entries are handled two ways:

- **Lazy**: GET, EXISTS, DEL, MGET, SCAN, and TTL treat an expired entry as
  absent (NOT_FOUND). The entry is removed at that moment with an entry-count
  decrement, freeing capacity. SCAN additionally purges all expired entries it
  encounters during shard iteration, not just those matching the prefix/cursor.
- **Active**: a background sweeper thread makes a full pass over all 16 shards
  every `KVR_SWEEP_INTERVAL_SECS` seconds (default 30, 0 = disabled). Per-shard
  write locks are held only per shard, never globally. The sweeper is O(n) — it
  scans all entries with no expiry index. At the designed scale (≤100K entries)
  this is acceptable. The sweeper respects the shutdown signal and exits cleanly.

TTL timestamps use `SystemTime` (wall clock) rather than a monotonic clock.
This is intentional: expiry timestamps must be comparable to real-world time
for snapshot portability across restarts.

### Limits

- **Max frame size**: 1 MiB (payload length, not including the 4-byte length prefix)
- **Max key length**: 65,535 bytes (u16)
- **Max value length**: Limited by `MAX_FRAME_SIZE` minus protocol overhead (opcode + key-length field + key + value-length field). For a typical short key, approximately 1 MiB.
- **Max entries**: 100,000 by default (configurable via `KVR_MAX_ENTRIES`, 0 = unlimited)
- **SCAN limit**: 1024 keys per request (server caps client-specified limit)
- **MGET limit**: 256 keys per request

### Example flow

```
Client → PING
Server ← 0x10

Client → SET "hello" "world"
Server ← 0x10

Client → GET "hello"
Server ← 0x10 0x00 0x00 0x00 0x05 0x77 0x6F 0x72 0x6C 0x64
         (OK, len=5, "world")

Client → EXISTS "hello"
Server ← 0x10

Client → GET "missing"
Server ← 0x12

Client → DEL "hello"
Server ← 0x11

Client → SETX "temp" "ephemeral" 3600000
Server ← 0x10

Client → GET "temp"
Server ← 0x10 0x00 0x00 0x00 0x09 0x65 0x70 0x68 0x65 0x6D 0x65 0x72 0x61 0x6C
         (OK, len=9, "ephemeral")

Client → GET "expired-key"
Server ← 0x12

Client → TTL "temp"
Server ← 0x10 0x01 0x00 0x00 0x00 0x00 0x00 0x36 0xEE 0x80
         (OK, has TTL, remaining=3600000ms)

Client → TTL "hello"
Server ← 0x12  (NOT_FOUND — key was deleted)

Client → TTL "permanent-key"
Server ← 0x10 0x00
         (OK, permanent)
```

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KVR_SOCKET_PATH` | `/tmp/kvr.sock` | UDS socket path |
| `KVR_TCP_ADDR` | (disabled) | Optional secondary TCP listener (debug only — unauthenticated) |
| `KVR_MAX_ENTRIES` | `100000` | Max key count (0 = unlimited) |
| `KVR_MAX_CONNECTIONS` | `256` | Max concurrent connections |
| `KVR_SWEEP_INTERVAL_SECS` | `30` | Sweeper interval in seconds (0 = disabled) |
| `KVR_SNAPSHOT_PATH` | (disabled) | Path for snapshot persistence (empty = disabled; SAVE returns `0xFF`) |
| `KVR_SNAPSHOT_ON_SHUTDOWN` | `true` | Save snapshot on graceful shutdown (when path set) |
| `KVR_SNAPSHOT_INTERVAL_SECS` | `0` | Periodic snapshot interval in seconds (0 = disabled) |

## Building

```bash
cargo build --release
```

### Installation from source

```bash
cargo install --path .
```

This installs the `server`, `bench`, and `bench_wire` binaries.

## Running

```bash
# Start the UDS server
cargo run --release --bin server

# Run the in-process benchmark (direct store access, no network)
cargo run --release --bin bench

# Run the UDS wire benchmark (SET/GET over Unix Domain Sockets)
cargo run --release --bin bench_wire

# Run tests
cargo test
```

### UDS wire benchmark results

On a typical development machine (4 threads, 200k ops, 20% writes / 80% reads):

```
── kvr UDS wire benchmark ──
Threads:       4
Ops/thread:    50000
Total ops:     200000
Elapsed:       0.447s
Throughput:    446944 ops/sec
Avg latency:   2.24 µs/op
```

### In-process benchmark results

Tested on an 8-core notebook with 64 GB RAM (8 threads, 500K ops/thread, 100K pre-populated keys):

```
── kvr in-process benchmark ──
Threads:       8
Ops/thread:    500000
Pre-populated: 100000 keys

SET           4000000 ops | 0.510s |      7839208 ops/sec |     0.13 µs/op
GET           4000000 ops | 0.266s |     15024723 ops/sec |     0.07 µs/op
EXISTS        4000000 ops | 0.270s |     14831734 ops/sec |     0.07 µs/op
DEL           8000000 ops | 0.672s |     11905118 ops/sec |     0.08 µs/op
SETX          4000000 ops | 0.435s |      9192724 ops/sec |     0.11 µs/op
TTL           4000000 ops | 0.250s |     16011777 ops/sec |     0.06 µs/op
MGET(10)      4000000 ops | 2.310s |      1731902 ops/sec |     0.58 µs/op | 17319021 keys/sec
SCAN(100)       80000 ops | 86.580s |         924 ops/sec |  1082.25 µs/op
SWEEP          100000 ops | 0.230s |      435414 ops/sec |     2.30 µs/op | removed=100000
LEN_ACTIVE     110000 ops | 0.006s |    17181033 ops/sec |     0.06 µs/op | active=100000

── Mixed workload (20% writes / 80% reads) ──
MIXED         4000000 ops | 3.438s |      1163526 ops/sec |     0.86 µs/op
```

Single-key operations (GET, TTL, EXISTS) achieve 14–16M ops/sec. MGET batches
10 keys per call at 1.7M ops/sec (17.3M keys/sec). SCAN is O(n) per call —
~1ms per scan over 100K entries. SWEEP purges 100K expired entries in 230ms.

## Docker

```bash
# Build the image
docker build -t kvr .

# Run the container
docker run -d \
  --name kvr \
  -v kvr-socket:/run/kvr \
  kvr

# Other containers can connect via the shared volume:
# docker run --rm -v kvr-socket:/run/kvr ... connect to /run/kvr/kvr.sock
```

The container runs as a non-root user (`kvr`), with the socket directory at `/run/kvr/` (permissions `0700`). The socket file itself has permissions `0600`. A health check using PING over UDS runs every 30 seconds via `kvr-server --ping`.

> **Note:** The socket directory (`0700`) and socket file (`0600`) are owned by `kvr:kvr`. Other containers connecting via the shared volume must run as the same UID. The `kvr` user's UID is image-dependent — check it with `docker run --rm kvr id -u kvr`, then run the client container with that numeric UID (e.g., `--user <uid>:<uid>`). Alternatively, build a custom image or entrypoint that relaxes the permissions.

## Security model

- **Trusted co-container processes only** — any process that can reach the socket path is trusted to read/write all keys.
- Socket file permissions are `0600` (owner read/write only).
- No authentication or authorization beyond filesystem permissions.
- Optional TCP listener is **unauthenticated** — use for debugging on loopback only.

## Non-goals

- **No replication or clustering** — single-node only.
- **No async runtime** — synchronous std threads.
- **No shared memory transport** — UDS is sufficient for single-container IPC.

## Tests

- **51 library tests** — basic CRUD, empty values, large values, concurrent access, memory bounds, entry counting, TTL (lazy expiry, sweeper, SET-after-SETX, capacity freed, concurrent races, ttl command), SCAN (basic, pagination, prefix, expired exclusion, expired purge, capacity freed, limit-zero), MGET (mixed, ordering, expired exclusion), snapshot (collect_for_snapshot, load_entry), len_active
- **90 server tests** — protocol unit tests (all 10 opcodes, error conditions, trailing bytes, invalid UTF-8, SETX/SCAN/MGET/SAVE/TTL edge cases), sweeper tests, snapshot tests (save/load roundtrip, expired-at-save filtered, corrupted file, bad magic, truncated file, .tmp-crash, concurrent save, over-capacity load, SAVE dispatch), TCP wire tests, UDS wire tests (roundtrip, large value, errors, SETX, SCAN pagination/prefix, MGET mixed/overflow, TTL roundtrip)
- **2 doctests** — crate-level usage example and ShardedKV usage example (set, get, set_with_ttl)

## Versioning

This project follows [Semantic Versioning](https://semver.org/). Until 1.0,
breaking changes may occur in minor releases.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for
guidelines. All contributions are subject to the CI quality gate (`cargo fmt`,
`cargo clippy -D warnings`, `cargo test`).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full text.

## Security

See [SECURITY.md](SECURITY.md) for vulnerability reporting guidelines.
- **CI** — GitHub Actions runs `cargo fmt --check`, `cargo clippy -- -D warnings`, and `cargo test` on every push and PR.
