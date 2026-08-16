//! # kvr protocol — wire protocol for the UDS server
//!
//! Shared protocol layer used by both the `server` binary and `bench_wire`.
//! Extracting this into a library module prevents the benchmark and server
//! from diverging on protocol behavior (UTF-8 handling, frame size limits,
//! error codes).

use crate::ShardedKV;
use std::io::{Read, Write};

/// Maximum frame size in bytes (1 MiB). Prevents OOM from oversized frames.
pub const MAX_FRAME_SIZE: usize = 1 << 20;

/// Opcodes for the framed protocol (client→server requests)
pub const OP_SET: u8 = 0;
pub const OP_GET: u8 = 1;
pub const OP_DEL: u8 = 2;
pub const OP_PING: u8 = 3;
pub const OP_EXISTS: u8 = 4;
pub const OP_SETX: u8 = 5;
pub const OP_SCAN: u8 = 6;
pub const OP_MGET: u8 = 7;
pub const OP_SAVE: u8 = 8;
pub const OP_TTL: u8 = 9;

/// Response status bytes (server→client)
pub const RESP_OK: u8 = 0x10;
pub const RESP_DELETED: u8 = 0x11;
pub const RESP_NOT_FOUND: u8 = 0x12;
pub const RESP_STORE_FULL: u8 = 0x13;
pub const RESP_ERROR: u8 = 0xFF;

/// Maximum keys returned by a single SCAN request.
pub const SCAN_LIMIT_CAP: usize = 1024;

/// Maximum keys accepted by a single MGET request.
pub const MGET_LIMIT_CAP: usize = 256;

/// Trait for snapshot persistence — allows the protocol layer to trigger
/// saves without depending on the concrete `SnapshotManager` type.
pub trait SnapshotSaver {
    /// Save a snapshot of the store to disk.
    fn save(&self, store: &ShardedKV) -> std::io::Result<()>;
}

/// Write a length-prefixed frame to a writer.
/// Rejects payloads larger than `MAX_FRAME_SIZE`.
pub fn write_frame<W: Write>(writer: &mut W, payload: &[u8]) -> std::io::Result<()> {
    if payload.len() > MAX_FRAME_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "oversized frame ({} bytes, max {MAX_FRAME_SIZE})",
                payload.len()
            ),
        ));
    }
    let len = payload.len() as u32;
    writer.write_all(&len.to_be_bytes())?;
    writer.write_all(payload)?;
    writer.flush()
}

/// Read a length-prefixed frame from a reader.
/// Rejects frames larger than `MAX_FRAME_SIZE`.
pub fn read_frame<R: Read>(reader: &mut R) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let frame_len = u32::from_be_bytes(len_buf) as usize;
    if frame_len > MAX_FRAME_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("oversized frame ({frame_len} bytes)"),
        ));
    }
    let mut frame = vec![0u8; frame_len];
    reader.read_exact(&mut frame)?;
    Ok(frame)
}

/// Dispatch a request frame against the store, returning the response bytes.
///
/// `snapshot` is `Some(&dyn SnapshotSaver)` if persistence is configured,
/// or `None` if SAVE should return `RESP_ERROR` (path not configured).
pub fn dispatch(frame: &[u8], store: &ShardedKV, snapshot: Option<&dyn SnapshotSaver>) -> Vec<u8> {
    if frame.is_empty() {
        return vec![RESP_ERROR];
    }

    let opcode = frame[0];
    let rest = &frame[1..];

    match opcode {
        OP_SET => {
            if rest.len() < 6 {
                return vec![RESP_ERROR];
            }
            let key_len = u16::from_be_bytes([rest[0], rest[1]]) as usize;
            let header_end = 2 + key_len + 4;
            if rest.len() < header_end {
                return vec![RESP_ERROR];
            }
            let key = &rest[2..2 + key_len];
            let val_start = 2 + key_len;
            let val_len = u32::from_be_bytes([
                rest[val_start],
                rest[val_start + 1],
                rest[val_start + 2],
                rest[val_start + 3],
            ]) as usize;
            // Checked: `dispatch` is public and can receive frames without
            // the `read_frame` size cap; on 32-bit targets a crafted
            // val_len near u32::MAX would overflow usize and mis-pass the
            // bounds check below.
            let val_end = match val_start
                .checked_add(4)
                .and_then(|e| e.checked_add(val_len))
            {
                Some(end) => end,
                None => return vec![RESP_ERROR],
            };
            if rest.len() != val_end {
                return vec![RESP_ERROR];
            }
            let key_str = match std::str::from_utf8(key) {
                Ok(s) => s.to_string(),
                Err(_) => return vec![RESP_ERROR],
            };
            // `val_end` was proven in-bounds above, so this slice is safe.
            let val = &rest[val_start + 4..val_end];
            if store.set(&key_str, val.to_vec()) {
                vec![RESP_OK]
            } else {
                vec![RESP_STORE_FULL]
            }
        }
        OP_GET => {
            if rest.len() < 2 {
                return vec![RESP_ERROR];
            }
            let key_len = u16::from_be_bytes([rest[0], rest[1]]) as usize;
            let expected_len = 2 + key_len;
            if rest.len() != expected_len {
                return vec![RESP_ERROR];
            }
            let key = &rest[2..2 + key_len];
            let key_str = match std::str::from_utf8(key) {
                Ok(s) => s.to_string(),
                Err(_) => return vec![RESP_ERROR],
            };
            match store.get(&key_str) {
                Some(val) => {
                    // Guard against oversized responses — values can enter
                    // via library API or snapshot load, not just the wire.
                    let total_size = 1usize.checked_add(4).and_then(|n| n.checked_add(val.len()));
                    match total_size {
                        Some(size) if size <= MAX_FRAME_SIZE => {
                            let mut resp = Vec::with_capacity(size);
                            resp.push(RESP_OK);
                            resp.extend_from_slice(&(val.len() as u32).to_be_bytes());
                            resp.extend_from_slice(&val);
                            resp
                        }
                        _ => vec![RESP_ERROR], // oversized response
                    }
                }
                None => vec![RESP_NOT_FOUND],
            }
        }
        OP_DEL => {
            if rest.len() < 2 {
                return vec![RESP_ERROR];
            }
            let key_len = u16::from_be_bytes([rest[0], rest[1]]) as usize;
            let expected_len = 2 + key_len;
            if rest.len() != expected_len {
                return vec![RESP_ERROR];
            }
            let key = &rest[2..2 + key_len];
            let key_str = match std::str::from_utf8(key) {
                Ok(s) => s.to_string(),
                Err(_) => return vec![RESP_ERROR],
            };
            match store.del(&key_str) {
                Some(_) => vec![RESP_DELETED],
                None => vec![RESP_NOT_FOUND],
            }
        }
        OP_PING => {
            // PING requires no payload. Reject any trailing bytes.
            if !rest.is_empty() {
                return vec![RESP_ERROR];
            }
            vec![RESP_OK]
        }
        OP_EXISTS => {
            // EXISTS: <2B key-len:BE><key>
            if rest.len() < 2 {
                return vec![RESP_ERROR];
            }
            let key_len = u16::from_be_bytes([rest[0], rest[1]]) as usize;
            let expected_len = 2 + key_len;
            if rest.len() != expected_len {
                return vec![RESP_ERROR];
            }
            let key = &rest[2..2 + key_len];
            let key_str = match std::str::from_utf8(key) {
                Ok(s) => s.to_string(),
                Err(_) => return vec![RESP_ERROR],
            };
            if store.contains(&key_str) {
                vec![RESP_OK]
            } else {
                vec![RESP_NOT_FOUND]
            }
        }
        OP_SETX => {
            // SETX: <2B key-len:BE><key><4B val-len:BE><val><8B ttl-ms:BE>
            // ttl-ms is relative milliseconds from server receipt; must be > 0.
            if rest.len() < 6 {
                return vec![RESP_ERROR];
            }
            let key_len = u16::from_be_bytes([rest[0], rest[1]]) as usize;
            let val_start = 2 + key_len;
            let header_end = val_start + 4;
            if rest.len() < header_end {
                return vec![RESP_ERROR];
            }
            let key = &rest[2..2 + key_len];
            let val_len = u32::from_be_bytes([
                rest[val_start],
                rest[val_start + 1],
                rest[val_start + 2],
                rest[val_start + 3],
            ]) as usize;
            // Checked: `dispatch` is public and can receive frames without
            // the `read_frame` size cap; on 32-bit targets a crafted
            // val_len near u32::MAX would overflow usize and mis-pass the
            // bounds check below.
            let val_end = match val_start
                .checked_add(4)
                .and_then(|e| e.checked_add(val_len))
            {
                Some(end) => end,
                None => return vec![RESP_ERROR],
            };
            let ttl_end = match val_end.checked_add(8) {
                Some(end) => end,
                None => return vec![RESP_ERROR],
            };
            if rest.len() != ttl_end {
                return vec![RESP_ERROR];
            }
            let ttl_ms = u64::from_be_bytes([
                rest[val_end],
                rest[val_end + 1],
                rest[val_end + 2],
                rest[val_end + 3],
                rest[val_end + 4],
                rest[val_end + 5],
                rest[val_end + 6],
                rest[val_end + 7],
            ]);
            if ttl_ms == 0 {
                return vec![RESP_ERROR];
            }
            let key_str = match std::str::from_utf8(key) {
                Ok(s) => s.to_string(),
                Err(_) => return vec![RESP_ERROR],
            };
            let val = &rest[val_start + 4..val_end];
            if store.set_with_ttl(&key_str, val.to_vec(), ttl_ms) {
                vec![RESP_OK]
            } else {
                vec![RESP_STORE_FULL]
            }
        }
        OP_SCAN => {
            // SCAN: <2B prefix-len:BE><prefix><2B limit:BE><2B cursor-len:BE><cursor>
            if rest.len() < 2 {
                return vec![RESP_ERROR];
            }
            let prefix_len = u16::from_be_bytes([rest[0], rest[1]]) as usize;
            let prefix_end = 2 + prefix_len;
            if rest.len() < prefix_end + 4 {
                return vec![RESP_ERROR];
            }
            let prefix = &rest[2..2 + prefix_len];
            let limit_end = prefix_end + 2;
            if rest.len() < limit_end + 2 {
                return vec![RESP_ERROR];
            }
            let limit = u16::from_be_bytes([rest[prefix_end], rest[prefix_end + 1]]) as usize;
            let cursor_len_start = limit_end;
            let cursor_len_end = cursor_len_start + 2;
            if rest.len() < cursor_len_end {
                return vec![RESP_ERROR];
            }
            let cursor_len =
                u16::from_be_bytes([rest[cursor_len_start], rest[cursor_len_start + 1]]) as usize;
            let cursor_end = cursor_len_end + cursor_len;
            if rest.len() != cursor_end {
                return vec![RESP_ERROR];
            }
            let prefix_str = if prefix.is_empty() {
                String::new()
            } else {
                match std::str::from_utf8(prefix) {
                    Ok(s) => s.to_string(),
                    Err(_) => return vec![RESP_ERROR],
                }
            };
            let cursor_str = if cursor_len == 0 {
                String::new()
            } else {
                let cursor_bytes = &rest[cursor_len_end..cursor_end];
                match std::str::from_utf8(cursor_bytes) {
                    Ok(s) => s.to_string(),
                    Err(_) => return vec![RESP_ERROR],
                }
            };
            let capped_limit = limit.min(SCAN_LIMIT_CAP);
            let (keys, more) = store.scan(&prefix_str, capped_limit, &cursor_str);

            // Pre-compute response size to check against MAX_FRAME_SIZE.
            // Response = 1 (OK) + 2 (count) + Σ(2 + key_len) + 1 (more-flag).
            // Guard against oversized keys (can enter via library API, not wire).
            let mut total_size: usize = 1 + 2 + 1;
            for key in &keys {
                if key.len() > u16::MAX as usize {
                    return vec![RESP_ERROR];
                }
                total_size += 2 + key.len();
            }
            if total_size > MAX_FRAME_SIZE {
                return vec![RESP_ERROR];
            }

            let mut resp = Vec::with_capacity(total_size);
            resp.push(RESP_OK);
            let count = keys.len() as u16;
            resp.extend_from_slice(&count.to_be_bytes());
            for key in &keys {
                let kl = key.len() as u16;
                resp.extend_from_slice(&kl.to_be_bytes());
                resp.extend_from_slice(key.as_bytes());
            }
            resp.push(if more { 0x01 } else { 0x00 });
            resp
        }
        OP_MGET => {
            // MGET: <2B count:BE> then count × (<2B key-len:BE><key>)
            if rest.len() < 2 {
                return vec![RESP_ERROR];
            }
            let count = u16::from_be_bytes([rest[0], rest[1]]) as usize;
            if count > MGET_LIMIT_CAP {
                return vec![RESP_ERROR];
            }

            // Parse all keys first.
            let mut keys: Vec<String> = Vec::with_capacity(count);
            let mut pos = 2;
            for _ in 0..count {
                if pos + 2 > rest.len() {
                    return vec![RESP_ERROR];
                }
                let key_len = u16::from_be_bytes([rest[pos], rest[pos + 1]]) as usize;
                pos += 2;
                if pos + key_len > rest.len() {
                    return vec![RESP_ERROR];
                }
                let key = &rest[pos..pos + key_len];
                pos += key_len;
                let key_str = match std::str::from_utf8(key) {
                    Ok(s) => s.to_string(),
                    Err(_) => return vec![RESP_ERROR],
                };
                keys.push(key_str);
            }
            if pos != rest.len() {
                return vec![RESP_ERROR];
            }

            let values = store.mget(&keys);

            // Build response: check size first.
            // Response = RESP_OK + per key: <1B flag><4B val-len><val>
            // Pre-compute total size to check against MAX_FRAME_SIZE.
            let mut total_size: usize = 1; // RESP_OK
            for val in &values {
                total_size += 1; // found-flag
                if let Some(v) = val {
                    total_size += 4 + v.len(); // val-len + val
                }
            }

            if total_size > MAX_FRAME_SIZE {
                return vec![RESP_ERROR];
            }

            let mut resp = Vec::with_capacity(total_size);
            resp.push(RESP_OK);
            for val in &values {
                match val {
                    Some(v) => {
                        resp.push(0x01); // found
                        resp.extend_from_slice(&(v.len() as u32).to_be_bytes());
                        resp.extend_from_slice(v);
                    }
                    None => {
                        resp.push(0x00); // not found
                    }
                }
            }
            resp
        }
        OP_SAVE => {
            // SAVE (no payload): synchronous snapshot.
            if !rest.is_empty() {
                return vec![RESP_ERROR];
            }
            match snapshot {
                Some(mgr) => match mgr.save(store) {
                    Ok(()) => vec![RESP_OK],
                    Err(e) => {
                        eprintln!("snapshot save failed: {e}");
                        vec![RESP_ERROR]
                    }
                },
                None => vec![RESP_ERROR], // path not configured
            }
        }
        OP_TTL => {
            // TTL: <2B key-len:BE><key>
            // Response: RESP_OK + <1B ttl-type> + optional <8B remaining-ms:BE>
            //   ttl-type 0x00 = permanent (no remaining-ms payload)
            //   ttl-type 0x01 = has TTL, followed by 8B remaining-ms
            // Or RESP_NOT_FOUND if key missing/expired.
            if rest.len() < 2 {
                return vec![RESP_ERROR];
            }
            let key_len = u16::from_be_bytes([rest[0], rest[1]]) as usize;
            let expected_len = 2 + key_len;
            if rest.len() != expected_len {
                return vec![RESP_ERROR];
            }
            let key = &rest[2..2 + key_len];
            let key_str = match std::str::from_utf8(key) {
                Ok(s) => s.to_string(),
                Err(_) => return vec![RESP_ERROR],
            };
            match store.ttl(&key_str) {
                Some(crate::TtlInfo::Permanent) => vec![RESP_OK, 0x00],
                Some(crate::TtlInfo::RemainingMs(ms)) => {
                    let mut resp = Vec::with_capacity(1 + 1 + 8);
                    resp.push(RESP_OK);
                    resp.push(0x01);
                    resp.extend_from_slice(&ms.to_be_bytes());
                    resp
                }
                None => vec![RESP_NOT_FOUND],
            }
        }
        _ => vec![RESP_ERROR],
    }
}
