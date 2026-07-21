//! All tests for the kvr server: protocol unit tests, TCP integration
//! tests, and UDS integration tests.

use crate::*;
use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;

// ─── Test servers ─────────────────────────────────────────────────────

pub struct TestServer {
    addr: std::net::SocketAddr,
    shutdown: Arc<ShutdownFlag>,
    handle: std::thread::JoinHandle<()>,
}

impl TestServer {
    pub fn bind(addr: &str) -> (Self, Arc<ShardedKV>) {
        let store = Arc::new(ShardedKV::new());
        let sem = Arc::new(ConnSemaphore::new(MAX_CONNECTIONS));
        let listener = std::net::TcpListener::bind(addr).expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let shutdown = ShutdownFlag::new();
        let shutdown_clone = Arc::clone(&shutdown);
        let store_for_thread = Arc::clone(&store);

        let handle = std::thread::spawn(move || {
            run_tcp_accept_loop(listener, store_for_thread, None, sem, shutdown_clone);
        });

        let server = TestServer {
            addr,
            shutdown,
            handle,
        };
        (server, store)
    }

    pub fn shutdown(self) {
        self.shutdown.signal();
        let _ = self.handle.join();
    }

    pub fn listener_addr(&self) -> std::net::SocketAddr {
        self.addr
    }
}

#[cfg(unix)]
pub struct UdsTestServer {
    socket_path: String,
    shutdown: Arc<ShutdownFlag>,
    handle: std::thread::JoinHandle<()>,
}

#[cfg(unix)]
impl UdsTestServer {
    pub fn bind(path: &str) -> (Self, Arc<ShardedKV>) {
        let store = Arc::new(ShardedKV::new());
        let sem = Arc::new(ConnSemaphore::new(MAX_CONNECTIONS));
        let listener = crate::bind_uds(path).expect("bind UDS");
        let shutdown = ShutdownFlag::new();
        let shutdown_clone = Arc::clone(&shutdown);
        let store_for_thread = Arc::clone(&store);

        let handle = std::thread::spawn(move || {
            run_uds_accept_loop(listener, store_for_thread, None, sem, shutdown_clone);
        });

        let server = UdsTestServer {
            socket_path: path.to_string(),
            shutdown,
            handle,
        };
        (server, store)
    }

    pub fn shutdown(self) {
        self.shutdown.signal();
        let _ = self.handle.join();
        let _ = std::fs::remove_file(&self.socket_path);
    }

    pub fn socket_path(&self) -> &str {
        &self.socket_path
    }
}

// ─── Frame builders ───────────────────────────────────────────────────

pub fn make_set(key: &str, val: &[u8]) -> Vec<u8> {
    assert!(key.len() <= u16::MAX as usize, "key too long");
    let mut frame = vec![OP_SET];
    let kl = key.len() as u16;
    frame.extend_from_slice(&kl.to_be_bytes());
    frame.extend_from_slice(key.as_bytes());
    let vl = val.len() as u32;
    frame.extend_from_slice(&vl.to_be_bytes());
    frame.extend_from_slice(val);
    frame
}

pub fn make_get(key: &str) -> Vec<u8> {
    assert!(key.len() <= u16::MAX as usize, "key too long");
    let mut frame = vec![OP_GET];
    let kl = key.len() as u16;
    frame.extend_from_slice(&kl.to_be_bytes());
    frame.extend_from_slice(key.as_bytes());
    frame
}

pub fn make_del(key: &str) -> Vec<u8> {
    assert!(key.len() <= u16::MAX as usize, "key too long");
    let mut frame = vec![OP_DEL];
    let kl = key.len() as u16;
    frame.extend_from_slice(&kl.to_be_bytes());
    frame.extend_from_slice(key.as_bytes());
    frame
}

pub fn make_ping() -> Vec<u8> {
    vec![OP_PING]
}

pub fn make_exists(key: &str) -> Vec<u8> {
    assert!(key.len() <= u16::MAX as usize, "key too long");
    let mut frame = vec![OP_EXISTS];
    let kl = key.len() as u16;
    frame.extend_from_slice(&kl.to_be_bytes());
    frame.extend_from_slice(key.as_bytes());
    frame
}

pub fn make_setx(key: &str, val: &[u8], ttl_ms: u64) -> Vec<u8> {
    assert!(key.len() <= u16::MAX as usize, "key too long");
    let mut frame = vec![OP_SETX];
    let kl = key.len() as u16;
    frame.extend_from_slice(&kl.to_be_bytes());
    frame.extend_from_slice(key.as_bytes());
    let vl = val.len() as u32;
    frame.extend_from_slice(&vl.to_be_bytes());
    frame.extend_from_slice(val);
    frame.extend_from_slice(&ttl_ms.to_be_bytes());
    frame
}

pub fn make_scan(prefix: &str, limit: u16, cursor: &str) -> Vec<u8> {
    assert!(prefix.len() <= u16::MAX as usize, "prefix too long");
    assert!(cursor.len() <= u16::MAX as usize, "cursor too long");
    let mut frame = vec![OP_SCAN];
    let pl = prefix.len() as u16;
    frame.extend_from_slice(&pl.to_be_bytes());
    frame.extend_from_slice(prefix.as_bytes());
    frame.extend_from_slice(&limit.to_be_bytes());
    let cl = cursor.len() as u16;
    frame.extend_from_slice(&cl.to_be_bytes());
    frame.extend_from_slice(cursor.as_bytes());
    frame
}

pub fn make_mget(keys: &[&str]) -> Vec<u8> {
    assert!(keys.len() <= u16::MAX as usize, "too many keys");
    let mut frame = vec![OP_MGET];
    let count = keys.len() as u16;
    frame.extend_from_slice(&count.to_be_bytes());
    for key in keys {
        assert!(key.len() <= u16::MAX as usize, "key too long");
        let kl = key.len() as u16;
        frame.extend_from_slice(&kl.to_be_bytes());
        frame.extend_from_slice(key.as_bytes());
    }
    frame
}

pub fn make_save() -> Vec<u8> {
    vec![OP_SAVE]
}

pub fn make_ttl(key: &str) -> Vec<u8> {
    assert!(key.len() <= u16::MAX as usize, "key too long");
    let mut frame = vec![OP_TTL];
    let kl = key.len() as u16;
    frame.extend_from_slice(&kl.to_be_bytes());
    frame.extend_from_slice(key.as_bytes());
    frame
}

pub fn wire_send_recv(stream: &mut TcpStream, frame: &[u8]) -> std::io::Result<Vec<u8>> {
    write_frame(stream, frame)?;
    read_frame(stream)
}

#[cfg(unix)]
pub fn uds_send_recv(
    stream: &mut std::os::unix::net::UnixStream,
    frame: &[u8],
) -> std::io::Result<Vec<u8>> {
    write_frame(stream, frame)?;
    read_frame(stream)
}

// ─── Protocol unit tests ──────────────────────────────────────────────

#[cfg(test)]
mod protocol_tests {
    use super::*;

    #[test]
    fn dispatch_empty_frame() {
        let store = ShardedKV::new();
        assert_eq!(dispatch(&[], &store, &None), vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_unknown_opcode() {
        let store = ShardedKV::new();
        assert_eq!(dispatch(&[0x99], &store, &None), vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_set_ok() {
        let store = ShardedKV::new();
        let frame = make_set("hello", b"world");
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_OK]);
        assert_eq!(store.get("hello"), Some(b"world".to_vec()));
    }

    #[test]
    fn dispatch_set_store_full() {
        let store = ShardedKV::with_max_entries(1);
        assert_eq!(dispatch(&make_set("a", b"1"), &store, &None), vec![RESP_OK]);
        // Store is full — new key gets STORE_FULL response.
        assert_eq!(
            dispatch(&make_set("b", b"2"), &store, &None),
            vec![RESP_STORE_FULL]
        );
        // Overwriting existing key still works.
        assert_eq!(
            dispatch(&make_set("a", b"updated"), &store, &None),
            vec![RESP_OK]
        );
    }

    #[test]
    fn dispatch_set_overwrite() {
        let store = ShardedKV::new();
        dispatch(&make_set("k", b"v1"), &store, &None);
        dispatch(&make_set("k", b"v2"), &store, &None);
        assert_eq!(store.get("k"), Some(b"v2".to_vec()));
    }

    #[test]
    fn dispatch_set_empty_value() {
        let store = ShardedKV::new();
        let frame = make_set("empty", &[]);
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_OK]);
        assert_eq!(store.get("empty"), Some(vec![]));
    }

    #[test]
    fn dispatch_set_trailing_bytes_rejected() {
        let store = ShardedKV::new();
        let mut frame = make_set("k", b"v");
        frame.push(0xDE);
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_set_short_key_len() {
        let store = ShardedKV::new();
        assert_eq!(dispatch(&[OP_SET, 0x01], &store, &None), vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_get_found() {
        let store = ShardedKV::new();
        store.set("key", b"val".to_vec());
        let resp = dispatch(&make_get("key"), &store, &None);
        assert_eq!(resp[0], RESP_OK);
        let val_len = u32::from_be_bytes([resp[1], resp[2], resp[3], resp[4]]) as usize;
        assert_eq!(val_len, 3);
        assert_eq!(&resp[5..8], b"val");
    }

    #[test]
    fn dispatch_get_not_found() {
        let store = ShardedKV::new();
        let resp = dispatch(&make_get("missing"), &store, &None);
        assert_eq!(resp, vec![RESP_NOT_FOUND]);
    }

    #[test]
    fn dispatch_get_empty_value_distinguished_from_missing() {
        let store = ShardedKV::new();
        store.set("empty", vec![]);
        let resp_found = dispatch(&make_get("empty"), &store, &None);
        let resp_missing = dispatch(&make_get("nope"), &store, &None);
        assert_eq!(resp_found[0], RESP_OK);
        assert!(resp_found.len() >= 5);
        assert_eq!(resp_missing, vec![RESP_NOT_FOUND]);
        assert_ne!(resp_found, resp_missing);
    }

    #[test]
    fn dispatch_get_large_value() {
        let store = ShardedKV::new();
        let big: Vec<u8> = (0..70_000).map(|i| (i % 256) as u8).collect();
        store.set("big", big.clone());
        let resp = dispatch(&make_get("big"), &store, &None);
        assert_eq!(resp[0], RESP_OK);
        let val_len = u32::from_be_bytes([resp[1], resp[2], resp[3], resp[4]]) as usize;
        assert_eq!(val_len, 70_000);
        assert_eq!(&resp[5..], &big[..]);
    }

    #[test]
    fn dispatch_del_found() {
        let store = ShardedKV::new();
        store.set("k", b"v".to_vec());
        assert_eq!(dispatch(&make_del("k"), &store, &None), vec![RESP_DELETED]);
        assert_eq!(store.get("k"), None);
    }

    #[test]
    fn dispatch_del_not_found() {
        let store = ShardedKV::new();
        assert_eq!(
            dispatch(&make_del("nope"), &store, &None),
            vec![RESP_NOT_FOUND]
        );
    }

    #[test]
    fn dispatch_get_invalid_utf8_key() {
        let store = ShardedKV::new();
        let mut frame = vec![OP_GET];
        frame.extend_from_slice(&[0, 3]);
        frame.extend_from_slice(&[0xFF, 0xFE, 0xFD]);
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_set_invalid_utf8_key() {
        let store = ShardedKV::new();
        let mut frame = vec![OP_SET];
        frame.extend_from_slice(&[0, 3]);
        frame.extend_from_slice(&[0xFF, 0xFE, 0xFD]);
        frame.extend_from_slice(&[0, 0, 0, 0]);
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_get_trailing_bytes_rejected() {
        let store = ShardedKV::new();
        store.set("k", b"v".to_vec());
        let mut frame = make_get("k");
        frame.push(0xDE);
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_del_trailing_bytes_rejected() {
        let store = ShardedKV::new();
        store.set("k", b"v".to_vec());
        let mut frame = make_del("k");
        frame.push(0xDE);
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_ERROR]);
        assert_eq!(store.get("k"), Some(b"v".to_vec()));
    }

    #[test]
    fn dispatch_roundtrip() {
        let store = ShardedKV::new();
        for i in 0..10 {
            let frame = make_set(&format!("k{i}"), format!("v{i}").as_bytes());
            assert_eq!(dispatch(&frame, &store, &None), vec![RESP_OK]);
        }
        for i in 0..10 {
            let resp = dispatch(&make_get(&format!("k{i}")), &store, &None);
            assert_eq!(resp[0], RESP_OK);
            let val_len = u32::from_be_bytes([resp[1], resp[2], resp[3], resp[4]]) as usize;
            assert_eq!(val_len, 2);
            assert_eq!(&resp[5..7], format!("v{i}").as_bytes());
        }
        for i in (0..10).step_by(2) {
            assert_eq!(
                dispatch(&make_del(&format!("k{i}")), &store, &None),
                vec![RESP_DELETED]
            );
        }
        for i in (0..10).step_by(2) {
            assert_eq!(
                dispatch(&make_get(&format!("k{i}")), &store, &None),
                vec![RESP_NOT_FOUND]
            );
        }
        for i in (1..10).step_by(2) {
            let resp = dispatch(&make_get(&format!("k{i}")), &store, &None);
            assert_eq!(resp[0], RESP_OK);
        }
    }

    #[test]
    fn dispatch_ping() {
        let store = ShardedKV::new();
        assert_eq!(dispatch(&[OP_PING], &store, &None), vec![RESP_OK]);
        // PING with trailing bytes should error.
        assert_eq!(dispatch(&[OP_PING, 0x01], &store, &None), vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_exists() {
        let store = ShardedKV::new();
        store.set("key", b"val".to_vec());

        // EXISTS on present key → OK
        assert_eq!(dispatch(&make_exists("key"), &store, &None), vec![RESP_OK]);
        // EXISTS on missing key → NOT_FOUND
        assert_eq!(
            dispatch(&make_exists("missing"), &store, &None),
            vec![RESP_NOT_FOUND]
        );
        // EXISTS with trailing bytes → ERROR
        let mut frame = make_exists("key");
        frame.push(0xDE);
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_exists_edge_cases() {
        let store = ShardedKV::new();
        // Empty key (key_len=0) — valid, should return NOT_FOUND.
        let frame = vec![OP_EXISTS, 0x00, 0x00];
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_NOT_FOUND]);

        // Set empty key, then EXISTS should return OK.
        store.set("", b"v".to_vec());
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_OK]);

        // Invalid UTF-8 key → ERROR
        let mut bad_frame = vec![OP_EXISTS];
        bad_frame.extend_from_slice(&[0, 3]);
        bad_frame.extend_from_slice(&[0xFF, 0xFE, 0xFD]);
        assert_eq!(dispatch(&bad_frame, &store, &None), vec![RESP_ERROR]);

        // Truncated key-len (claims 5 bytes, only 2 available) → ERROR
        assert_eq!(
            dispatch(&[OP_EXISTS, 0x00, 0x05, b'a', b'b'], &store, &None),
            vec![RESP_ERROR]
        );
    }

    // ─── SETX tests ───────────────────────────────────────────────────

    #[test]
    fn dispatch_setx_ok() {
        let store = ShardedKV::new();
        let frame = make_setx("temp", b"ephemeral", 3_600_000);
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_OK]);
        assert_eq!(store.get("temp"), Some(b"ephemeral".to_vec()));
    }

    #[test]
    fn dispatch_setx_ttl_zero_rejected() {
        let store = ShardedKV::new();
        let frame = make_setx("temp", b"v", 0);
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_ERROR]);
        // Key should not be stored.
        assert_eq!(store.get("temp"), None);
    }

    #[test]
    fn dispatch_setx_store_full() {
        let store = ShardedKV::with_max_entries(1);
        assert_eq!(
            dispatch(&make_setx("a", b"1", 3_600_000), &store, &None),
            vec![RESP_OK]
        );
        // Store is full — new key gets STORE_FULL.
        assert_eq!(
            dispatch(&make_setx("b", b"2", 3_600_000), &store, &None),
            vec![RESP_STORE_FULL]
        );
        // Overwriting existing key still works.
        assert_eq!(
            dispatch(&make_setx("a", b"updated", 3_600_000), &store, &None),
            vec![RESP_OK]
        );
    }

    #[test]
    fn dispatch_setx_trailing_bytes_rejected() {
        let store = ShardedKV::new();
        let mut frame = make_setx("k", b"v", 1000);
        frame.push(0xDE);
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_setx_truncated() {
        let store = ShardedKV::new();
        // Missing the 8-byte ttl-ms field.
        let mut frame = make_set("k", b"v");
        frame[0] = OP_SETX; // Change opcode to SETX but don't add TTL.
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_setx_invalid_utf8_key() {
        let store = ShardedKV::new();
        let mut frame = vec![OP_SETX];
        frame.extend_from_slice(&[0, 3]);
        frame.extend_from_slice(&[0xFF, 0xFE, 0xFD]);
        frame.extend_from_slice(&[0, 0, 0, 0]); // val_len=0
        frame.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0x03, 0xE8]); // ttl=1000
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_setx_empty_value() {
        let store = ShardedKV::new();
        let frame = make_setx("empty", &[], 3_600_000);
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_OK]);
        assert_eq!(store.get("empty"), Some(vec![]));
    }

    #[test]
    fn dispatch_setx_expiry_via_lazy_get() {
        let store = ShardedKV::new();
        // 50ms TTL.
        let frame = make_setx("temp", b"v", 50);
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_OK]);
        assert_eq!(store.len(), 1);

        std::thread::sleep(std::time::Duration::from_millis(80));

        // GET on expired → NOT_FOUND, entry removed, count decremented.
        let resp = dispatch(&make_get("temp"), &store, &None);
        assert_eq!(resp, vec![RESP_NOT_FOUND]);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn dispatch_set_after_setx_clears_ttl() {
        let store = ShardedKV::new();
        // SETX with short TTL.
        dispatch(&make_setx("k", b"v1", 50), &store, &None);
        // Plain SET overwrites — entry becomes permanent.
        dispatch(&make_set("k", b"v2"), &store, &None);
        assert_eq!(store.len(), 1);

        // Wait past original TTL — entry should still be present.
        std::thread::sleep(std::time::Duration::from_millis(80));
        let resp = dispatch(&make_get("k"), &store, &None);
        assert_eq!(resp[0], RESP_OK);
    }

    // ─── SCAN tests ────────────────────────────────────────────────────

    #[test]
    fn dispatch_scan_basic() {
        let store = ShardedKV::new();
        store.set("apple", b"1".to_vec());
        store.set("app2", b"2".to_vec());
        store.set("banana", b"3".to_vec());

        // Scan with prefix "app" — should return "app2" and "apple" sorted.
        let resp = dispatch(&make_scan("app", 100, ""), &store, &None);
        assert_eq!(resp[0], RESP_OK);
        let count = u16::from_be_bytes([resp[1], resp[2]]) as usize;
        assert_eq!(count, 2);
        // Keys are lexicographically sorted: "app2" < "apple".
        let mut pos = 3;
        let key1_len = u16::from_be_bytes([resp[pos], resp[pos + 1]]) as usize;
        pos += 2;
        assert_eq!(&resp[pos..pos + key1_len], b"app2");
        pos += key1_len;
        let key2_len = u16::from_be_bytes([resp[pos], resp[pos + 1]]) as usize;
        pos += 2;
        assert_eq!(&resp[pos..pos + key2_len], b"apple");
        pos += key2_len;
        // more-flag = 0 (no more pages).
        assert_eq!(resp[pos], 0x00);
    }

    #[test]
    fn dispatch_scan_empty_prefix() {
        let store = ShardedKV::new();
        store.set("a", b"1".to_vec());
        store.set("b", b"2".to_vec());
        store.set("c", b"3".to_vec());

        // Empty prefix = scan all.
        let resp = dispatch(&make_scan("", 100, ""), &store, &None);
        assert_eq!(resp[0], RESP_OK);
        let count = u16::from_be_bytes([resp[1], resp[2]]) as usize;
        assert_eq!(count, 3);
    }

    #[test]
    fn dispatch_scan_pagination() {
        let store = ShardedKV::new();
        // Insert keys k00..k09 in a non-sorted order.
        for i in 0..10 {
            store.set(&format!("k{i:02}"), b"v".to_vec());
        }

        // Page 1: limit=3, empty cursor.
        let resp = dispatch(&make_scan("", 3, ""), &store, &None);
        assert_eq!(resp[0], RESP_OK);
        let count = u16::from_be_bytes([resp[1], resp[2]]) as usize;
        assert_eq!(count, 3);
        assert_eq!(resp[resp.len() - 1], 0x01); // more = true

        // Extract last key of page 1 as cursor.
        let mut pos = 3;
        let mut last_key = String::new();
        for _ in 0..3 {
            let kl = u16::from_be_bytes([resp[pos], resp[pos + 1]]) as usize;
            pos += 2;
            last_key = String::from_utf8(resp[pos..pos + kl].to_vec()).unwrap();
            pos += kl;
        }

        // Page 2: limit=3, cursor=last key of page 1.
        let resp = dispatch(&make_scan("", 3, &last_key), &store, &None);
        assert_eq!(resp[0], RESP_OK);
        let count = u16::from_be_bytes([resp[1], resp[2]]) as usize;
        assert_eq!(count, 3);
        assert_eq!(resp[resp.len() - 1], 0x01); // more = true

        // Extract last key of page 2.
        pos = 3;
        for _ in 0..3 {
            let kl = u16::from_be_bytes([resp[pos], resp[pos + 1]]) as usize;
            pos += 2;
            last_key = String::from_utf8(resp[pos..pos + kl].to_vec()).unwrap();
            pos += kl;
        }

        // Page 3: limit=3, cursor=last key of page 2.
        let resp = dispatch(&make_scan("", 3, &last_key), &store, &None);
        assert_eq!(resp[0], RESP_OK);
        let count = u16::from_be_bytes([resp[1], resp[2]]) as usize;
        assert_eq!(count, 3);
        assert_eq!(resp[resp.len() - 1], 0x01); // more = true (10 total, 9 returned, 1 left)

        // Extract last key of page 3.
        pos = 3;
        for _ in 0..3 {
            let kl = u16::from_be_bytes([resp[pos], resp[pos + 1]]) as usize;
            pos += 2;
            last_key = String::from_utf8(resp[pos..pos + kl].to_vec()).unwrap();
            pos += kl;
        }

        // Page 4: limit=3, cursor=last key of page 3 — only 1 key left.
        let resp = dispatch(&make_scan("", 3, &last_key), &store, &None);
        assert_eq!(resp[0], RESP_OK);
        let count = u16::from_be_bytes([resp[1], resp[2]]) as usize;
        assert_eq!(count, 1);
        assert_eq!(resp[resp.len() - 1], 0x00); // more = false
    }

    #[test]
    fn dispatch_scan_excludes_expired() {
        let store = ShardedKV::new();
        store.set("perm", b"1".to_vec());
        store.set_with_ttl("temp", b"2".to_vec(), 50);

        // Wait for expiry.
        std::thread::sleep(std::time::Duration::from_millis(80));

        let resp = dispatch(&make_scan("", 100, ""), &store, &None);
        assert_eq!(resp[0], RESP_OK);
        let count = u16::from_be_bytes([resp[1], resp[2]]) as usize;
        assert_eq!(count, 1); // only "perm"

        // Scan should have purged the expired "temp" entry.
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn dispatch_scan_limit_capped() {
        let store = ShardedKV::new();
        for i in 0..10 {
            store.set(&format!("k{i:02}"), b"v".to_vec());
        }

        // Request limit=2000, should cap at 1024 and return all 10.
        let resp = dispatch(&make_scan("", 2000, ""), &store, &None);
        assert_eq!(resp[0], RESP_OK);
        let count = u16::from_be_bytes([resp[1], resp[2]]) as usize;
        assert_eq!(count, 10);
    }

    #[test]
    fn dispatch_scan_limit_capped_at_1024() {
        let store = ShardedKV::new();
        // Insert 1025 keys — more than the 1024 cap.
        for i in 0..1025 {
            store.set(&format!("k{i:04}"), b"v".to_vec());
        }

        // Request limit=2000, should cap at 1024 and return exactly 1024.
        let resp = dispatch(&make_scan("", 2000, ""), &store, &None);
        assert_eq!(resp[0], RESP_OK);
        let count = u16::from_be_bytes([resp[1], resp[2]]) as usize;
        assert_eq!(count, 1024);
        // more = true since there are more keys.
        assert_eq!(resp[resp.len() - 1], 0x01);
    }

    #[test]
    fn dispatch_scan_trailing_bytes_rejected() {
        let store = ShardedKV::new();
        let mut frame = make_scan("", 10, "");
        frame.push(0xDE);
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_scan_empty_store() {
        let store = ShardedKV::new();
        let resp = dispatch(&make_scan("", 100, ""), &store, &None);
        assert_eq!(resp[0], RESP_OK);
        let count = u16::from_be_bytes([resp[1], resp[2]]) as usize;
        assert_eq!(count, 0);
        assert_eq!(resp[3], 0x00); // more = false
    }

    // ─── MGET tests ────────────────────────────────────────────────────

    #[test]
    fn dispatch_mget_mixed_found_missing() {
        let store = ShardedKV::new();
        store.set("a", b"val_a".to_vec());
        store.set("c", b"val_c".to_vec());

        let resp = dispatch(&make_mget(&["a", "b", "c"]), &store, &None);
        assert_eq!(resp[0], RESP_OK);

        // Key "a" — found.
        assert_eq!(resp[1], 0x01);
        let len_a = u32::from_be_bytes([resp[2], resp[3], resp[4], resp[5]]) as usize;
        assert_eq!(len_a, 5);
        assert_eq!(&resp[6..6 + len_a], b"val_a");
        let mut pos = 6 + len_a;

        // Key "b" — not found.
        assert_eq!(resp[pos], 0x00);
        pos += 1;

        // Key "c" — found.
        assert_eq!(resp[pos], 0x01);
        let len_c = u32::from_be_bytes([resp[pos + 1], resp[pos + 2], resp[pos + 3], resp[pos + 4]])
            as usize;
        assert_eq!(len_c, 5);
        assert_eq!(&resp[pos + 5..pos + 5 + len_c], b"val_c");
    }

    #[test]
    fn dispatch_mget_preserves_order() {
        let store = ShardedKV::new();
        store.set("x", b"1".to_vec());
        store.set("y", b"2".to_vec());
        store.set("z", b"3".to_vec());

        // Request in non-sorted order.
        let resp = dispatch(&make_mget(&["z", "x", "y"]), &store, &None);
        assert_eq!(resp[0], RESP_OK);

        let mut pos = 1;
        // "z" first.
        assert_eq!(resp[pos], 0x01);
        let len = u32::from_be_bytes([resp[pos + 1], resp[pos + 2], resp[pos + 3], resp[pos + 4]])
            as usize;
        assert_eq!(&resp[pos + 5..pos + 5 + len], b"3");
        pos += 5 + len;

        // "x" second.
        assert_eq!(resp[pos], 0x01);
        let len = u32::from_be_bytes([resp[pos + 1], resp[pos + 2], resp[pos + 3], resp[pos + 4]])
            as usize;
        assert_eq!(&resp[pos + 5..pos + 5 + len], b"1");
        pos += 5 + len;

        // "y" third.
        assert_eq!(resp[pos], 0x01);
        let len = u32::from_be_bytes([resp[pos + 1], resp[pos + 2], resp[pos + 3], resp[pos + 4]])
            as usize;
        assert_eq!(&resp[pos + 5..pos + 5 + len], b"2");
    }

    #[test]
    fn dispatch_mget_all_missing() {
        let store = ShardedKV::new();
        let resp = dispatch(&make_mget(&["a", "b", "c"]), &store, &None);
        assert_eq!(resp[0], RESP_OK);
        assert_eq!(resp[1], 0x00); // a not found
        assert_eq!(resp[2], 0x00); // b not found
        assert_eq!(resp[3], 0x00); // c not found
    }

    #[test]
    fn dispatch_mget_empty() {
        let store = ShardedKV::new();
        let resp = dispatch(&make_mget(&[]), &store, &None);
        assert_eq!(resp[0], RESP_OK);
        assert_eq!(resp.len(), 1); // just RESP_OK, no entries
    }

    #[test]
    fn dispatch_mget_count_exceeds_cap() {
        let store = ShardedKV::new();
        // Build a request with 257 keys (cap is 256).
        let keys: Vec<&str> = (0..257).map(|_| "k").collect();
        let resp = dispatch(&make_mget(&keys), &store, &None);
        assert_eq!(resp, vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_mget_trailing_bytes_rejected() {
        let store = ShardedKV::new();
        let mut frame = make_mget(&["a"]);
        frame.push(0xDE);
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_mget_excludes_expired() {
        let store = ShardedKV::new();
        store.set("perm", b"1".to_vec());
        store.set_with_ttl("temp", b"2".to_vec(), 50);

        std::thread::sleep(std::time::Duration::from_millis(80));

        let resp = dispatch(&make_mget(&["perm", "temp"]), &store, &None);
        assert_eq!(resp[0], RESP_OK);

        // "perm" — found.
        assert_eq!(resp[1], 0x01);
        let len = u32::from_be_bytes([resp[2], resp[3], resp[4], resp[5]]) as usize;
        assert_eq!(&resp[6..6 + len], b"1");
        let pos = 6 + len;

        // "temp" — not found (expired).
        assert_eq!(resp[pos], 0x00);
    }

    // ─── TTL tests ─────────────────────────────────────────────────────

    #[test]
    fn dispatch_ttl_permanent() {
        let store = ShardedKV::new();
        store.set("k", b"v".to_vec());
        let resp = dispatch(&make_ttl("k"), &store, &None);
        assert_eq!(resp[0], RESP_OK);
        assert_eq!(resp[1], 0x00); // permanent
        assert_eq!(resp.len(), 2); // no remaining-ms payload
    }

    #[test]
    fn dispatch_ttl_with_expiry() {
        let store = ShardedKV::new();
        store.set_with_ttl("temp", b"v".to_vec(), 3_600_000);
        let resp = dispatch(&make_ttl("temp"), &store, &None);
        assert_eq!(resp[0], RESP_OK);
        assert_eq!(resp[1], 0x01); // has TTL
        assert_eq!(resp.len(), 10); // 1 + 1 + 8
        let remaining = u64::from_be_bytes([
            resp[2], resp[3], resp[4], resp[5], resp[6], resp[7], resp[8], resp[9],
        ]);
        // Should be close to 3_600_000 ms (allow small delta for execution time).
        assert!(remaining > 3_500_000 && remaining <= 3_600_000);
    }

    #[test]
    fn dispatch_ttl_not_found() {
        let store = ShardedKV::new();
        let resp = dispatch(&make_ttl("missing"), &store, &None);
        assert_eq!(resp, vec![RESP_NOT_FOUND]);
    }

    #[test]
    fn dispatch_ttl_expired_purges() {
        let store = ShardedKV::new();
        store.set_with_ttl("temp", b"v".to_vec(), 50);
        assert_eq!(store.len(), 1);

        std::thread::sleep(std::time::Duration::from_millis(80));

        // TTL on expired → NOT_FOUND, entry removed, count decremented.
        let resp = dispatch(&make_ttl("temp"), &store, &None);
        assert_eq!(resp, vec![RESP_NOT_FOUND]);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn dispatch_ttl_trailing_bytes_rejected() {
        let store = ShardedKV::new();
        store.set("k", b"v".to_vec());
        let mut frame = make_ttl("k");
        frame.push(0xDE);
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_ttl_invalid_utf8_key() {
        let store = ShardedKV::new();
        let mut frame = vec![OP_TTL];
        frame.extend_from_slice(&[0, 3]);
        frame.extend_from_slice(&[0xFF, 0xFE, 0xFD]);
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_ttl_truncated() {
        let store = ShardedKV::new();
        // Only 1 byte after opcode (need at least 2 for key-len).
        assert_eq!(dispatch(&[OP_TTL, 0x01], &store, &None), vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_ttl_empty_key_not_found() {
        let store = ShardedKV::new();
        let frame = vec![OP_TTL, 0x00, 0x00];
        assert_eq!(dispatch(&frame, &store, &None), vec![RESP_NOT_FOUND]);
    }

    // ─── Card 2 new tests ──────────────────────────────────────────────

    #[test]
    fn dispatch_get_oversized_value() {
        // A value larger than MAX_FRAME_SIZE entered via library API
        // (not the wire protocol) should return RESP_ERROR on GET, not
        // an oversized response frame.
        let store = ShardedKV::new();
        let big_val = vec![0u8; MAX_FRAME_SIZE + 1];
        store.set("big", big_val);

        let frame = make_get("big");
        let resp = dispatch(&frame, &store, &None);
        assert_eq!(resp, vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_get_normal_value_still_works() {
        // Regression guard: normal GET must still work after adding
        // the oversized response check.
        let store = ShardedKV::new();
        store.set("key", b"value".to_vec());

        let frame = make_get("key");
        let resp = dispatch(&frame, &store, &None);
        assert_eq!(resp[0], RESP_OK);
    }

    #[test]
    fn dispatch_get_boundary_max_size() {
        // Value of exactly MAX_FRAME_SIZE - 5 (1 + 4 + val = MAX_FRAME_SIZE)
        // should be the largest accepted GET response.
        let store = ShardedKV::new();
        let val = vec![0u8; MAX_FRAME_SIZE - 5];
        store.set("boundary", val);

        let frame = make_get("boundary");
        let resp = dispatch(&frame, &store, &None);
        assert_eq!(resp[0], RESP_OK);
        assert_eq!(resp.len(), MAX_FRAME_SIZE);
    }

    #[test]
    fn dispatch_get_one_byte_over_boundary() {
        // Value of MAX_FRAME_SIZE - 4 (total = MAX_FRAME_SIZE + 1) should
        // be rejected.
        let store = ShardedKV::new();
        let val = vec![0u8; MAX_FRAME_SIZE - 4];
        store.set("over", val);

        let frame = make_get("over");
        let resp = dispatch(&frame, &store, &None);
        assert_eq!(resp, vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_scan_oversized_key() {
        // A key longer than u16::MAX entered via library API should
        // return RESP_ERROR from SCAN, not a truncated response.
        let store = ShardedKV::new();
        let big_key = "x".repeat(u16::MAX as usize + 1);
        store.set(&big_key, b"v".to_vec());

        let frame = make_scan("", 100, "");
        let resp = dispatch(&frame, &store, &None);
        assert_eq!(resp, vec![RESP_ERROR]);
    }
}

// ─── Sweeper tests ───────────────────────────────────────────────────

#[cfg(test)]
mod sweeper_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn sweeper_disabled_when_interval_zero() {
        // run_sweeper with interval=0 should return immediately (disabled).
        let store = Arc::new(ShardedKV::new());
        let shutdown = ShutdownFlag::new();

        // Insert a key that would otherwise be swept.
        store.set_with_ttl("temp", b"v".to_vec(), 50);
        thread::sleep(Duration::from_millis(80));

        // Run sweeper with interval=0 — should not sweep anything.
        run_sweeper(Arc::clone(&store), Arc::clone(&shutdown), 0);

        // Entry is still there (not swept, not lazily accessed).
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn sweeper_exits_on_shutdown() {
        // Start the sweeper, signal shutdown, verify it exits promptly.
        let store = Arc::new(ShardedKV::new());
        let shutdown = ShutdownFlag::new();

        let store_clone = Arc::clone(&store);
        let shutdown_clone = Arc::clone(&shutdown);
        let handle = std::thread::spawn(move || {
            run_sweeper(store_clone, shutdown_clone, 30);
        });

        // Give it a moment to start.
        thread::sleep(Duration::from_millis(200));

        // Signal shutdown.
        shutdown.signal();

        // Should exit within a few seconds (1s poll granularity).
        // Using a channel to implement a timeout on join.
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = handle.join();
            let _ = tx.send(());
        });
        rx.recv_timeout(Duration::from_secs(5))
            .expect("sweeper did not exit within 5s");
    }

    #[test]
    fn sweeper_removes_expired_entries() {
        // Start the sweeper with a short interval, insert short-TTL keys,
        // verify they get swept without client access.
        let store = Arc::new(ShardedKV::new());
        let shutdown = ShutdownFlag::new();

        // Insert keys with 50ms TTL.
        store.set_with_ttl("a", b"1".to_vec(), 50);
        store.set_with_ttl("b", b"2".to_vec(), 50);
        store.set("perm", b"forever".to_vec());
        assert_eq!(store.len(), 3);

        let store_clone = Arc::clone(&store);
        let shutdown_clone = Arc::clone(&shutdown);
        let handle = std::thread::spawn(move || {
            // 1-second interval — will sweep after ~1s.
            run_sweeper(store_clone, shutdown_clone, 1);
        });

        // Wait long enough for the TTL to expire and the sweeper to run.
        thread::sleep(Duration::from_millis(1500));

        shutdown.signal();
        handle.join().expect("sweeper thread panicked");

        // Sweeper should have removed the expired keys, permanent stays.
        assert_eq!(store.len(), 1);
        assert_eq!(store.get("perm"), Some(b"forever".to_vec()));
        assert_eq!(store.get("a"), None);
        assert_eq!(store.get("b"), None);
    }
}

// ─── TCP integration tests ────────────────────────────────────────────

#[cfg(test)]
mod tcp_integration_tests {
    use super::*;

    #[test]
    fn wire_roundtrip() {
        let (server, _store) = TestServer::bind("127.0.0.1:0");
        let addr = server.listener_addr();

        let mut stream = TcpStream::connect(addr).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let resp = wire_send_recv(&mut stream, &make_set("hello", b"world")).expect("SET");
        assert_eq!(resp, vec![RESP_OK]);

        let resp = wire_send_recv(&mut stream, &make_get("hello")).expect("GET");
        assert_eq!(resp[0], RESP_OK);
        let val_len = u32::from_be_bytes([resp[1], resp[2], resp[3], resp[4]]) as usize;
        assert_eq!(val_len, 5);
        assert_eq!(&resp[5..10], b"world");

        let resp = wire_send_recv(&mut stream, &make_get("missing")).expect("GET missing");
        assert_eq!(resp, vec![RESP_NOT_FOUND]);

        let resp = wire_send_recv(&mut stream, &make_del("hello")).expect("DEL");
        assert_eq!(resp, vec![RESP_DELETED]);

        let resp = wire_send_recv(&mut stream, &make_del("hello")).expect("DEL again");
        assert_eq!(resp, vec![RESP_NOT_FOUND]);

        server.shutdown();
    }

    #[test]
    fn wire_large_value() {
        let (server, _store) = TestServer::bind("127.0.0.1:0");
        let addr = server.listener_addr();

        let mut stream = TcpStream::connect(addr).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let big: Vec<u8> = (0..70_000).map(|i| (i % 256) as u8).collect();
        let resp = wire_send_recv(&mut stream, &make_set("big", &big)).expect("SET big");
        assert_eq!(resp, vec![RESP_OK]);

        let resp = wire_send_recv(&mut stream, &make_get("big")).expect("GET big");
        assert_eq!(resp[0], RESP_OK);
        let val_len = u32::from_be_bytes([resp[1], resp[2], resp[3], resp[4]]) as usize;
        assert_eq!(val_len, 70_000);
        assert_eq!(&resp[5..], &big[..]);

        server.shutdown();
    }

    #[test]
    fn wire_error_responses() {
        let (server, _store) = TestServer::bind("127.0.0.1:0");
        let addr = server.listener_addr();

        let mut stream = TcpStream::connect(addr).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let resp = wire_send_recv(&mut stream, &[]).expect("empty frame");
        assert_eq!(resp, vec![RESP_ERROR]);

        let resp = wire_send_recv(&mut stream, &[0x99]).expect("bad opcode");
        assert_eq!(resp, vec![RESP_ERROR]);

        let resp = wire_send_recv(&mut stream, &[OP_SET, 0x01]).expect("truncated");
        assert_eq!(resp, vec![RESP_ERROR]);

        server.shutdown();
    }

    #[test]
    fn wire_oversized_frame() {
        let (server, _store) = TestServer::bind("127.0.0.1:0");
        let addr = server.listener_addr();

        let mut stream = TcpStream::connect(addr).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let oversized_len: u32 = (MAX_FRAME_SIZE + 1) as u32;
        stream
            .write_all(&oversized_len.to_be_bytes())
            .expect("send");

        let resp = read_frame(&mut stream);
        match resp {
            Ok(data) => assert_eq!(data, vec![RESP_ERROR]),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {}
            Err(e) => panic!("unexpected error: {e}"),
        }

        server.shutdown();
    }
}

// ─── UDS integration tests ────────────────────────────────────────────

#[cfg(all(test, unix))]
mod uds_integration_tests {
    use super::*;
    use std::os::unix::net::UnixStream;

    fn temp_socket_path() -> String {
        let pid = std::process::id();
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("/tmp/kvr_test_{pid}_{ts}.sock")
    }

    #[test]
    fn uds_roundtrip() {
        let path = temp_socket_path();
        let (server, _store) = UdsTestServer::bind(&path);

        let mut stream = UnixStream::connect(&path).expect("connect UDS");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let resp = uds_send_recv(&mut stream, &make_set("hello", b"world")).expect("SET");
        assert_eq!(resp, vec![RESP_OK]);

        let resp = uds_send_recv(&mut stream, &make_get("hello")).expect("GET");
        assert_eq!(resp[0], RESP_OK);
        let val_len = u32::from_be_bytes([resp[1], resp[2], resp[3], resp[4]]) as usize;
        assert_eq!(val_len, 5);
        assert_eq!(&resp[5..10], b"world");

        let resp = uds_send_recv(&mut stream, &make_get("missing")).expect("GET missing");
        assert_eq!(resp, vec![RESP_NOT_FOUND]);

        let resp = uds_send_recv(&mut stream, &make_del("hello")).expect("DEL");
        assert_eq!(resp, vec![RESP_DELETED]);

        server.shutdown();
    }

    #[test]
    fn uds_large_value() {
        let path = temp_socket_path();
        let (server, _store) = UdsTestServer::bind(&path);

        let mut stream = UnixStream::connect(&path).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let big: Vec<u8> = (0..70_000).map(|i| (i % 256) as u8).collect();
        let resp = uds_send_recv(&mut stream, &make_set("big", &big)).expect("SET");
        assert_eq!(resp, vec![RESP_OK]);

        let resp = uds_send_recv(&mut stream, &make_get("big")).expect("GET");
        assert_eq!(resp[0], RESP_OK);
        let val_len = u32::from_be_bytes([resp[1], resp[2], resp[3], resp[4]]) as usize;
        assert_eq!(val_len, 70_000);
        assert_eq!(&resp[5..], &big[..]);

        server.shutdown();
    }

    #[test]
    fn uds_error_responses() {
        let path = temp_socket_path();
        let (server, _store) = UdsTestServer::bind(&path);

        let mut stream = UnixStream::connect(&path).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let resp = uds_send_recv(&mut stream, &[]).expect("empty frame");
        assert_eq!(resp, vec![RESP_ERROR]);

        let resp = uds_send_recv(&mut stream, &[0x99]).expect("bad opcode");
        assert_eq!(resp, vec![RESP_ERROR]);

        server.shutdown();
    }

    #[test]
    fn uds_socket_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let path = temp_socket_path();
        let (server, _store) = UdsTestServer::bind(&path);

        let perms = std::fs::metadata(&path)
            .expect("stat socket")
            .permissions()
            .mode();
        assert_eq!(
            perms & 0o777,
            0o600,
            "socket should have 0600 permissions, got {:o}",
            perms & 0o777
        );

        server.shutdown();
    }

    #[test]
    fn uds_stale_socket_cleanup() {
        let path = temp_socket_path();
        // Create a stale socket file (bind then drop the listener without cleanup).
        {
            let _stale = std::os::unix::net::UnixListener::bind(&path).expect("bind stale");
        }
        assert!(
            std::fs::metadata(&path).is_ok(),
            "stale socket should exist"
        );

        // bind_uds should detect the stale socket and remove it.
        let (server, _store) = UdsTestServer::bind(&path);

        let mut stream = UnixStream::connect(&path).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let resp = uds_send_recv(&mut stream, &make_set("k", b"v")).expect("SET");
        assert_eq!(resp, vec![RESP_OK]);

        server.shutdown();
    }

    #[test]
    fn uds_oversized_frame() {
        let path = temp_socket_path();
        let (server, _store) = UdsTestServer::bind(&path);

        let mut stream = UnixStream::connect(&path).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let oversized_len: u32 = (MAX_FRAME_SIZE + 1) as u32;
        stream
            .write_all(&oversized_len.to_be_bytes())
            .expect("send");

        let resp = read_frame(&mut stream);
        match resp {
            Ok(data) => assert_eq!(data, vec![RESP_ERROR]),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {}
            Err(e) => panic!("unexpected error: {e}"),
        }

        server.shutdown();
    }

    #[test]
    fn uds_ping_and_exists() {
        let path = temp_socket_path();
        let (server, _store) = UdsTestServer::bind(&path);

        let mut stream = UnixStream::connect(&path).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        // PING
        let resp = uds_send_recv(&mut stream, &make_ping()).expect("PING");
        assert_eq!(resp, vec![RESP_OK]);

        // SET a key
        let resp = uds_send_recv(&mut stream, &make_set("k", b"v")).expect("SET");
        assert_eq!(resp, vec![RESP_OK]);

        // EXISTS on present key
        let resp = uds_send_recv(&mut stream, &make_exists("k")).expect("EXISTS");
        assert_eq!(resp, vec![RESP_OK]);

        // EXISTS on missing key
        let resp = uds_send_recv(&mut stream, &make_exists("missing")).expect("EXISTS missing");
        assert_eq!(resp, vec![RESP_NOT_FOUND]);

        server.shutdown();
    }

    #[test]
    fn uds_setx_roundtrip() {
        let path = temp_socket_path();
        let (server, _store) = UdsTestServer::bind(&path);

        let mut stream = UnixStream::connect(&path).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        // SETX with a long TTL.
        let resp =
            uds_send_recv(&mut stream, &make_setx("temp", b"ephemeral", 3_600_000)).expect("SETX");
        assert_eq!(resp, vec![RESP_OK]);

        // GET should return the value.
        let resp = uds_send_recv(&mut stream, &make_get("temp")).expect("GET");
        assert_eq!(resp[0], RESP_OK);
        let val_len = u32::from_be_bytes([resp[1], resp[2], resp[3], resp[4]]) as usize;
        assert_eq!(val_len, 9);
        assert_eq!(&resp[5..14], b"ephemeral");

        // EXISTS should return OK.
        let resp = uds_send_recv(&mut stream, &make_exists("temp")).expect("EXISTS");
        assert_eq!(resp, vec![RESP_OK]);

        server.shutdown();
    }

    #[test]
    fn uds_setx_ttl_zero_rejected() {
        let path = temp_socket_path();
        let (server, _store) = UdsTestServer::bind(&path);

        let mut stream = UnixStream::connect(&path).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let resp = uds_send_recv(&mut stream, &make_setx("temp", b"v", 0)).expect("SETX ttl=0");
        assert_eq!(resp, vec![RESP_ERROR]);

        // Key should not be stored.
        let resp = uds_send_recv(&mut stream, &make_get("temp")).expect("GET");
        assert_eq!(resp, vec![RESP_NOT_FOUND]);

        server.shutdown();
    }

    #[test]
    fn uds_setx_expiry() {
        let path = temp_socket_path();
        let (server, _store) = UdsTestServer::bind(&path);

        let mut stream = UnixStream::connect(&path).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        // SETX with 50ms TTL.
        let resp = uds_send_recv(&mut stream, &make_setx("temp", b"v", 50)).expect("SETX");
        assert_eq!(resp, vec![RESP_OK]);

        // GET immediately — should find the value.
        let resp = uds_send_recv(&mut stream, &make_get("temp")).expect("GET");
        assert_eq!(resp[0], RESP_OK);

        // Wait for expiry.
        std::thread::sleep(Duration::from_millis(80));

        // GET on expired → NOT_FOUND.
        let resp = uds_send_recv(&mut stream, &make_get("temp")).expect("GET expired");
        assert_eq!(resp, vec![RESP_NOT_FOUND]);

        server.shutdown();
    }

    #[test]
    fn uds_set_after_setx_clears_ttl() {
        let path = temp_socket_path();
        let (server, _store) = UdsTestServer::bind(&path);

        let mut stream = UnixStream::connect(&path).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        // SETX with short TTL.
        let resp = uds_send_recv(&mut stream, &make_setx("k", b"v1", 50)).expect("SETX");
        assert_eq!(resp, vec![RESP_OK]);

        // Plain SET overwrites — clears TTL.
        let resp = uds_send_recv(&mut stream, &make_set("k", b"v2")).expect("SET");
        assert_eq!(resp, vec![RESP_OK]);

        // Wait past original TTL.
        std::thread::sleep(Duration::from_millis(80));

        // GET should still find the value (TTL was cleared).
        let resp = uds_send_recv(&mut stream, &make_get("k")).expect("GET after TTL");
        assert_eq!(resp[0], RESP_OK);
        let val_len = u32::from_be_bytes([resp[1], resp[2], resp[3], resp[4]]) as usize;
        assert_eq!(val_len, 2);
        assert_eq!(&resp[5..7], b"v2");

        server.shutdown();
    }

    #[test]
    fn uds_scan_pagination() {
        let path = temp_socket_path();
        let (server, _store) = UdsTestServer::bind(&path);

        let mut stream = UnixStream::connect(&path).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        // Insert 5 keys.
        for i in 0..5 {
            let resp = uds_send_recv(&mut stream, &make_set(&format!("k{i}"), b"v")).expect("SET");
            assert_eq!(resp, vec![RESP_OK]);
        }

        // Page 1: limit=2.
        let resp = uds_send_recv(&mut stream, &make_scan("", 2, "")).expect("SCAN page 1");
        assert_eq!(resp[0], RESP_OK);
        let count = u16::from_be_bytes([resp[1], resp[2]]) as usize;
        assert_eq!(count, 2);
        assert_eq!(resp[resp.len() - 1], 0x01); // more

        // Get last key as cursor.
        let mut pos = 3;
        let mut last_key = String::new();
        for _ in 0..2 {
            let kl = u16::from_be_bytes([resp[pos], resp[pos + 1]]) as usize;
            pos += 2;
            last_key = String::from_utf8(resp[pos..pos + kl].to_vec()).unwrap();
            pos += kl;
        }

        // Page 2: cursor = last key.
        let resp = uds_send_recv(&mut stream, &make_scan("", 2, &last_key)).expect("SCAN page 2");
        assert_eq!(resp[0], RESP_OK);
        let count = u16::from_be_bytes([resp[1], resp[2]]) as usize;
        assert_eq!(count, 2);
        assert_eq!(resp[resp.len() - 1], 0x01); // more

        // Get last key as cursor.
        pos = 3;
        for _ in 0..2 {
            let kl = u16::from_be_bytes([resp[pos], resp[pos + 1]]) as usize;
            pos += 2;
            last_key = String::from_utf8(resp[pos..pos + kl].to_vec()).unwrap();
            pos += kl;
        }

        // Page 3: cursor = last key — 1 key left.
        let resp = uds_send_recv(&mut stream, &make_scan("", 2, &last_key)).expect("SCAN page 3");
        assert_eq!(resp[0], RESP_OK);
        let count = u16::from_be_bytes([resp[1], resp[2]]) as usize;
        assert_eq!(count, 1);
        assert_eq!(resp[resp.len() - 1], 0x00); // no more

        server.shutdown();
    }

    #[test]
    fn uds_scan_prefix() {
        let path = temp_socket_path();
        let (server, _store) = UdsTestServer::bind(&path);

        let mut stream = UnixStream::connect(&path).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        uds_send_recv(&mut stream, &make_set("apple", b"1")).expect("SET");
        uds_send_recv(&mut stream, &make_set("app2", b"2")).expect("SET");
        uds_send_recv(&mut stream, &make_set("banana", b"3")).expect("SET");

        let resp = uds_send_recv(&mut stream, &make_scan("app", 100, "")).expect("SCAN");
        assert_eq!(resp[0], RESP_OK);
        let count = u16::from_be_bytes([resp[1], resp[2]]) as usize;
        assert_eq!(count, 2); // "app2" and "apple"

        server.shutdown();
    }

    #[test]
    fn uds_mget_mixed() {
        let path = temp_socket_path();
        let (server, _store) = UdsTestServer::bind(&path);

        let mut stream = UnixStream::connect(&path).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        uds_send_recv(&mut stream, &make_set("a", b"val_a")).expect("SET");
        uds_send_recv(&mut stream, &make_set("c", b"val_c")).expect("SET");

        let resp = uds_send_recv(&mut stream, &make_mget(&["a", "b", "c"])).expect("MGET");
        assert_eq!(resp[0], RESP_OK);
        // "a" found, "b" missing, "c" found.
        assert_eq!(resp[1], 0x01); // found
        assert_eq!(resp[6 + 5], 0x00); // not found (after "val_a")
                                       // Verify "c" found after that.
        let pos = 6 + 5 + 1;
        assert_eq!(resp[pos], 0x01); // found

        server.shutdown();
    }

    #[test]
    fn uds_mget_overflow() {
        let path = temp_socket_path();
        let (server, _store) = UdsTestServer::bind(&path);

        let mut stream = UnixStream::connect(&path).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        // Insert 256 keys with large values to exceed 1 MiB response.
        let big_val = vec![0x41u8; 5000]; // 5 KB each
        for i in 0..256 {
            let resp =
                uds_send_recv(&mut stream, &make_set(&format!("k{i:03}"), &big_val)).expect("SET");
            assert_eq!(resp, vec![RESP_OK]);
        }

        // MGET all 256 keys — response would be ~256 * (1 + 4 + 5000) = ~1.28 MiB > 1 MiB.
        let keys: Vec<String> = (0..256).map(|i| format!("k{i:03}")).collect();
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let resp = uds_send_recv(&mut stream, &make_mget(&key_refs)).expect("MGET");
        assert_eq!(resp, vec![RESP_ERROR]); // overflow → ERROR

        server.shutdown();
    }

    #[test]
    fn uds_ttl_roundtrip() {
        let path = temp_socket_path();
        let (server, _store) = UdsTestServer::bind(&path);

        let mut stream = UnixStream::connect(&path).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        // SET a permanent key, then TTL → permanent.
        uds_send_recv(&mut stream, &make_set("perm", b"v")).expect("SET");
        let resp = uds_send_recv(&mut stream, &make_ttl("perm")).expect("TTL perm");
        assert_eq!(resp[0], RESP_OK);
        assert_eq!(resp[1], 0x00); // permanent
        assert_eq!(resp.len(), 2);

        // SETX with TTL, then TTL → has remaining ms.
        uds_send_recv(&mut stream, &make_setx("temp", b"v", 3_600_000)).expect("SETX");
        let resp = uds_send_recv(&mut stream, &make_ttl("temp")).expect("TTL temp");
        assert_eq!(resp[0], RESP_OK);
        assert_eq!(resp[1], 0x01); // has TTL
        assert_eq!(resp.len(), 10);
        let remaining = u64::from_be_bytes([
            resp[2], resp[3], resp[4], resp[5], resp[6], resp[7], resp[8], resp[9],
        ]);
        assert!(remaining > 3_500_000 && remaining <= 3_600_000);

        // TTL on missing key → NOT_FOUND.
        let resp = uds_send_recv(&mut stream, &make_ttl("missing")).expect("TTL missing");
        assert_eq!(resp, vec![RESP_NOT_FOUND]);

        server.shutdown();
    }
}

// ─── Snapshot tests ──────────────────────────────────────────────────

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use std::time::Duration;

    fn temp_snapshot_path() -> String {
        let pid = std::process::id();
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("/tmp/kvr_snap_{pid}_{ts}.bin")
    }

    fn path_exists(path: &str) -> bool {
        std::fs::metadata(path).is_ok()
    }

    #[test]
    fn save_and_load_roundtrip() {
        let path = temp_snapshot_path();
        let store = ShardedKV::new();
        store.set("a", b"val_a".to_vec());
        store.set("b", b"val_b".to_vec());
        store.set_with_ttl("temp", b"ephemeral".to_vec(), 3_600_000);

        let mgr = SnapshotManager::new(std::path::PathBuf::from(&path));
        mgr.save(&store).expect("save");

        // Load into a new store.
        let entries = SnapshotManager::load(std::path::Path::new(&path)).expect("load");
        let new_store = ShardedKV::new();
        let now = crate::now_ms();
        for (key, value, expires_at) in entries {
            if let Some(exp) = expires_at {
                if now >= exp {
                    continue;
                }
            }
            new_store.load_entry(key, value, expires_at);
        }

        assert_eq!(new_store.len(), 3);
        assert_eq!(new_store.get("a"), Some(b"val_a".to_vec()));
        assert_eq!(new_store.get("b"), Some(b"val_b".to_vec()));
        assert_eq!(new_store.get("temp"), Some(b"ephemeral".to_vec()));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn save_with_ttl_preserves_expiry() {
        let path = temp_snapshot_path();
        let store = ShardedKV::new();
        // Short TTL — will be expired when we save.
        store.set_with_ttl("short", b"v".to_vec(), 50);
        // Long TTL — will still be valid when we save.
        store.set_with_ttl("long", b"v".to_vec(), 3_600_000);

        // Wait for short TTL to expire before saving.
        std::thread::sleep(Duration::from_millis(80));

        let mgr = SnapshotManager::new(std::path::PathBuf::from(&path));
        mgr.save(&store).expect("save");

        // Load — expired entry was filtered at save time, so only "long" is in the file.
        let entries = SnapshotManager::load(std::path::Path::new(&path)).expect("load");
        // "short" was filtered at save time, so entries.len() == 1, not 2.
        assert_eq!(entries.len(), 1);
        let new_store = ShardedKV::new();
        let now = crate::now_ms();
        let mut loaded = 0;
        let mut skipped = 0;
        for (key, value, expires_at) in entries {
            if let Some(exp) = expires_at {
                if now >= exp {
                    skipped += 1;
                    continue;
                }
            }
            new_store.load_entry(key, value, expires_at);
            loaded += 1;
        }

        assert_eq!(skipped, 0); // no load-time skipping needed
        assert_eq!(loaded, 1); // "long" is still valid
        assert_eq!(new_store.len(), 1);
        assert_eq!(new_store.get("long"), Some(b"v".to_vec()));
        assert_eq!(new_store.get("short"), None);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn corrupted_file_refuses_load() {
        let path = temp_snapshot_path();
        let store = ShardedKV::new();
        store.set("a", b"1".to_vec());

        let mgr = SnapshotManager::new(std::path::PathBuf::from(&path));
        mgr.save(&store).expect("save");

        // Corrupt the file by flipping a byte.
        let data = std::fs::read(&path).expect("read");
        let mut corrupted = data.clone();
        // Flip a byte in the middle (not in the CRC itself).
        let flip_pos = corrupted.len() / 2;
        corrupted[flip_pos] ^= 0xFF;
        std::fs::write(&path, &corrupted).expect("write");

        // Load should fail.
        let result = SnapshotManager::load(std::path::Path::new(&path));
        assert!(result.is_err(), "corrupted file should refuse to load");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bad_magic_refuses_load() {
        let path = temp_snapshot_path();
        // Write a file with wrong magic.
        let mut bad_data = vec![b'X', b'X', b'X', b'X']; // wrong magic
        bad_data.extend_from_slice(&0u64.to_be_bytes()); // count = 0
        bad_data.extend_from_slice(&0u32.to_be_bytes()); // CRC = 0
        std::fs::write(&path, &bad_data).expect("write");

        let result = SnapshotManager::load(std::path::Path::new(&path));
        assert!(result.is_err(), "bad magic should refuse to load");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn truncated_file_refuses_load() {
        let path = temp_snapshot_path();
        // Write a truncated file (magic + partial count).
        let truncated = b"KVR1\x00\x00";
        std::fs::write(&path, truncated).expect("write");

        let result = SnapshotManager::load(std::path::Path::new(&path));
        assert!(result.is_err(), "truncated file should refuse to load");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn tmp_crash_preserves_old_file() {
        let path = temp_snapshot_path();
        let store = ShardedKV::new();
        store.set("old", b"1".to_vec());

        let mgr = SnapshotManager::new(std::path::PathBuf::from(&path));
        mgr.save(&store).expect("save first");

        // Simulate a crash mid-save: write .tmp but don't rename.
        let tmp_path = format!("{path}.tmp");
        std::fs::write(&tmp_path, b"incomplete").expect("write tmp");

        // The original file should still be intact and loadable.
        assert!(path_exists(&path));
        let entries = SnapshotManager::load(std::path::Path::new(&path)).expect("load old");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "old");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&tmp_path);
    }

    #[test]
    fn concurrent_save_serialization() {
        let path = temp_snapshot_path();
        let store = Arc::new(ShardedKV::new());
        store.set("a", b"1".to_vec());
        store.set("b", b"2".to_vec());

        let mgr = Arc::new(SnapshotManager::new(std::path::PathBuf::from(&path)));

        // Spawn two threads that both try to save.
        let mgr1 = Arc::clone(&mgr);
        let mgr2 = Arc::clone(&mgr);
        let store1 = Arc::clone(&store);
        let store2 = Arc::clone(&store);

        let h1 = std::thread::spawn(move || mgr1.save(&store1));
        let h2 = std::thread::spawn(move || mgr2.save(&store2));

        let r1 = h1.join().unwrap();
        let r2 = h2.join().unwrap();

        assert!(r1.is_ok(), "save 1 should succeed");
        assert!(r2.is_ok(), "save 2 should succeed");

        // File should be valid and loadable.
        let entries = SnapshotManager::load(std::path::Path::new(&path)).expect("load");
        assert_eq!(entries.len(), 2);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn over_capacity_load() {
        // Load more entries than max_entries — should load everything anyway.
        let path = temp_snapshot_path();
        let store = ShardedKV::new();
        for i in 0..10 {
            store.set(&format!("k{i}"), b"v".to_vec());
        }

        let mgr = SnapshotManager::new(std::path::PathBuf::from(&path));
        mgr.save(&store).expect("save");

        // Load into a store with max_entries=5 (less than 10).
        let entries = SnapshotManager::load(std::path::Path::new(&path)).expect("load");
        let bounded_store = ShardedKV::with_max_entries(5);
        let now = crate::now_ms();
        for (key, value, expires_at) in entries {
            if let Some(exp) = expires_at {
                if now >= exp {
                    continue;
                }
            }
            bounded_store.load_entry(key, value, expires_at);
        }

        // All 10 entries loaded — count reflects reality.
        assert_eq!(bounded_store.len(), 10);
        // New SET for non-existing key should return STORE_FULL (over capacity).
        assert!(!bounded_store.set("new", b"v".to_vec()));
        // After deleting entries to bring count under max, new SET should succeed.
        bounded_store.del("k0");
        bounded_store.del("k1");
        bounded_store.del("k2");
        bounded_store.del("k3");
        bounded_store.del("k4");
        bounded_store.del("k5");
        assert!(bounded_store.set("new", b"v".to_vec()));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn dispatch_save_no_path_returns_error() {
        let store = ShardedKV::new();
        store.set("a", b"1".to_vec());
        // No snapshot manager configured.
        let resp = dispatch(&make_save(), &store, &None);
        assert_eq!(resp, vec![RESP_ERROR]);
    }

    #[test]
    fn dispatch_save_success() {
        let path = temp_snapshot_path();
        let store = ShardedKV::new();
        store.set("a", b"1".to_vec());

        let mgr = Arc::new(SnapshotManager::new(std::path::PathBuf::from(&path)));
        let resp = dispatch(&make_save(), &store, &Some(Arc::clone(&mgr)));
        assert_eq!(resp, vec![RESP_OK]);
        assert!(path_exists(&path));

        // File should be loadable.
        let entries = SnapshotManager::load(std::path::Path::new(&path)).expect("load");
        assert_eq!(entries.len(), 1);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn dispatch_save_trailing_bytes_rejected() {
        let path = temp_snapshot_path();
        let store = ShardedKV::new();
        let mgr = Arc::new(SnapshotManager::new(std::path::PathBuf::from(&path)));

        let mut frame = make_save();
        frame.push(0xDE);
        let resp = dispatch(&frame, &store, &Some(mgr));
        assert_eq!(resp, vec![RESP_ERROR]);
    }

    #[test]
    fn crc32_known_value() {
        // CRC32 of "123456789" is 0xCBF43926.
        assert_eq!(crc32(b"123456789"), 0xCBF43926);
    }

    #[test]
    fn empty_store_snapshot() {
        let path = temp_snapshot_path();
        let store = ShardedKV::new();
        let mgr = SnapshotManager::new(std::path::PathBuf::from(&path));
        mgr.save(&store).expect("save");

        let entries = SnapshotManager::load(std::path::Path::new(&path)).expect("load");
        assert!(entries.is_empty());

        let _ = std::fs::remove_file(&path);
    }

    // ─── Card 2 new tests ──────────────────────────────────────────────

    #[test]
    fn snapshot_save_oversized_key() {
        // A key longer than u16::MAX (65535) entered via library API
        // should cause save() to return an error, not silently truncate.
        let path = temp_snapshot_path();
        let store = ShardedKV::new();
        let big_key = "x".repeat(u16::MAX as usize + 1);
        store.set(&big_key, b"v".to_vec());

        let mgr = SnapshotManager::new(std::path::PathBuf::from(&path));
        let result = mgr.save(&store);
        assert!(result.is_err(), "save should fail for oversized key");
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

        // No snapshot file should have been written.
        assert!(!path_exists(&path));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn snapshot_save_normal_keys_still_work() {
        // Regression guard: normal keys must still save successfully
        // after adding the oversized key guard.
        let path = temp_snapshot_path();
        let store = ShardedKV::new();
        store.set("normal_key", b"val".to_vec());

        let mgr = SnapshotManager::new(std::path::PathBuf::from(&path));
        mgr.save(&store).expect("save should succeed");

        let entries = SnapshotManager::load(std::path::Path::new(&path)).expect("load");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "normal_key");

        let _ = std::fs::remove_file(&path);
    }
}
