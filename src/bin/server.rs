use kvr::{now_ms, ShardedKV};
use std::io::{BufReader, BufWriter, Read, Write};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;

#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};

/// Maximum frame size in bytes (1 MiB). Prevents OOM from oversized frames.
const MAX_FRAME_SIZE: usize = 1 << 20;

/// Opcodes for the framed protocol (client→server requests)
const OP_SET: u8 = 0;
const OP_GET: u8 = 1;
const OP_DEL: u8 = 2;
const OP_PING: u8 = 3;
const OP_EXISTS: u8 = 4;
const OP_SETX: u8 = 5;
const OP_SCAN: u8 = 6;
const OP_MGET: u8 = 7;
const OP_SAVE: u8 = 8;
const OP_TTL: u8 = 9;

/// Response status bytes (server→client)
const RESP_OK: u8 = 0x10;
const RESP_DELETED: u8 = 0x11;
const RESP_NOT_FOUND: u8 = 0x12;
const RESP_STORE_FULL: u8 = 0x13;
const RESP_ERROR: u8 = 0xFF;

/// Maximum concurrent client connections (connection cap / DoS protection).
const MAX_CONNECTIONS: usize = 256;

/// Maximum keys returned by a single SCAN request.
const SCAN_LIMIT_CAP: usize = 1024;

/// Maximum keys accepted by a single MGET request.
const MGET_LIMIT_CAP: usize = 256;

/// Read timeout for individual client connections (slow-loris protection).
const READ_TIMEOUT_SECS: u64 = 30;

/// Default UDS socket path.
const DEFAULT_SOCKET_PATH: &str = "/tmp/kvr.sock";

/// Poll interval for the accept loop (used for shutdown checks).
const ACCEPT_POLL_TIMEOUT: Duration = Duration::from_millis(100);

// ─── Shutdown flag ────────────────────────────────────────────────────

struct ShutdownFlag(AtomicBool);

impl ShutdownFlag {
    fn new() -> Arc<Self> {
        Arc::new(Self(AtomicBool::new(false)))
    }

    fn is_set(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    fn signal(&self) {
        self.0.store(true, Ordering::Release);
    }
}

// ─── Connection semaphore ─────────────────────────────────────────────

struct ConnGuard {
    sem: Arc<ConnSemaphore>,
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.sem.release();
    }
}

struct ConnSemaphore {
    permits: Mutex<usize>,
    cvar: Condvar,
}

impl ConnSemaphore {
    fn new(max: usize) -> Self {
        Self {
            permits: Mutex::new(max),
            cvar: Condvar::new(),
        }
    }

    fn acquire_or_shutdown(sem: &Arc<ConnSemaphore>, shutdown: &ShutdownFlag) -> Option<ConnGuard> {
        let sem_clone = Arc::clone(sem);
        let mut p = sem.permits.lock().unwrap();
        loop {
            if shutdown.is_set() {
                return None;
            }
            if *p > 0 {
                *p -= 1;
                return Some(ConnGuard { sem: sem_clone });
            }
            let (lock, _) = sem.cvar.wait_timeout(p, ACCEPT_POLL_TIMEOUT).unwrap();
            p = lock;
        }
    }

    fn release(&self) {
        let mut p = self.permits.lock().unwrap();
        *p += 1;
        self.cvar.notify_one();
    }
}

// ─── Frame I/O ────────────────────────────────────────────────────────

fn write_frame<W: Write>(writer: &mut W, payload: &[u8]) -> std::io::Result<()> {
    let len = payload.len() as u32;
    writer.write_all(&len.to_be_bytes())?;
    writer.write_all(payload)?;
    writer.flush()
}

fn read_frame<R: Read>(reader: &mut R) -> std::io::Result<Vec<u8>> {
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

// ─── Client handler ───────────────────────────────────────────────────

fn handle_client<R: Read, W: Write>(
    reader: R,
    writer: W,
    store: Arc<ShardedKV>,
    snapshot: Option<Arc<SnapshotManager>>,
) {
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    loop {
        match read_frame(&mut reader) {
            Ok(frame) => {
                let resp = dispatch(&frame, &store, &snapshot);
                if let Err(e) = write_frame(&mut writer, &resp) {
                    eprintln!("client write error: {e}");
                    break;
                }
            }
            Err(e) => {
                let kind = e.kind();
                if kind == std::io::ErrorKind::UnexpectedEof
                    || kind == std::io::ErrorKind::ConnectionReset
                    || kind == std::io::ErrorKind::TimedOut
                    || kind == std::io::ErrorKind::WouldBlock
                {
                    break;
                }
                eprintln!("client read error: {e}");
                let _ = write_frame(&mut writer, &[RESP_ERROR]);
                break;
            }
        }
    }
}

// ─── Dispatch ─────────────────────────────────────────────────────────

fn dispatch(frame: &[u8], store: &ShardedKV, snapshot: &Option<Arc<SnapshotManager>>) -> Vec<u8> {
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
            let val_end = val_start + 4 + val_len;
            if rest.len() != val_end {
                return vec![RESP_ERROR];
            }
            let key_str = match std::str::from_utf8(key) {
                Ok(s) => s.to_string(),
                Err(_) => return vec![RESP_ERROR],
            };
            let val = &rest[val_start + 4..val_start + 4 + val_len];
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
            let val_end = val_start + 4 + val_len;
            let ttl_end = val_end + 8;
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
            let val = &rest[val_start + 4..val_start + 4 + val_len];
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
                Some(kvr::TtlInfo::Permanent) => vec![RESP_OK, 0x00],
                Some(kvr::TtlInfo::RemainingMs(ms)) => {
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

// ─── TCP accept loop ──────────────────────────────────────────────────

fn run_tcp_accept_loop(
    listener: std::net::TcpListener,
    store: Arc<ShardedKV>,
    snapshot: Option<Arc<SnapshotManager>>,
    sem: Arc<ConnSemaphore>,
    shutdown: Arc<ShutdownFlag>,
) {
    listener.set_nonblocking(true).expect("set non-blocking");

    while !shutdown.is_set() {
        match listener.accept() {
            Ok((s, _)) => {
                let _ = s.set_nonblocking(false);
                let _ = s.set_read_timeout(Some(Duration::from_secs(READ_TIMEOUT_SECS)));
                let guard = match ConnSemaphore::acquire_or_shutdown(&sem, &shutdown) {
                    Some(g) => g,
                    None => break,
                };
                let store = Arc::clone(&store);
                let snapshot = snapshot.clone();
                match thread::Builder::new().spawn(move || {
                    let _guard = guard;
                    let reader = match s.try_clone() {
                        Ok(r) => r,
                        Err(e) => {
                            eprintln!("failed to clone stream: {e}");
                            return;
                        }
                    };
                    let _ = catch_unwind(AssertUnwindSafe(|| {
                        handle_client(reader, s, store, snapshot)
                    }));
                }) {
                    Ok(_) => {}
                    Err(e) => eprintln!("failed to spawn client thread: {e}"),
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_POLL_TIMEOUT);
            }
            Err(e) => {
                eprintln!("accept error: {e}");
                thread::sleep(ACCEPT_POLL_TIMEOUT);
            }
        }
    }
}

// ─── UDS accept loop ──────────────────────────────────────────────────

#[cfg(unix)]
fn run_uds_accept_loop(
    listener: UnixListener,
    store: Arc<ShardedKV>,
    snapshot: Option<Arc<SnapshotManager>>,
    sem: Arc<ConnSemaphore>,
    shutdown: Arc<ShutdownFlag>,
) {
    listener.set_nonblocking(true).expect("set non-blocking");

    while !shutdown.is_set() {
        match listener.accept() {
            Ok((s, _)) => {
                let _ = s.set_nonblocking(false);
                let _ = s.set_read_timeout(Some(Duration::from_secs(READ_TIMEOUT_SECS)));
                let guard = match ConnSemaphore::acquire_or_shutdown(&sem, &shutdown) {
                    Some(g) => g,
                    None => break,
                };
                let store = Arc::clone(&store);
                let snapshot = snapshot.clone();
                match thread::Builder::new().spawn(move || {
                    let _guard = guard;
                    let reader = match s.try_clone() {
                        Ok(r) => r,
                        Err(e) => {
                            eprintln!("failed to clone stream: {e}");
                            return;
                        }
                    };
                    let _ = catch_unwind(AssertUnwindSafe(|| {
                        handle_client(reader, s, store, snapshot)
                    }));
                }) {
                    Ok(_) => {}
                    Err(e) => eprintln!("failed to spawn client thread: {e}"),
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_POLL_TIMEOUT);
            }
            Err(e) => {
                eprintln!("accept error: {e}");
                thread::sleep(ACCEPT_POLL_TIMEOUT);
            }
        }
    }
}

/// Bind a UDS listener with stale-socket detection and permission hardening.
/// - If the path doesn't exist: bind directly.
/// - If the path exists but is a stale socket (connect fails): remove and bind.
/// - If the path exists and is a live socket: fail (don't steal it).
/// - If the path exists but is not a socket: fail (don't remove arbitrary files).
///
/// After binding, sets permissions to 0o600 (owner read/write only).
#[cfg(unix)]
fn bind_uds(socket_path: &str) -> std::io::Result<UnixListener> {
    use std::os::unix::fs::PermissionsExt;

    // Check if path exists and handle stale sockets.
    match std::fs::metadata(socket_path) {
        Ok(metadata) => {
            if metadata.file_type().is_socket() {
                // Try to connect — if it fails, the socket is stale.
                match UnixStream::connect(socket_path) {
                    Ok(_) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::AddrInUse,
                            format!("socket path {socket_path} is in use by a live server"),
                        ));
                    }
                    Err(_) => {
                        // Stale socket — safe to remove.
                        std::fs::remove_file(socket_path)?;
                    }
                }
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AddrInUse,
                    format!("socket path {socket_path} exists but is not a socket"),
                ));
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Path doesn't exist — good, proceed to bind.
        }
        Err(e) => return Err(e),
    }

    let listener = UnixListener::bind(socket_path)?;

    // Set permissions to owner-only. This is a security control — fail hard
    // if it doesn't work rather than running with default (umask) permissions.
    std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600)).map_err(|e| {
        let _ = std::fs::remove_file(socket_path);
        std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("failed to set socket permissions to 0600: {e}"),
        )
    })?;

    Ok(listener)
}

/// RAII guard that removes the socket file on drop.
/// Only removes if the path still exists and is a socket (best-effort).
#[cfg(unix)]
struct SocketCleanup {
    path: String,
}

#[cfg(unix)]
impl Drop for SocketCleanup {
    fn drop(&mut self) {
        if let Ok(meta) = std::fs::metadata(&self.path) {
            if meta.file_type().is_socket() {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }
}

// ─── Signal handling ──────────────────────────────────────────────────

/// Install SIGTERM and SIGINT handlers that signal the shutdown flag.
/// Panics if registration fails — without signal handlers, the server
/// cannot be gracefully stopped.
fn install_signal_handlers(shutdown: Arc<ShutdownFlag>) {
    let shutdown_clone = Arc::clone(&shutdown);
    // SAFETY: The closure only performs an atomic store, which is
    // async-signal-safe. signal_hook::low_level::register installs a
    // signal handler that calls this closure on signal delivery.
    unsafe {
        signal_hook::low_level::register(signal_hook::consts::SIGTERM, move || {
            shutdown_clone.signal();
        })
        .expect("failed to register SIGTERM handler");
    }
    let shutdown_clone = Arc::clone(&shutdown);
    unsafe {
        signal_hook::low_level::register(signal_hook::consts::SIGINT, move || {
            shutdown_clone.signal();
        })
        .expect("failed to register SIGINT handler");
    }
}

// ─── CRC32 (bitwise, hand-rolled) ─────────────────────────────────────

/// Compute CRC32 (IEEE 802.3 polynomial 0xEDB88320) over the given bytes.
/// Hand-rolled (bitwise) to avoid adding a dependency.
fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for &byte in data {
        let mut c = crc ^ byte as u32;
        for _ in 0..8 {
            if c & 1 != 0 {
                c = (c >> 1) ^ 0xEDB88320;
            } else {
                c >>= 1;
            }
        }
        crc = c;
    }
    !crc
}

// ─── Snapshot persistence ────────────────────────────────────────────

/// A snapshot entry: (key, value, optional expiry timestamp).
type SnapshotEntry = (String, Vec<u8>, Option<u64>);

/// Manages snapshot persistence: saving the store to a file and loading on startup.
///
/// All snapshot writes serialize through a single mutex — concurrent SAVEs
/// must not interleave.
pub struct SnapshotManager {
    path: std::path::PathBuf,
    save_lock: parking_lot::Mutex<()>,
}

/// Snapshot file format:
/// - Magic: b"KVR1" (4 bytes)
/// - Entry count: <8B count:BE>
/// - Entries: count × (<2B key-len:BE><key><4B val-len:BE><val><8B expires-at:BE>)
///   expires-at is 0 for no TTL (None), otherwise UNIX epoch milliseconds.
/// - CRC32: <4B CRC32:BE> of everything after the magic.
const SNAPSHOT_MAGIC: &[u8] = b"KVR1";

impl SnapshotManager {
    pub fn new(path: std::path::PathBuf) -> Self {
        SnapshotManager {
            path,
            save_lock: parking_lot::Mutex::new(()),
        }
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Save a snapshot of the store to disk.
    ///
    /// Acquires read locks on ALL shards simultaneously for a point-in-time view,
    /// serializes, releases locks, then writes to `<path>.tmp`, fsyncs, atomically
    /// renames to `<path>`, and fsyncs the parent directory. A crash mid-save
    /// never corrupts or removes an existing valid snapshot.
    pub fn save(&self, store: &ShardedKV) -> std::io::Result<()> {
        let _lock = self.save_lock.lock();

        // Collect entries (point-in-time snapshot under all shard read locks).
        let entries = store.collect_for_snapshot();

        // Serialize.
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(SNAPSHOT_MAGIC);
        buf.extend_from_slice(&(entries.len() as u64).to_be_bytes());
        for (key, value, expires_at) in &entries {
            // Guard against silent truncation — keys/values larger than the
            // format limits must not be silently cast down.
            if key.len() > u16::MAX as usize {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("snapshot key too long ({} bytes, max {})", key.len(), u16::MAX),
                ));
            }
            if value.len() > u32::MAX as usize {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("snapshot value too long ({} bytes, max {})", value.len(), u32::MAX),
                ));
            }
            let key_len = key.len() as u16;
            buf.extend_from_slice(&key_len.to_be_bytes());
            buf.extend_from_slice(key.as_bytes());
            let val_len = value.len() as u32;
            buf.extend_from_slice(&val_len.to_be_bytes());
            buf.extend_from_slice(value);
            let exp = expires_at.unwrap_or(0);
            buf.extend_from_slice(&exp.to_be_bytes());
        }

        // Compute CRC32 over everything after magic.
        let crc = crc32(&buf[SNAPSHOT_MAGIC.len()..]);
        buf.extend_from_slice(&crc.to_be_bytes());

        // Write to .tmp file.
        let tmp_path = {
            let mut tmp = self.path.as_os_str().to_owned();
            tmp.push(".tmp");
            std::path::PathBuf::from(tmp)
        };
        {
            let mut file = std::fs::File::create(&tmp_path)?;
            file.write_all(&buf)?;
            file.sync_all()?;
        }

        // Atomic rename to final path.
        std::fs::rename(&tmp_path, &self.path)?;

        // fsync parent directory to make rename durable.
        if let Some(parent) = self.path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }

        Ok(())
    }

    /// Load a snapshot from disk.
    ///
    /// Returns the entries (key, value, expires_at) for the caller to insert.
    /// Does NOT filter expired entries — the caller must do that.
    ///
    /// Rejects on bad magic, truncation, or CRC mismatch. Never half-loads.
    pub fn load(path: &std::path::Path) -> std::io::Result<Vec<SnapshotEntry>> {
        let data = std::fs::read(path)?;

        // Check magic.
        if data.len() < SNAPSHOT_MAGIC.len() + 8 + 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "snapshot file too short",
            ));
        }
        if &data[..SNAPSHOT_MAGIC.len()] != SNAPSHOT_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bad snapshot magic",
            ));
        }

        let after_magic = &data[SNAPSHOT_MAGIC.len()..];

        // Verify CRC.
        let data_len = after_magic.len();
        if data_len < 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "snapshot file truncated (no CRC)",
            ));
        }
        let stored_crc = u32::from_be_bytes([
            after_magic[data_len - 4],
            after_magic[data_len - 3],
            after_magic[data_len - 2],
            after_magic[data_len - 1],
        ]);
        let body = &after_magic[..data_len - 4];
        let computed_crc = crc32(body);
        if stored_crc != computed_crc {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "snapshot CRC mismatch",
            ));
        }

        // Parse entries.
        let mut pos = 0;
        let count = u64::from_be_bytes([
            body[pos],
            body[pos + 1],
            body[pos + 2],
            body[pos + 3],
            body[pos + 4],
            body[pos + 5],
            body[pos + 6],
            body[pos + 7],
        ]);
        pos += 8;

        // Cap pre-allocation to avoid OOM from corrupted count.
        let cap = (count as usize).min(body.len() / 14); // min entry size ≈ 14 bytes
        let mut entries = Vec::with_capacity(cap);
        for _ in 0..count {
            if pos + 2 > body.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "snapshot truncated at key length",
                ));
            }
            let key_len = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
            pos += 2;
            if pos + key_len > body.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "snapshot truncated at key",
                ));
            }
            let key = String::from_utf8(body[pos..pos + key_len].to_vec()).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "snapshot contains invalid UTF-8 key",
                )
            })?;
            pos += key_len;

            if pos + 4 > body.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "snapshot truncated at value length",
                ));
            }
            let val_len =
                u32::from_be_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]])
                    as usize;
            pos += 4;
            if pos + val_len > body.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "snapshot truncated at value",
                ));
            }
            let value = body[pos..pos + val_len].to_vec();
            pos += val_len;

            if pos + 8 > body.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "snapshot truncated at expires-at",
                ));
            }
            let expires_raw = u64::from_be_bytes([
                body[pos],
                body[pos + 1],
                body[pos + 2],
                body[pos + 3],
                body[pos + 4],
                body[pos + 5],
                body[pos + 6],
                body[pos + 7],
            ]);
            pos += 8;
            let expires_at = if expires_raw == 0 {
                None
            } else {
                Some(expires_raw)
            };

            entries.push((key, value, expires_at));
        }

        if pos != body.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "snapshot has trailing bytes after entries",
            ));
        }

        Ok(entries)
    }
}

// ─── Sweeper thread ───────────────────────────────────────────────────

/// Run a background sweeper that periodically removes expired entries.
///
/// Sweeps every `interval_secs` seconds. Checks `shutdown.is_set()` every
/// second for responsive shutdown. Exits cleanly when shutdown is signaled.
/// If `interval_secs` is 0, the sweeper is disabled (returns immediately).
fn run_sweeper(store: Arc<ShardedKV>, shutdown: Arc<ShutdownFlag>, interval_secs: u64) {
    if interval_secs == 0 {
        return; // disabled
    }
    let interval = Duration::from_secs(interval_secs);
    let poll = Duration::from_secs(1);

    eprintln!("sweeper thread started (interval: {interval_secs}s)");

    while !shutdown.is_set() {
        // Sleep in 1-second increments to check shutdown promptly.
        let mut remaining = interval;
        while remaining > Duration::ZERO && !shutdown.is_set() {
            let step = remaining.min(poll);
            thread::sleep(step);
            remaining = remaining.saturating_sub(step);
        }
        if shutdown.is_set() {
            break;
        }
        let removed = store.sweep_expired();
        if removed > 0 {
            eprintln!("sweeper: removed {removed} expired entries");
        }
    }

    eprintln!("sweeper thread exiting");
}

/// Run a background thread that periodically saves snapshots.
///
/// Saves every `interval_secs` seconds. Checks `shutdown.is_set()` every
/// second for responsive shutdown. Exits cleanly when shutdown is signaled.
fn run_periodic_snapshot(
    store: Arc<ShardedKV>,
    mgr: Arc<SnapshotManager>,
    shutdown: Arc<ShutdownFlag>,
    interval_secs: u64,
) {
    let interval = Duration::from_secs(interval_secs);
    let poll = Duration::from_secs(1);

    eprintln!("snapshot thread started (interval: {interval_secs}s)");

    while !shutdown.is_set() {
        let mut remaining = interval;
        while remaining > Duration::ZERO && !shutdown.is_set() {
            let step = remaining.min(poll);
            thread::sleep(step);
            remaining = remaining.saturating_sub(step);
        }
        if shutdown.is_set() {
            break;
        }
        if let Err(e) = mgr.save(&store) {
            eprintln!("periodic snapshot failed: {e}");
        }
    }

    eprintln!("snapshot thread exiting");
}

// ─── Production entry point ───────────────────────────────────────────

#[cfg(unix)]
fn main() {
    let socket_path =
        std::env::var("KVR_SOCKET_PATH").unwrap_or_else(|_| DEFAULT_SOCKET_PATH.to_string());
    let tcp_addr = std::env::var("KVR_TCP_ADDR"); // optional secondary TCP
    let max_entries: usize = std::env::var("KVR_MAX_ENTRIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);
    let max_connections: usize = std::env::var("KVR_MAX_CONNECTIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(MAX_CONNECTIONS);
    let sweep_interval_secs: u64 = std::env::var("KVR_SWEEP_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);

    // Snapshot configuration.
    let snapshot_path = std::env::var("KVR_SNAPSHOT_PATH")
        .ok()
        .filter(|s| !s.is_empty());
    let snapshot_on_shutdown: bool = std::env::var("KVR_SNAPSHOT_ON_SHUTDOWN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(true); // default true when path set
    let snapshot_interval_secs: u64 = std::env::var("KVR_SNAPSHOT_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0); // 0 = disabled

    let store = Arc::new(if max_entries > 0 {
        ShardedKV::with_max_entries(max_entries)
    } else {
        ShardedKV::new()
    });

    // Load snapshot on startup if path is configured and file exists.
    if let Some(ref path_str) = snapshot_path {
        let path = std::path::PathBuf::from(path_str);
        if path.exists() {
            match SnapshotManager::load(&path) {
                Ok(entries) => {
                    let now = now_ms();
                    let mut loaded = 0;
                    let mut skipped = 0;
                    for (key, value, expires_at) in entries {
                        // Skip entries that are already expired.
                        if let Some(exp) = expires_at {
                            if now >= exp {
                                skipped += 1;
                                continue;
                            }
                        }
                        store.load_entry(key, value, expires_at);
                        loaded += 1;
                    }
                    eprintln!("snapshot: loaded {loaded} entries from {path_str} (skipped {skipped} expired)");
                }
                Err(e) => {
                    eprintln!("snapshot: failed to load from {path_str}: {e} — starting empty");
                }
            }
        }
    }

    // Create SnapshotManager if path is configured.
    let snapshot: Option<Arc<SnapshotManager>> = snapshot_path
        .as_ref()
        .map(|p| Arc::new(SnapshotManager::new(std::path::PathBuf::from(p))));

    let sem = Arc::new(ConnSemaphore::new(max_connections));
    let shutdown = ShutdownFlag::new();

    install_signal_handlers(Arc::clone(&shutdown));

    // Start the background sweeper for TTL expiry.
    {
        let store_sweeper = Arc::clone(&store);
        let shutdown_sweeper = Arc::clone(&shutdown);
        let _ = thread::Builder::new()
            .name("sweeper".to_string())
            .spawn(move || run_sweeper(store_sweeper, shutdown_sweeper, sweep_interval_secs));
    }

    // Start periodic snapshot thread if configured.
    if let Some(ref mgr) = snapshot {
        if snapshot_interval_secs > 0 {
            let store_snap = Arc::clone(&store);
            let shutdown_snap = Arc::clone(&shutdown);
            let mgr_snap = Arc::clone(mgr);
            let interval = snapshot_interval_secs;
            let _ = thread::Builder::new()
                .name("snapshot".to_string())
                .spawn(move || {
                    run_periodic_snapshot(store_snap, mgr_snap, shutdown_snap, interval)
                });
        }
    }

    // Bind UDS with stale-socket detection and permission hardening.
    let listener = match bind_uds(&socket_path) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("failed to bind UDS socket {socket_path}: {e}");
            std::process::exit(1);
        }
    };

    // RAII cleanup — removes socket file on drop (including panic unwind).
    let _socket_cleanup = SocketCleanup {
        path: socket_path.clone(),
    };

    eprintln!(
        "kvr server listening on {socket_path} (UDS, max {max_connections} connections, {READ_TIMEOUT_SECS}s read timeout, max entries: {max_entries})"
    );

    // Optionally start a secondary TCP listener (debug only — unauthenticated).
    if let Ok(addr) = tcp_addr {
        eprintln!("WARNING: TCP listener enabled on {addr} — unauthenticated, use for debug only");
        let shutdown_tcp = Arc::clone(&shutdown);
        let store_tcp = Arc::clone(&store);
        let sem_tcp = Arc::clone(&sem);
        let snapshot_tcp = snapshot.clone();
        let _ = thread::Builder::new()
            .name("tcp-listener".to_string())
            .spawn(move || match std::net::TcpListener::bind(&addr) {
                Ok(listener) => {
                    eprintln!("kvr TCP listener on {addr}");
                    run_tcp_accept_loop(listener, store_tcp, snapshot_tcp, sem_tcp, shutdown_tcp);
                }
                Err(e) => eprintln!("failed to bind TCP {addr}: {e}"),
            });
    }

    let shutdown_for_uds = Arc::clone(&shutdown);
    let store_for_uds = Arc::clone(&store);
    let sem_for_uds = Arc::clone(&sem);
    let snapshot_for_uds = snapshot.clone();

    run_uds_accept_loop(
        listener,
        store_for_uds,
        snapshot_for_uds,
        sem_for_uds,
        shutdown_for_uds,
    );

    eprintln!("kvr server shut down");

    // Save snapshot on graceful shutdown if configured.
    if snapshot_on_shutdown {
        if let Some(ref mgr) = snapshot {
            match mgr.save(&store) {
                Ok(()) => eprintln!("snapshot: saved on shutdown"),
                Err(e) => eprintln!("snapshot: failed to save on shutdown: {e}"),
            }
        }
    }
    // _socket_cleanup drops here, removing the socket file.
}

#[cfg(not(unix))]
fn main() {
    eprintln!("kvr server requires Unix (UDS).");
    std::process::exit(1);
}

// ─── Tests ────────────────────────────────────────────────────────────
// All test code is gated behind #[cfg(test)] and lives at the bottom of this
// file. Test helpers (TestServer, UdsTestServer, frame builders) are also
// cfg(test) to avoid dead-code warnings in production builds.

#[cfg(test)]
#[path = "server/tests.rs"]
mod tests;
