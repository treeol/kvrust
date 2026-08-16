use kvr::protocol::{dispatch, read_frame, write_frame, OP_PING, RESP_ERROR, RESP_OK};
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

/// Maximum concurrent client connections (connection cap / DoS protection).
const MAX_CONNECTIONS: usize = 256;

/// Read timeout for individual client connections (slow-loris protection).
const READ_TIMEOUT_SECS: u64 = 30;

/// Default UDS socket path.
const DEFAULT_SOCKET_PATH: &str = "/tmp/kvr.sock";

/// Poll interval for the accept loop (used for shutdown checks).
const ACCEPT_POLL_TIMEOUT: Duration = Duration::from_millis(100);

// ─── Shutdown flag ────────────────────────────────────────────────────

/// Shared shutdown flag. Wraps an `Arc<AtomicBool>` so the same atomic can be
/// handed to the safe `signal_hook::flag::register` handlers.
struct ShutdownFlag(Arc<AtomicBool>);

impl ShutdownFlag {
    fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(AtomicBool::new(false))))
    }

    fn is_set(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn signal(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// The underlying atomic, for sharing with signal handlers.
    fn raw(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.0)
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
                let snap_ref: Option<&dyn kvr::protocol::SnapshotSaver> = snapshot
                    .as_ref()
                    .map(|m| m.as_ref() as &dyn kvr::protocol::SnapshotSaver);
                let resp = dispatch(&frame, &store, snap_ref);
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

/// Install SIGTERM and SIGINT handlers that set the shutdown flag.
/// Panics if registration fails — without signal handlers, the server
/// cannot be gracefully stopped.
///
/// Uses the safe `signal_hook::flag::register`, which installs the
/// handler and stores `true` (SeqCst) into the shared atomic on signal
/// delivery — no `unsafe` needed in this crate.
fn install_signal_handlers(shutdown: Arc<ShutdownFlag>) {
    let flag = shutdown.raw();
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&flag))
        .expect("failed to register SIGTERM handler");
    signal_hook::flag::register(signal_hook::consts::SIGINT, flag)
        .expect("failed to register SIGINT handler");
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
    pub fn save_snapshot(&self, store: &ShardedKV) -> std::io::Result<()> {
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
                    format!(
                        "snapshot key too long ({} bytes, max {})",
                        key.len(),
                        u16::MAX
                    ),
                ));
            }
            if value.len() > u32::MAX as usize {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "snapshot value too long ({} bytes, max {})",
                        value.len(),
                        u32::MAX
                    ),
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
            // The snapshot holds all persisted data — restrict it to the
            // owner, matching the 0600 socket. On Unix the file is created
            // with mode 0600 up front (no permissive window); the
            // set_permissions call also re-asserts 0600 if a stale .tmp from
            // an earlier crash already existed (mode() only applies at
            // creation).
            let mut file: std::fs::File;
            #[cfg(unix)]
            {
                use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
                let mut opts = std::fs::OpenOptions::new();
                opts.write(true).create(true).truncate(true).mode(0o600);
                file = opts.open(&tmp_path)?;
                file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            }
            #[cfg(not(unix))]
            {
                file = std::fs::File::create(&tmp_path)?;
            }
            file.write_all(&buf)?;
            file.sync_all()?;
        }

        // Atomic rename to final path.
        std::fs::rename(&tmp_path, &self.path)?;

        // fsync parent directory to make rename durable. Propagate
        // failures — the crash-safety guarantee requires the rename to be
        // durable, not merely visible. An empty parent (bare filename) is
        // resolved against the current directory.
        if let Some(parent) = self.path.parent() {
            let parent_dir = if parent.as_os_str().is_empty() {
                std::path::Path::new(".")
            } else {
                parent
            };
            let dir = std::fs::File::open(parent_dir).map_err(|e| {
                std::io::Error::new(e.kind(), format!("open snapshot parent dir: {e}"))
            })?;
            dir.sync_all().map_err(|e| {
                std::io::Error::new(e.kind(), format!("fsync snapshot parent dir: {e}"))
            })?;
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

impl kvr::protocol::SnapshotSaver for SnapshotManager {
    fn save(&self, store: &ShardedKV) -> std::io::Result<()> {
        self.save_snapshot(store)
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
        if let Err(e) = mgr.save_snapshot(&store) {
            eprintln!("periodic snapshot failed: {e}");
        }
    }

    eprintln!("snapshot thread exiting");
}

// ─── Ping client (Docker healthcheck) ─────────────────────────────────

/// Connect to a UDS socket, send a PING, and verify the response.
/// Exits 0 on success (RESP_OK), 1 on any error.
/// Used by the Docker HEALTHCHECK — replaces the broken nc/printf/grep chain.
#[cfg(unix)]
fn ping_client(socket_path: &str) -> ! {
    let exit_code = (|| -> Result<i32, ()> {
        let mut stream = UnixStream::connect(socket_path).map_err(|e| {
            eprintln!("ping: failed to connect to {socket_path}: {e}");
        })?;

        // Set a read timeout so the healthcheck can't hang beyond Docker's timeout.
        let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));

        // Send PING frame using the shared protocol frame writer.
        write_frame(&mut stream, &[OP_PING]).map_err(|e| {
            eprintln!("ping: failed to write: {e}");
        })?;

        // Read response frame using the shared protocol frame reader.
        let resp = read_frame(&mut stream).map_err(|e| {
            eprintln!("ping: failed to read response: {e}");
        })?;

        // Verify the response is exactly [RESP_OK].
        if resp == [RESP_OK] {
            Ok(0)
        } else {
            eprintln!("ping: unexpected response: {resp:?}");
            Ok(1)
        }
    })();
    let exit_code = exit_code.unwrap_or(1);

    std::process::exit(exit_code);
}

// ─── Configuration ────────────────────────────────────────────────────

/// Server configuration parsed from `KVR_*` environment variables.
///
/// Fails startup on any malformed value instead of silently falling back to a
/// default, so a typo in a safety-relevant limit can't change behavior
/// unnoticed. String-typed vars (`KVR_SOCKET_PATH`, `KVR_TCP_ADDR`,
/// `KVR_SNAPSHOT_PATH`) are not parsed and keep their original lenient
/// handling.
#[cfg(unix)]
struct Config {
    socket_path: String,
    tcp_addr: Option<String>,
    max_entries: usize,
    max_connections: usize,
    sweep_interval_secs: u64,
    snapshot_path: Option<String>,
    snapshot_on_shutdown: bool,
    snapshot_interval_secs: u64,
}

#[cfg(unix)]
impl Config {
    fn load() -> Self {
        // Parse a numeric/bool env var. Unset or empty -> default. A present
        // but malformed value (bad syntax, or non-UTF-8) -> refuse to start
        // (a silent default would hide a typo).
        fn parse<T>(var: &str, default: T) -> T
        where
            T: std::str::FromStr,
            T::Err: std::fmt::Display,
        {
            match std::env::var(var) {
                Ok(v) if v.is_empty() => default,
                Ok(v) => match v.parse() {
                    Ok(parsed) => parsed,
                    Err(e) => {
                        eprintln!("kvr: invalid {var}={v:?} ({e}); refusing to start");
                        std::process::exit(1);
                    }
                },
                // Distinguish "unset" from "present but not valid UTF-8": the
                // latter is a real config error and must not silently default.
                Err(std::env::VarError::NotUnicode(_)) => {
                    eprintln!("kvr: {var} is set to a non-UTF-8 value; refusing to start");
                    std::process::exit(1);
                }
                Err(std::env::VarError::NotPresent) => default,
            }
        }

        let max_connections = parse("KVR_MAX_CONNECTIONS", MAX_CONNECTIONS);
        if max_connections == 0 {
            eprintln!("kvr: KVR_MAX_CONNECTIONS must be > 0; refusing to start");
            std::process::exit(1);
        }

        Self {
            socket_path: std::env::var("KVR_SOCKET_PATH")
                .unwrap_or_else(|_| DEFAULT_SOCKET_PATH.to_string()),
            tcp_addr: std::env::var("KVR_TCP_ADDR").ok(),
            max_entries: parse("KVR_MAX_ENTRIES", 100_000usize),
            max_connections,
            sweep_interval_secs: parse("KVR_SWEEP_INTERVAL_SECS", 30u64),
            snapshot_path: std::env::var("KVR_SNAPSHOT_PATH")
                .ok()
                .filter(|s| !s.is_empty()),
            snapshot_on_shutdown: parse("KVR_SNAPSHOT_ON_SHUTDOWN", true),
            snapshot_interval_secs: parse("KVR_SNAPSHOT_INTERVAL_SECS", 0u64),
        }
    }
}

// ─── Production entry point ───────────────────────────────────────────

#[cfg(unix)]
fn main() {
    // Docker healthcheck mode: kvr-server --ping <socket_path>
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 3 && args[1] == "--ping" {
        ping_client(&args[2]);
    }

    // Parse configuration (fails startup on malformed values).
    let config = Config::load();
    let Config {
        socket_path,
        tcp_addr,
        max_entries,
        max_connections,
        sweep_interval_secs,
        snapshot_path,
        snapshot_on_shutdown,
        snapshot_interval_secs,
    } = config;
    eprintln!(
        "kvr config: socket={socket_path:?}, tcp_addr={tcp_addr:?}, \
         max_entries={max_entries} (0 = unlimited), \
         max_connections={max_connections}, sweep_interval={sweep_interval_secs}s (0 = disabled), \
         snapshot_path={snapshot_path:?}, snapshot_on_shutdown={snapshot_on_shutdown}, \
         snapshot_interval={snapshot_interval_secs}s (0 = disabled)"
    );

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
    if let Some(addr) = tcp_addr {
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
            match mgr.save_snapshot(&store) {
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
