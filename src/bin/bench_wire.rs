use kvr::ShardedKV;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const OP_SET: u8 = 0;
const OP_GET: u8 = 1;
const OP_PING: u8 = 3;
const RESP_OK: u8 = 0x10;
const SOCKET_PATH: &str = "/tmp/kvr_bench.sock";

fn write_frame(stream: &mut UnixStream, payload: &[u8]) -> std::io::Result<()> {
    stream.write_all(&(payload.len() as u32).to_be_bytes())?;
    stream.write_all(payload)?;
    stream.flush()
}

fn read_frame(stream: &mut UnixStream) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut frame = vec![0u8; len];
    stream.read_exact(&mut frame)?;
    Ok(frame)
}

fn make_set(key: &str, val: &[u8]) -> Vec<u8> {
    let mut frame = vec![OP_SET];
    let kl = key.len() as u16;
    frame.extend_from_slice(&kl.to_be_bytes());
    frame.extend_from_slice(key.as_bytes());
    let vl = val.len() as u32;
    frame.extend_from_slice(&vl.to_be_bytes());
    frame.extend_from_slice(val);
    frame
}

fn make_get(key: &str) -> Vec<u8> {
    let mut frame = vec![OP_GET];
    let kl = key.len() as u16;
    frame.extend_from_slice(&kl.to_be_bytes());
    frame.extend_from_slice(key.as_bytes());
    frame
}

fn make_ping() -> Vec<u8> {
    vec![OP_PING]
}

fn send_recv(stream: &mut UnixStream, frame: &[u8]) -> std::io::Result<Vec<u8>> {
    write_frame(stream, frame)?;
    read_frame(stream)
}

fn main() {
    // Start the server in-process.
    let store = Arc::new(ShardedKV::with_max_entries(0)); // unlimited
    let socket_path = SOCKET_PATH.to_string();

    // Clean up stale socket.
    let _ = std::fs::remove_file(&socket_path);

    // Import server internals — we need to start the UDS accept loop.
    // Since the server binary is separate, we'll start a minimal UDS server here.
    let listener =
        std::os::unix::net::UnixListener::bind(&socket_path).expect("failed to bind UDS for bench");
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600));

    let store_clone = Arc::clone(&store);
    let server_thread = thread::spawn(move || {
        use std::io::{BufReader, BufWriter};

        for stream in listener.incoming() {
            match stream {
                Ok(s) => {
                    let store = Arc::clone(&store_clone);
                    thread::spawn(move || {
                        let reader = match s.try_clone() {
                            Ok(r) => r,
                            Err(_) => return,
                        };
                        let mut reader = BufReader::new(reader);
                        let mut writer = BufWriter::new(s);

                        loop {
                            let mut len_buf = [0u8; 4];
                            if reader.read_exact(&mut len_buf).is_err() {
                                break;
                            }
                            let len = u32::from_be_bytes(len_buf) as usize;
                            let mut frame = vec![0u8; len];
                            if reader.read_exact(&mut frame).is_err() {
                                break;
                            }

                            // Dispatch
                            let resp = if frame.is_empty() {
                                vec![0xFF]
                            } else {
                                let opcode = frame[0];
                                let rest = &frame[1..];
                                match opcode {
                                    OP_SET => {
                                        if rest.len() < 6 {
                                            vec![0xFF]
                                        } else {
                                            let key_len =
                                                u16::from_be_bytes([rest[0], rest[1]]) as usize;
                                            let val_start = 2 + key_len;
                                            if rest.len() < val_start + 4 {
                                                vec![0xFF]
                                            } else {
                                                let val_len = u32::from_be_bytes([
                                                    rest[val_start],
                                                    rest[val_start + 1],
                                                    rest[val_start + 2],
                                                    rest[val_start + 3],
                                                ])
                                                    as usize;
                                                let val_end = val_start + 4 + val_len;
                                                if rest.len() != val_end {
                                                    vec![0xFF]
                                                } else {
                                                    let key = &rest[2..2 + key_len];
                                                    let val = &rest[val_start + 4..val_end];
                                                    store.set(
                                                        std::str::from_utf8(key).unwrap_or(""),
                                                        val.to_vec(),
                                                    );
                                                    vec![RESP_OK]
                                                }
                                            }
                                        }
                                    }
                                    OP_GET => {
                                        if rest.len() < 2 {
                                            vec![0xFF]
                                        } else {
                                            let key_len =
                                                u16::from_be_bytes([rest[0], rest[1]]) as usize;
                                            if rest.len() != 2 + key_len {
                                                vec![0xFF]
                                            } else {
                                                let key = &rest[2..2 + key_len];
                                                match store
                                                    .get(std::str::from_utf8(key).unwrap_or(""))
                                                {
                                                    Some(val) => {
                                                        let mut resp =
                                                            Vec::with_capacity(1 + 4 + val.len());
                                                        resp.push(RESP_OK);
                                                        resp.extend_from_slice(
                                                            &(val.len() as u32).to_be_bytes(),
                                                        );
                                                        resp.extend_from_slice(&val);
                                                        resp
                                                    }
                                                    None => vec![0x12],
                                                }
                                            }
                                        }
                                    }
                                    OP_PING => {
                                        if rest.is_empty() {
                                            vec![RESP_OK]
                                        } else {
                                            vec![0xFF]
                                        }
                                    }
                                    _ => vec![0xFF],
                                }
                            };

                            let _ = writer.write_all(&(resp.len() as u32).to_be_bytes());
                            let _ = writer.write_all(&resp);
                            let _ = writer.flush();
                        }
                    });
                }
                Err(_) => break,
            }
        }
    });

    // Wait for server to be ready.
    thread::sleep(Duration::from_millis(100));

    let num_threads = 4;
    let ops_per_thread = 50_000;
    let num_keys = 1_000;

    let keys: Arc<Vec<String>> = Arc::new((0..num_keys).map(|i| format!("key-{i:05}")).collect());

    let mut handles = vec![];
    let start = Instant::now();

    for _t in 0..num_threads {
        let keys = Arc::clone(&keys);
        handles.push(thread::spawn(move || {
            let mut stream = UnixStream::connect(SOCKET_PATH).expect("connect");

            // PING first to verify connection.
            let resp = send_recv(&mut stream, &make_ping()).expect("PING");
            assert_eq!(resp, vec![RESP_OK]);

            let mut local_ops = 0u64;
            for i in 0..ops_per_thread {
                let key_idx = i % num_keys;
                let key = &keys[key_idx];
                if i % 5 == 0 {
                    let val = format!("val-{i}").into_bytes();
                    let resp = send_recv(&mut stream, &make_set(key, &val)).expect("SET");
                    assert_eq!(resp, vec![RESP_OK]);
                } else {
                    let _resp = send_recv(&mut stream, &make_get(key)).expect("GET");
                }
                local_ops += 1;
            }
            local_ops
        }));
    }

    let total_ops: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    let elapsed = start.elapsed();
    let ops_sec = total_ops as f64 / elapsed.as_secs_f64();
    let p50_us = elapsed.as_micros() as f64 / total_ops as f64;

    println!("── kvr UDS wire benchmark ──");
    println!("Threads:       {num_threads}");
    println!("Ops/thread:    {ops_per_thread}");
    println!("Total ops:     {total_ops}");
    println!("Elapsed:       {:.3}s", elapsed.as_secs_f64());
    println!("Throughput:    {ops_sec:.0} ops/sec");
    println!("Avg latency:   {p50_us:.2} µs/op");

    // Cleanup
    let _ = std::fs::remove_file(SOCKET_PATH);
    drop(server_thread);
}
