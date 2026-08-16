use kvr::ShardedKV;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Run a benchmark for a single operation type.
///
/// Each thread executes `ops_per_thread` operations against the shared store.
/// Returns (total_ops, elapsed).
fn bench_op(
    label: &str,
    num_threads: usize,
    ops_per_thread: u64,
    f: impl Fn(Arc<ShardedKV>, u64) -> u64 + Send + Sync + 'static,
) {
    let store = Arc::new(ShardedKV::new());

    // Pre-populate with 100K keys so read-heavy ops have data to work with.
    let num_keys: u64 = 100_000;
    let keys: Arc<Vec<String>> = Arc::new((0..num_keys).map(|i| format!("key-{i:05}")).collect());
    for key in keys.iter() {
        store.set(key, b"benchmark-value".to_vec());
    }

    let f = Arc::new(f);
    let keys = Arc::clone(&keys);
    let store_bench = Arc::clone(&store);

    let mut handles = vec![];
    let start = Instant::now();

    for _t in 0..num_threads {
        let store = Arc::clone(&store_bench);
        let keys = Arc::clone(&keys);
        let f = Arc::clone(&f);
        handles.push(thread::spawn(move || {
            let mut local_ops = 0u64;
            for i in 0..ops_per_thread {
                local_ops += f(store.clone(), i);
                // Suppress unused warning for keys when not needed.
                let _ = &keys;
            }
            local_ops
        }));
    }

    let total_ops: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    let elapsed = start.elapsed();
    let elapsed_s = elapsed.as_secs_f64();
    let ops_sec = total_ops as f64 / elapsed_s;
    let avg_us = elapsed.as_micros() as f64 / total_ops as f64;

    println!(
        "{label:<10} {total_ops:>10} ops | {elapsed_s:.3}s | {ops_sec:>12.0} ops/sec | {avg_us:>8.2} µs/op"
    );
}

fn main() {
    let num_threads = 8;
    let ops_per_thread = 500_000;
    let num_keys: u64 = 100_000;

    let keys: Arc<Vec<String>> = Arc::new((0..num_keys).map(|i| format!("key-{i:05}")).collect());

    println!("── kvr in-process benchmark ──");
    println!("Threads:       {num_threads}");
    println!("Ops/thread:    {ops_per_thread}");
    println!("Pre-populated: {num_keys} keys");
    println!();

    // ── SET ──────────────────────────────────────────────────────────
    bench_op("SET", num_threads, ops_per_thread, {
        let keys = Arc::clone(&keys);
        move |store, i| {
            let key_idx = i % num_keys;
            store.set(&keys[key_idx as usize], b"val".to_vec());
            1
        }
    });

    // ── GET (100% hits) ──────────────────────────────────────────────
    bench_op("GET", num_threads, ops_per_thread, {
        let keys = Arc::clone(&keys);
        move |store, i| {
            let key_idx = i % num_keys;
            let _ = store.get(&keys[key_idx as usize]);
            1
        }
    });

    // ── EXISTS ───────────────────────────────────────────────────────
    bench_op("EXISTS", num_threads, ops_per_thread, {
        let keys = Arc::clone(&keys);
        move |store, i| {
            let key_idx = i % num_keys;
            let _ = store.contains(&keys[key_idx as usize]);
            1
        }
    });

    // ── DEL (re-set before delete to keep the store populated) ───────
    bench_op("DEL", num_threads, ops_per_thread, {
        let keys = Arc::clone(&keys);
        move |store, i| {
            let key_idx = i % num_keys;
            let key = &keys[key_idx as usize];
            // Re-set then immediately delete — measures del on a live key.
            // Only the delete is counted; the set is a warm-up.
            store.set(key, b"v".to_vec());
            let _ = store.del(key);
            1
        }
    });

    // ── SETX (with TTL) ──────────────────────────────────────────────
    bench_op("SETX", num_threads, ops_per_thread, {
        let keys = Arc::clone(&keys);
        move |store, i| {
            let key_idx = i % num_keys;
            store.set_with_ttl(&keys[key_idx as usize], b"v".to_vec(), 3_600_000);
            1
        }
    });

    // ── TTL (query remaining TTL) ────────────────────────────────────
    // Pre-populate with SETX so keys have TTLs.
    {
        let store = Arc::new(ShardedKV::new());
        for key in keys.iter() {
            store.set_with_ttl(key, b"v".to_vec(), 3_600_000);
        }

        let mut handles = vec![];
        let start = Instant::now();

        for _t in 0..num_threads {
            let store = Arc::clone(&store);
            let keys = Arc::clone(&keys);
            handles.push(thread::spawn(move || {
                let mut local_ops = 0u64;
                for i in 0..ops_per_thread {
                    let key_idx = i % num_keys;
                    let _ = store.ttl(&keys[key_idx as usize]);
                    local_ops += 1;
                }
                local_ops
            }));
        }

        let total_ops: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let elapsed = start.elapsed();
        let elapsed_s = elapsed.as_secs_f64();
        let ops_sec = total_ops as f64 / elapsed_s;
        let avg_us = elapsed.as_micros() as f64 / total_ops as f64;
        println!(
            "TTL        {total_ops:>10} ops | {elapsed_s:.3}s | {ops_sec:>12.0} ops/sec | {avg_us:>8.2} µs/op"
        );
    }

    // ── MGET (batch of 10 keys per call) ─────────────────────────────
    {
        let store = Arc::new(ShardedKV::new());
        for key in keys.iter() {
            store.set(key, b"v".to_vec());
        }

        let batch_size = 10usize;
        let mut handles = vec![];
        let start = Instant::now();

        for _t in 0..num_threads {
            let store = Arc::clone(&store);
            let keys = Arc::clone(&keys);
            handles.push(thread::spawn(move || {
                let mut local_ops = 0u64;
                for i in 0..ops_per_thread {
                    let base = (i as usize * batch_size) % num_keys as usize;
                    let batch: Vec<String> = (0..batch_size)
                        .map(|j| keys[(base + j) % num_keys as usize].clone())
                        .collect();
                    let _ = store.mget(&batch);
                    local_ops += 1;
                }
                local_ops
            }));
        }

        let total_ops: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let elapsed = start.elapsed();
        let elapsed_s = elapsed.as_secs_f64();
        let ops_sec = total_ops as f64 / elapsed_s;
        let avg_us = elapsed.as_micros() as f64 / total_ops as f64;
        let keys_fetched = total_ops * batch_size as u64;
        let keys_sec = keys_fetched as f64 / elapsed_s;
        println!(
            "MGET(10)   {total_ops:>10} ops | {elapsed_s:.3}s | {ops_sec:>12.0} ops/sec | {avg_us:>8.2} µs/op | {keys_sec:>12.0} keys/sec"
        );
    }

    // ── SCAN (prefix scan, limit=100) ────────────────────────────────
    // SCAN is O(n) per call, so use fewer ops than the single-key benchmarks.
    {
        let store = Arc::new(ShardedKV::new());
        for key in keys.iter() {
            store.set(key, b"v".to_vec());
        }

        let scan_ops = 10_000; // 10K scans × 8 threads = 80K total scans
        let mut handles = vec![];
        let start = Instant::now();

        for _t in 0..num_threads {
            let store = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                let mut local_ops = 0u64;
                for i in 0..scan_ops {
                    // Cycle through prefixes to vary the scan.
                    let prefix = match i % 10 {
                        0 => "key-0",
                        1 => "key-1",
                        2 => "key-2",
                        3 => "key-3",
                        4 => "key-4",
                        5 => "key-5",
                        6 => "key-6",
                        7 => "key-7",
                        8 => "key-8",
                        _ => "key-9",
                    };
                    let _ = store.scan(prefix, 100, "");
                    local_ops += 1;
                }
                local_ops
            }));
        }

        let total_ops: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let elapsed = start.elapsed();
        let elapsed_s = elapsed.as_secs_f64();
        let ops_sec = total_ops as f64 / elapsed_s;
        let avg_us = elapsed.as_micros() as f64 / total_ops as f64;
        println!(
            "SCAN(100)  {total_ops:>10} ops | {elapsed_s:.3}s | {ops_sec:>12.0} ops/sec | {avg_us:>8.2} µs/op"
        );
    }

    // ── sweep_expired ────────────────────────────────────────────────
    {
        let store = Arc::new(ShardedKV::new());
        // Insert 100K keys with short TTL, wait for expiry, then sweep.
        for key in keys.iter() {
            store.set_with_ttl(key, b"v".to_vec(), 50);
        }
        thread::sleep(Duration::from_millis(80));

        let start = Instant::now();
        let removed = store.sweep_expired();
        let elapsed = start.elapsed();
        let elapsed_s = elapsed.as_secs_f64();
        let n = num_keys;
        let ops_sec = n as f64 / elapsed_s;
        let avg_us = elapsed.as_micros() as f64 / n as f64;
        println!(
            "SWEEP      {n:>10} ops | {elapsed_s:.3}s | {ops_sec:>12.0} ops/sec | {avg_us:>8.2} µs/op | removed={removed}"
        );
    }

    // ── len_active ───────────────────────────────────────────────────
    {
        let store = Arc::new(ShardedKV::new());
        for key in keys.iter() {
            store.set(key, b"v".to_vec());
        }
        // Insert some short-TTL keys and let them expire.
        for i in 0..10_000u64 {
            store.set_with_ttl(&format!("temp-{i}"), b"v".to_vec(), 50);
        }
        thread::sleep(Duration::from_millis(80));

        let start = Instant::now();
        let active = store.len_active();
        let elapsed = start.elapsed();
        let elapsed_s = elapsed.as_secs_f64();
        let n = num_keys + 10_000;
        let ops_sec = n as f64 / elapsed_s;
        let avg_us = elapsed.as_micros() as f64 / n as f64;
        println!(
            "LEN_ACTIVE {n:>10} ops | {elapsed_s:.3}s | {ops_sec:>12.0} ops/sec | {avg_us:>8.2} µs/op | active={active}"
        );
    }

    println!();
    println!("── Mixed workload (20% writes / 80% reads) ──");
    bench_mixed(num_threads, ops_per_thread, &keys);
}

fn bench_mixed(num_threads: usize, ops_per_thread: u64, keys: &Arc<Vec<String>>) {
    let num_keys = keys.len() as u64;
    let store = Arc::new(ShardedKV::new());
    for key in keys.iter() {
        store.set(key, b"benchmark-value".to_vec());
    }

    let mut handles = vec![];
    let start = Instant::now();

    for _t in 0..num_threads {
        let store = Arc::clone(&store);
        let keys = Arc::clone(keys);
        handles.push(thread::spawn(move || {
            let mut local_ops = 0u64;
            for i in 0..ops_per_thread {
                let key_idx = (i % num_keys) as usize;
                let key = &keys[key_idx];
                if i % 5 == 0 {
                    store.set(key, b"val".to_vec());
                } else {
                    let _ = store.get(key);
                }
                local_ops += 1;
            }
            local_ops
        }));
    }

    let total_ops: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    let elapsed = start.elapsed();
    let elapsed_s = elapsed.as_secs_f64();
    let ops_sec = total_ops as f64 / elapsed_s;
    let avg_us = elapsed.as_micros() as f64 / total_ops as f64;

    println!(
        "MIXED      {total_ops:>10} ops | {elapsed_s:.3}s | {ops_sec:>12.0} ops/sec | {avg_us:>8.2} µs/op"
    );
}
