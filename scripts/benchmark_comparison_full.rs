use bytes::Bytes;
use rocksdb::{DB, Options as RocksOptions, WriteOptions as RocksWriteOptions};
use gneissdb::{Db, Options, WriteOptions};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

struct BenchResult {
    name: String,
    gneissdb_ops: f64,
    rocksdb_ops: f64,
    skip_comparison: bool,
    note: Option<&'static str>,
}

impl BenchResult {
    fn new(name: &str, gneissdb_ops: f64, rocksdb_ops: f64) -> Self {
        Self {
            name: name.to_string(),
            gneissdb_ops,
            rocksdb_ops,
            skip_comparison: false,
            note: None,
        }
    }

    fn with_note(mut self, note: &'static str) -> Self {
        self.note = Some(note);
        self.skip_comparison = true;
        self
    }

    fn ratio(&self) -> f64 {
        self.gneissdb_ops / self.rocksdb_ops
    }
}

fn print_header() {
    println!("\n{}", "=".repeat(80));
    println!("                    RocksDB vs rocksdb-rs Benchmark Comparison");
    println!("{}\n", "=".repeat(80));
}

fn print_result(result: &BenchResult) {
    let ratio = result.ratio();
    let status = if result.skip_comparison {
        "⚠"
    } else if ratio >= 1.0 {
        "✓"
    } else {
        "✗"
    };
    let ratio_str = if result.skip_comparison {
        "N/A (see note)".to_string()
    } else if ratio >= 1.0 {
        format!("{:.1}x faster", ratio)
    } else {
        format!("{:.1}x slower", 1.0 / ratio)
    };

    println!(
        "{} {:<35} rocksdb-rs: {:>12.0} ops/s | RocksDB: {:>12.0} ops/s | {}",
        status, result.name, result.gneissdb_ops, result.rocksdb_ops, ratio_str
    );
    if let Some(note) = result.note {
        println!("   └─ Note: {}", note);
    }
}

fn print_summary(results: &[BenchResult]) {
    println!("\n{}", "-".repeat(80));
    println!("SUMMARY");
    println!("{}", "-".repeat(80));

    let comparable: Vec<_> = results.iter().filter(|r| !r.skip_comparison).collect();
    let wins = comparable.iter().filter(|r| r.ratio() >= 1.0).count();
    let losses = comparable.len() - wins;
    let skipped = results.len() - comparable.len();

    println!(
        "rocksdb-rs wins: {} | RocksDB wins: {} | Skipped: {}",
        wins, losses, skipped
    );

    if !comparable.is_empty() {
        let avg_ratio: f64 =
            comparable.iter().map(|r| r.ratio()).sum::<f64>() / comparable.len() as f64;
        println!("Average performance ratio: {:.2}x", avg_ratio);
    }

    if skipped > 0 {
        println!("\n⚠ Note: Sync benchmarks skipped from comparison.");
        println!("  On macOS, librocksdb-sys doesn't enable F_FULLFSYNC, so RocksDB's");
        println!("  'sync' writes don't actually sync to disk. rocksdb-rs does real sync.");
    }
}

#[tokio::main]
async fn main() {
    print_header();

    let mut results = Vec::new();

    println!("Running benchmarks... (this may take a few minutes)\n");

    results.push(bench_sequential_write_nosync().await);
    results.push(bench_sequential_write_sync().await);
    results.push(bench_concurrent_write_nosync().await);
    results.push(bench_concurrent_write_sync().await);
    results.push(bench_batch_write().await);
    results.push(bench_random_read_memtable().await);
    results.push(bench_random_read_sstable().await);
    results.push(bench_sequential_read().await);
    results.push(bench_range_scan().await);
    results.push(bench_small_values().await);
    results.push(bench_large_values().await);
    results.push(bench_mixed_workload().await);

    println!("\n{}", "=".repeat(80));
    println!("                              RESULTS");
    println!("{}", "=".repeat(80));

    for result in &results {
        print_result(result);
    }

    print_summary(&results);
}

async fn bench_sequential_write_nosync() -> BenchResult {
    let count = 10_000;
    let value = "x".repeat(256);

    let gneissdb_ops = {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), Options::default().sync_writes(false))
            .await
            .unwrap();

        let start = Instant::now();
        for i in 0..count {
            let key = format!("key{:08}", i);
            db.put(key, value.clone()).await.unwrap();
        }
        let elapsed = start.elapsed();
        db.close().await.unwrap();

        count as f64 / elapsed.as_secs_f64()
    };

    let rocksdb_ops = {
        let dir = tempdir().unwrap();
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, dir.path()).unwrap();

        let start = Instant::now();
        for i in 0..count {
            let key = format!("key{:08}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        let elapsed = start.elapsed();

        count as f64 / elapsed.as_secs_f64()
    };

    BenchResult::new("Sequential Write (no sync)", gneissdb_ops, rocksdb_ops)
}

async fn bench_sequential_write_sync() -> BenchResult {
    let count = 100;
    let value = "x".repeat(256);

    let gneissdb_ops = {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), Options::default()).await.unwrap();

        let start = Instant::now();
        for i in 0..count {
            let key = format!("key{:08}", i);
            db.put_opt(key, value.clone(), WriteOptions::default().sync(true))
                .await
                .unwrap();
        }
        let elapsed = start.elapsed();
        db.close().await.unwrap();

        count as f64 / elapsed.as_secs_f64()
    };

    let rocksdb_ops = {
        let dir = tempdir().unwrap();
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, dir.path()).unwrap();

        let mut write_opts = RocksWriteOptions::default();
        write_opts.set_sync(true);

        let start = Instant::now();
        for i in 0..count {
            let key = format!("key{:08}", i);
            db.put_opt(key.as_bytes(), value.as_bytes(), &write_opts)
                .unwrap();
        }
        let elapsed = start.elapsed();

        count as f64 / elapsed.as_secs_f64()
    };

    BenchResult::new("Sequential Write (sync)", gneissdb_ops, rocksdb_ops)
        .with_note("RocksDB not actually syncing on macOS (missing F_FULLFSYNC)")
}

async fn bench_concurrent_write_nosync() -> BenchResult {
    let num_writers = 8;
    let writes_per_writer = 5_000;
    let total = num_writers * writes_per_writer;
    let value = "x".repeat(256);

    let gneissdb_ops = {
        let dir = tempdir().unwrap();
        let db = Arc::new(
            Db::open(dir.path(), Options::default().sync_writes(false))
                .await
                .unwrap(),
        );

        let start = Instant::now();
        let mut handles = Vec::new();

        for writer_id in 0..num_writers {
            let db = db.clone();
            let value = value.clone();
            let handle = tokio::spawn(async move {
                for i in 0..writes_per_writer {
                    let key = format!("key{:04}_{:06}", writer_id, i);
                    db.put(key, value.clone()).await.unwrap();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
        let elapsed = start.elapsed();

        let db = Arc::try_unwrap(db).ok().unwrap();
        db.close().await.unwrap();

        total as f64 / elapsed.as_secs_f64()
    };

    let rocksdb_ops = {
        let dir = tempdir().unwrap();
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = Arc::new(DB::open(&opts, dir.path()).unwrap());

        let start = Instant::now();
        let mut handles = Vec::new();

        for writer_id in 0..num_writers {
            let db = db.clone();
            let value = value.clone();
            let handle = std::thread::spawn(move || {
                for i in 0..writes_per_writer {
                    let key = format!("key{:04}_{:06}", writer_id, i);
                    db.put(key.as_bytes(), value.as_bytes()).unwrap();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
        let elapsed = start.elapsed();

        total as f64 / elapsed.as_secs_f64()
    };

    BenchResult::new("Concurrent Write (no sync, 8 threads)", gneissdb_ops, rocksdb_ops)
}

async fn bench_concurrent_write_sync() -> BenchResult {
    let num_writers = 8;
    let writes_per_writer = 100;
    let total = num_writers * writes_per_writer;
    let value = "x".repeat(256);

    let gneissdb_ops = {
        let dir = tempdir().unwrap();
        let db = Arc::new(Db::open(dir.path(), Options::default()).await.unwrap());

        let start = Instant::now();
        let mut handles = Vec::new();

        for writer_id in 0..num_writers {
            let db = db.clone();
            let value = value.clone();
            let handle = tokio::spawn(async move {
                for i in 0..writes_per_writer {
                    let key = format!("key{:04}_{:06}", writer_id, i);
                    db.put_opt(key, value.clone(), WriteOptions::default().sync(true))
                        .await
                        .unwrap();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
        let elapsed = start.elapsed();

        let db = Arc::try_unwrap(db).ok().unwrap();
        db.close().await.unwrap();

        total as f64 / elapsed.as_secs_f64()
    };

    let rocksdb_ops = {
        let dir = tempdir().unwrap();
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = Arc::new(DB::open(&opts, dir.path()).unwrap());

        let mut write_opts = RocksWriteOptions::default();
        write_opts.set_sync(true);

        let start = Instant::now();
        let mut handles = Vec::new();

        for writer_id in 0..num_writers {
            let db = db.clone();
            let value = value.clone();
            let handle = std::thread::spawn(move || {
                let mut write_opts = RocksWriteOptions::default();
                write_opts.set_sync(true);
                for i in 0..writes_per_writer {
                    let key = format!("key{:04}_{:06}", writer_id, i);
                    db.put_opt(key.as_bytes(), value.as_bytes(), &write_opts)
                        .unwrap();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
        let elapsed = start.elapsed();

        total as f64 / elapsed.as_secs_f64()
    };

    BenchResult::new("Concurrent Write (sync, 8 threads)", gneissdb_ops, rocksdb_ops)
        .with_note("RocksDB not actually syncing on macOS (missing F_FULLFSYNC)")
}

async fn bench_batch_write() -> BenchResult {
    let batch_count = 1_000;
    let ops_per_batch = 100;
    let value = Bytes::from("x".repeat(256));

    let keys: Vec<Vec<Bytes>> = (0..batch_count)
        .map(|batch_id| {
            (0..ops_per_batch)
                .map(|i| Bytes::from(format!("key{:04}_{:06}", batch_id, i)))
                .collect()
        })
        .collect();

    let gneissdb_ops = {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), Options::default()
            .sync_writes(false)
            .memtable_size(256 * 1024 * 1024))
            .await
            .unwrap();

        let start = Instant::now();
        for batch_keys in &keys {
            let mut batch = gneissdb::WriteBatch::new();
            for key in batch_keys {
                batch.put(key.clone(), value.clone());
            }
            db.write(batch).await.unwrap();
        }
        let elapsed = start.elapsed();
        db.close().await.unwrap();

        (batch_count * ops_per_batch) as f64 / elapsed.as_secs_f64()
    };

    let rocksdb_ops = {
        let dir = tempdir().unwrap();
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        opts.set_write_buffer_size(256 * 1024 * 1024);
        let db = DB::open(&opts, dir.path()).unwrap();

        let start = Instant::now();
        for batch_keys in &keys {
            let mut batch = rocksdb::WriteBatch::default();
            for key in batch_keys {
                batch.put(key.as_ref(), value.as_ref());
            }
            db.write(batch).unwrap();
        }
        let elapsed = start.elapsed();

        (batch_count * ops_per_batch) as f64 / elapsed.as_secs_f64()
    };

    BenchResult::new("Batch Write (100 ops/batch)", gneissdb_ops, rocksdb_ops)
}

async fn bench_random_read_memtable() -> BenchResult {
    let count = 10_000;
    let value = "x".repeat(256);

    let gneissdb_ops = {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), Options::default().sync_writes(false))
            .await
            .unwrap();

        for i in 0..count {
            let key = format!("key{:08}", i);
            db.put(key, value.clone()).await.unwrap();
        }

        let indices: Vec<usize> = (0..count).map(|i| (i * 7919) % count).collect();

        let start = Instant::now();
        for &i in &indices {
            let key = format!("key{:08}", i);
            let _ = db.get(&key).await.unwrap();
        }
        let elapsed = start.elapsed();
        db.close().await.unwrap();

        count as f64 / elapsed.as_secs_f64()
    };

    let rocksdb_ops = {
        let dir = tempdir().unwrap();
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, dir.path()).unwrap();

        for i in 0..count {
            let key = format!("key{:08}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let indices: Vec<usize> = (0..count).map(|i| (i * 7919) % count).collect();

        let start = Instant::now();
        for &i in &indices {
            let key = format!("key{:08}", i);
            let _ = db.get(key.as_bytes()).unwrap();
        }
        let elapsed = start.elapsed();

        count as f64 / elapsed.as_secs_f64()
    };

    BenchResult::new("Random Read (memtable)", gneissdb_ops, rocksdb_ops)
}

async fn bench_random_read_sstable() -> BenchResult {
    let write_count = 5_000;
    let read_count = 2_000;
    let value = "x".repeat(256);

    let gneissdb_ops = {
        let dir = tempdir().unwrap();
        let db = Db::open(
            dir.path(),
            Options::default()
                .sync_writes(false)
                .memtable_size(512 * 1024),
        )
        .await
        .unwrap();

        for i in 0..write_count {
            let key = format!("key{:08}", i);
            db.put(key, value.clone()).await.unwrap();
        }
        db.flush().await.unwrap();

        let indices: Vec<usize> = (0..read_count).map(|i| (i * 7919) % write_count).collect();

        let start = Instant::now();
        for &i in &indices {
            let key = format!("key{:08}", i);
            let _ = db.get(&key).await.unwrap();
        }
        let elapsed = start.elapsed();
        db.close().await.unwrap();

        read_count as f64 / elapsed.as_secs_f64()
    };

    let rocksdb_ops = {
        let dir = tempdir().unwrap();
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, dir.path()).unwrap();

        for i in 0..write_count {
            let key = format!("key{:08}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        db.flush().unwrap();

        let indices: Vec<usize> = (0..read_count).map(|i| (i * 7919) % write_count).collect();

        let start = Instant::now();
        for &i in &indices {
            let key = format!("key{:08}", i);
            let _ = db.get(key.as_bytes()).unwrap();
        }
        let elapsed = start.elapsed();

        read_count as f64 / elapsed.as_secs_f64()
    };

    BenchResult::new("Random Read (SSTable)", gneissdb_ops, rocksdb_ops)
}

async fn bench_sequential_read() -> BenchResult {
    let count = 10_000;
    let value = "x".repeat(256);

    let gneissdb_ops = {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), Options::default().sync_writes(false))
            .await
            .unwrap();

        for i in 0..count {
            let key = format!("key{:08}", i);
            db.put(key, value.clone()).await.unwrap();
        }

        let start = Instant::now();
        for i in 0..count {
            let key = format!("key{:08}", i);
            let _ = db.get(&key).await.unwrap();
        }
        let elapsed = start.elapsed();
        db.close().await.unwrap();

        count as f64 / elapsed.as_secs_f64()
    };

    let rocksdb_ops = {
        let dir = tempdir().unwrap();
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, dir.path()).unwrap();

        for i in 0..count {
            let key = format!("key{:08}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let start = Instant::now();
        for i in 0..count {
            let key = format!("key{:08}", i);
            let _ = db.get(key.as_bytes()).unwrap();
        }
        let elapsed = start.elapsed();

        count as f64 / elapsed.as_secs_f64()
    };

    BenchResult::new("Sequential Read (memtable)", gneissdb_ops, rocksdb_ops)
}

async fn bench_range_scan() -> BenchResult {
    let count = 10_000;
    let scan_count = 1_000;
    let scan_size = 100;
    let value = "x".repeat(256);

    let gneissdb_ops = {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), Options::default().sync_writes(false))
            .await
            .unwrap();

        for i in 0..count {
            let key = format!("key{:08}", i);
            db.put(key, value.clone()).await.unwrap();
        }

        let start = Instant::now();
        for i in 0..scan_count {
            let start_key = format!("key{:08}", (i * 7) % (count - scan_size));
            let end_key = format!("key{:08}", (i * 7) % (count - scan_size) + scan_size);
            let results = db.scan(&start_key, &end_key, scan_size);
            std::hint::black_box(results);
        }
        let elapsed = start.elapsed();
        db.close().await.unwrap();

        (scan_count * scan_size) as f64 / elapsed.as_secs_f64()
    };

    let rocksdb_ops = {
        let dir = tempdir().unwrap();
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, dir.path()).unwrap();

        for i in 0..count {
            let key = format!("key{:08}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let start = Instant::now();
        for i in 0..scan_count {
            let start_key = format!("key{:08}", (i * 7) % (count - scan_size));
            let end_key = format!("key{:08}", (i * 7) % (count - scan_size) + scan_size);
            let mut count = 0;
            let iter = db.iterator(rocksdb::IteratorMode::From(
                start_key.as_bytes(),
                rocksdb::Direction::Forward,
            ));
            for item in iter {
                let (k, _v) = item.unwrap();
                if k.as_ref() >= end_key.as_bytes() {
                    break;
                }
                count += 1;
                if count >= scan_size {
                    break;
                }
            }
            std::hint::black_box(count);
        }
        let elapsed = start.elapsed();

        (scan_count * scan_size) as f64 / elapsed.as_secs_f64()
    };

    BenchResult::new("Range Scan (100 keys)", gneissdb_ops, rocksdb_ops)
}

async fn bench_small_values() -> BenchResult {
    let count = 50_000;
    let value = "x".repeat(16);

    let gneissdb_ops = {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), Options::default().sync_writes(false))
            .await
            .unwrap();

        let start = Instant::now();
        for i in 0..count {
            let key = format!("k{:06}", i);
            db.put(key, value.clone()).await.unwrap();
        }
        let elapsed = start.elapsed();
        db.close().await.unwrap();

        count as f64 / elapsed.as_secs_f64()
    };

    let rocksdb_ops = {
        let dir = tempdir().unwrap();
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, dir.path()).unwrap();

        let start = Instant::now();
        for i in 0..count {
            let key = format!("k{:06}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        let elapsed = start.elapsed();

        count as f64 / elapsed.as_secs_f64()
    };

    BenchResult::new("Small Values (16 bytes)", gneissdb_ops, rocksdb_ops)
}

async fn bench_large_values() -> BenchResult {
    let count = 2_000;
    let value = "x".repeat(4096);

    let gneissdb_ops = {
        let dir = tempdir().unwrap();
        let db = Db::open(
            dir.path(),
            Options::default()
                .sync_writes(false)
                .memtable_size(16 * 1024 * 1024),
        )
        .await
        .unwrap();

        let start = Instant::now();
        for i in 0..count {
            let key = format!("key{:08}", i);
            db.put(key, value.clone()).await.unwrap();
        }
        let elapsed = start.elapsed();
        db.close().await.unwrap();

        count as f64 / elapsed.as_secs_f64()
    };

    let rocksdb_ops = {
        let dir = tempdir().unwrap();
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, dir.path()).unwrap();

        let start = Instant::now();
        for i in 0..count {
            let key = format!("key{:08}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        let elapsed = start.elapsed();

        count as f64 / elapsed.as_secs_f64()
    };

    BenchResult::new("Large Values (4KB)", gneissdb_ops, rocksdb_ops)
}

async fn bench_mixed_workload() -> BenchResult {
    let ops = 10_000;
    let value = "x".repeat(256);

    let gneissdb_ops = {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), Options::default().sync_writes(false))
            .await
            .unwrap();

        for i in 0..1000 {
            let key = format!("key{:08}", i);
            db.put(key, value.clone()).await.unwrap();
        }

        let start = Instant::now();
        for i in 0..ops {
            let key = format!("key{:08}", i % 2000);
            if i % 3 == 0 {
                let _ = db.get(&key).await.unwrap();
            } else {
                db.put(key, value.clone()).await.unwrap();
            }
        }
        let elapsed = start.elapsed();
        db.close().await.unwrap();

        ops as f64 / elapsed.as_secs_f64()
    };

    let rocksdb_ops = {
        let dir = tempdir().unwrap();
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, dir.path()).unwrap();

        for i in 0..1000 {
            let key = format!("key{:08}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let start = Instant::now();
        for i in 0..ops {
            let key = format!("key{:08}", i % 2000);
            if i % 3 == 0 {
                let _ = db.get(key.as_bytes()).unwrap();
            } else {
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        }
        let elapsed = start.elapsed();

        ops as f64 / elapsed.as_secs_f64()
    };

    BenchResult::new("Mixed (67% write, 33% read)", gneissdb_ops, rocksdb_ops)
}
