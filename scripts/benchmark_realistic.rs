use gneissdb::{Db, Options, WriteOptions};
use rocksdb::{Options as RocksOptions, WriteOptions as RocksWriteOptions, DB};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

const VALUE_SIZE: usize = 1024;
const COUNT: usize = 500;

#[tokio::main]
async fn main() {
    println!("=== Realistic SSD Benchmark ===\n");
    println!("Config: {} ops, {} byte values\n", COUNT, VALUE_SIZE);

    let value = "x".repeat(VALUE_SIZE);

    println!("--- Test 1: With fsync (durable writes) ---\n");

    {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), Options::default()).await.unwrap();

        let start = Instant::now();
        for i in 0..COUNT {
            let key = format!("key{:08}", i);
            db.put_opt(key, value.clone(), WriteOptions::default().sync(true))
                .await
                .unwrap();
        }
        let elapsed = start.elapsed();

        println!(
            "rocksdb-rs (sync=true): {:.2}ms = {:.0} ops/sec",
            elapsed.as_secs_f64() * 1000.0,
            COUNT as f64 / elapsed.as_secs_f64()
        );

        db.close().await.unwrap();
    }

    {
        let dir = tempdir().unwrap();
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, dir.path()).unwrap();

        let mut write_opts = RocksWriteOptions::default();
        write_opts.set_sync(true);

        let start = Instant::now();
        for i in 0..COUNT {
            let key = format!("key{:08}", i);
            db.put_opt(key.as_bytes(), value.as_bytes(), &write_opts)
                .unwrap();
        }
        let elapsed = start.elapsed();

        println!(
            "RocksDB (sync=true):    {:.2}ms = {:.0} ops/sec",
            elapsed.as_secs_f64() * 1000.0,
            COUNT as f64 / elapsed.as_secs_f64()
        );
    }

    println!("\n--- Test 2: Without fsync (buffered) ---\n");

    {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), Options::default().sync_writes(false))
            .await
            .unwrap();

        let start = Instant::now();
        for i in 0..COUNT {
            let key = format!("key{:08}", i);
            db.put(key, value.clone()).await.unwrap();
        }
        let elapsed = start.elapsed();

        println!(
            "rocksdb-rs (sync=false): {:.2}ms = {:.0} ops/sec",
            elapsed.as_secs_f64() * 1000.0,
            COUNT as f64 / elapsed.as_secs_f64()
        );

        db.close().await.unwrap();
    }

    {
        let dir = tempdir().unwrap();
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, dir.path()).unwrap();

        let start = Instant::now();
        for i in 0..COUNT {
            let key = format!("key{:08}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        let elapsed = start.elapsed();

        println!(
            "RocksDB (sync=false):    {:.2}ms = {:.0} ops/sec",
            elapsed.as_secs_f64() * 1000.0,
            COUNT as f64 / elapsed.as_secs_f64()
        );
    }

    println!("\n--- Test 3: Concurrent sync writes (group commit effectiveness) ---\n");

    {
        let dir = tempdir().unwrap();
        let db = Arc::new(Db::open(dir.path(), Options::default()).await.unwrap());
        let completed = Arc::new(AtomicU64::new(0));

        let num_writers = 8;
        let writes_per_writer = 1000;
        let total_writes = num_writers * writes_per_writer;

        let start = Instant::now();
        let mut handles = Vec::new();

        for writer_id in 0..num_writers {
            let db = db.clone();
            let completed = completed.clone();
            let value = value.clone();

            let handle = tokio::spawn(async move {
                for i in 0..writes_per_writer {
                    let key = format!("key{:04}_{:06}", writer_id, i);
                    db.put_opt(key, value.clone(), WriteOptions::default().sync(true))
                        .await
                        .unwrap();
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let elapsed = start.elapsed();
        let ops_per_sec = total_writes as f64 / elapsed.as_secs_f64();

        println!(
            "rocksdb-rs concurrent sync ({} writers, {} writes each):",
            num_writers, writes_per_writer
        );
        println!(
            "  Total: {:.2}ms = {:.0} ops/sec",
            elapsed.as_secs_f64() * 1000.0,
            ops_per_sec
        );
        println!(
            "  Estimated fsyncs: ~{} (if group commit working: ~{})",
            total_writes,
            total_writes / 10
        );

        let inner_db = Arc::try_unwrap(db).ok().unwrap();
        inner_db.close().await.unwrap();
    }

    println!("\n--- Test 4: Large dataset ---\n");

    let large_count = 10_000;
    let large_value = "y".repeat(1024);

    {
        let dir = tempdir().unwrap();
        let db = Db::open(
            dir.path(),
            Options::default()
                .sync_writes(false)
                .memtable_size(4 * 1024 * 1024),
        )
        .await
        .unwrap();

        let start = Instant::now();
        for i in 0..large_count {
            let key = format!("key{:08}", i);
            db.put(key, large_value.clone()).await.unwrap();
        }
        db.flush().await.unwrap();
        let elapsed = start.elapsed();

        println!(
            "rocksdb-rs ({} x 1KB): {:.2}ms = {:.0} ops/sec",
            large_count,
            elapsed.as_secs_f64() * 1000.0,
            large_count as f64 / elapsed.as_secs_f64()
        );

        db.close().await.unwrap();
    }

    {
        let dir = tempdir().unwrap();
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, dir.path()).unwrap();

        let start = Instant::now();
        for i in 0..large_count {
            let key = format!("key{:08}", i);
            db.put(key.as_bytes(), large_value.as_bytes()).unwrap();
        }
        db.flush().unwrap();
        let elapsed = start.elapsed();

        println!(
            "RocksDB ({} x 1KB):    {:.2}ms = {:.0} ops/sec",
            large_count,
            elapsed.as_secs_f64() * 1000.0,
            large_count as f64 / elapsed.as_secs_f64()
        );
    }

    println!("\n--- Test 5: Random reads from SSTable ---\n");

    {
        let dir = tempdir().unwrap();
        let db = Db::open(
            dir.path(),
            Options::default()
                .sync_writes(false)
                .memtable_size(1024 * 1024),
        )
        .await
        .unwrap();

        for i in 0..5_000 {
            let key = format!("key{:08}", i);
            db.put(key, large_value.clone()).await.unwrap();
        }
        db.flush().await.unwrap();

        let indices: Vec<usize> = (0..1_000).map(|i| (i * 7919) % 5_000).collect();

        let start = Instant::now();
        for &i in &indices {
            let key = format!("key{:08}", i);
            let _ = db.get(&key).await.unwrap();
        }
        let elapsed = start.elapsed();

        println!(
            "rocksdb-rs random SSTable reads: {:.2}ms = {:.0} ops/sec",
            elapsed.as_secs_f64() * 1000.0,
            1_000.0 / elapsed.as_secs_f64()
        );

        db.close().await.unwrap();
    }

    {
        let dir = tempdir().unwrap();
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, dir.path()).unwrap();

        for i in 0..5_000 {
            let key = format!("key{:08}", i);
            db.put(key.as_bytes(), large_value.as_bytes()).unwrap();
        }
        db.flush().unwrap();

        let indices: Vec<usize> = (0..1_000).map(|i| (i * 7919) % 5_000).collect();

        let start = Instant::now();
        for &i in &indices {
            let key = format!("key{:08}", i);
            let _ = db.get(key.as_bytes()).unwrap();
        }
        let elapsed = start.elapsed();

        println!(
            "RocksDB random SSTable reads:    {:.2}ms = {:.0} ops/sec",
            elapsed.as_secs_f64() * 1000.0,
            1_000.0 / elapsed.as_secs_f64()
        );
    }

    println!("\n--- Summary ---");
    println!("The concurrent sync test shows group commit effectiveness.");
    println!("If group commit is working, concurrent sync writes should be much faster");
    println!("than sequential sync writes (fewer fsyncs due to batching).");
}
