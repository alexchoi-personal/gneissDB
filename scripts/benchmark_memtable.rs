use gneissdb::{Db, Options};
use rocksdb::{Options as RocksOptions, WriteOptions as RocksWriteOptions, DB};
use std::time::Instant;
use tempfile::tempdir;

#[tokio::main]
async fn main() {
    println!("Memtable-focused benchmark (no sync writes)\n");

    let count = 10000;
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();

    println!("Writing {} keys (no sync)...\n", count);

    let start = Instant::now();
    {
        let opts = Options::default().sync_writes(false);
        let db = Db::open(dir1.path(), opts).await.unwrap();
        for i in 0..count {
            let key = format!("key{:08}", i);
            let value = format!("value{:08}", i);
            db.put(key, value).await.unwrap();
        }
        db.close().await.unwrap();
    }
    let gneissdb_time = start.elapsed();

    let start = Instant::now();
    {
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, dir2.path()).unwrap();
        let write_opts = RocksWriteOptions::default();
        for i in 0..count {
            let key = format!("key{:08}", i);
            let value = format!("value{:08}", i);
            db.put_opt(key.as_bytes(), value.as_bytes(), &write_opts)
                .unwrap();
        }
    }
    let rocksdb_time = start.elapsed();

    println!("Results (no sync writes):");
    println!(
        "  rocksdb-rs: {:.2}ms ({:.0} ops/sec)",
        gneissdb_time.as_secs_f64() * 1000.0,
        count as f64 / gneissdb_time.as_secs_f64()
    );
    println!(
        "  RocksDB:    {:.2}ms ({:.0} ops/sec)",
        rocksdb_time.as_secs_f64() * 1000.0,
        count as f64 / rocksdb_time.as_secs_f64()
    );

    let ratio = gneissdb_time.as_secs_f64() / rocksdb_time.as_secs_f64();
    if ratio > 1.0 {
        println!("  Ratio: rocksdb-rs is {:.2}x slower", ratio);
    } else {
        println!("  Ratio: rocksdb-rs is {:.2}x faster", 1.0 / ratio);
    }

    println!("\nRead benchmark (from memtable)...\n");

    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();

    {
        let opts = Options::default().sync_writes(false);
        let db = Db::open(dir1.path(), opts).await.unwrap();
        for i in 0..count {
            db.put(format!("key{:08}", i), format!("value{:08}", i))
                .await
                .unwrap();
        }
        db.close().await.unwrap();
    }
    {
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, dir2.path()).unwrap();
        for i in 0..count {
            db.put(
                format!("key{:08}", i).as_bytes(),
                format!("value{:08}", i).as_bytes(),
            )
            .unwrap();
        }
    }

    let start = Instant::now();
    {
        let db = Db::open(dir1.path(), Options::default()).await.unwrap();
        for i in 0..count {
            let _ = db.get(format!("key{:08}", i)).await.unwrap();
        }
        db.close().await.unwrap();
    }
    let gneissdb_read = start.elapsed();

    let start = Instant::now();
    {
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, dir2.path()).unwrap();
        for i in 0..count {
            let _ = db.get(format!("key{:08}", i).as_bytes()).unwrap();
        }
    }
    let rocksdb_read = start.elapsed();

    println!("Results (reads from recovered data):");
    println!(
        "  rocksdb-rs: {:.2}ms ({:.0} ops/sec)",
        gneissdb_read.as_secs_f64() * 1000.0,
        count as f64 / gneissdb_read.as_secs_f64()
    );
    println!(
        "  RocksDB:    {:.2}ms ({:.0} ops/sec)",
        rocksdb_read.as_secs_f64() * 1000.0,
        count as f64 / rocksdb_read.as_secs_f64()
    );

    let ratio = gneissdb_read.as_secs_f64() / rocksdb_read.as_secs_f64();
    if ratio > 1.0 {
        println!("  Ratio: rocksdb-rs is {:.2}x slower", ratio);
    } else {
        println!("  Ratio: rocksdb-rs is {:.2}x faster", 1.0 / ratio);
    }
}
