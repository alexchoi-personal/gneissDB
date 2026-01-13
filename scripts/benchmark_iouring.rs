use gneissdb::{Db, Options};
use std::time::Instant;
use tempfile::tempdir;

#[cfg(all(target_os = "linux", feature = "io_uring"))]
use gneissdb::IoEngine;

#[tokio::main]
async fn main() {
    println!("io_uring Benchmark\n");
    println!("==================\n");

    let count = 10000;

    println!("Standard I/O Benchmark:");
    println!("-----------------------");
    benchmark_standard(count).await;

    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    {
        println!("\nio_uring I/O Benchmark:");
        println!("-----------------------");
        benchmark_iouring(count).await;
    }

    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    {
        println!("\nio_uring not available (requires Linux + io_uring feature)");
    }
}

async fn benchmark_standard(count: usize) {
    let dir = tempdir().unwrap();
    let opts = Options::default().sync_writes(false);

    let start = Instant::now();
    {
        let db = Db::open(dir.path(), opts).await.unwrap();
        for i in 0..count {
            let key = format!("key{:08}", i);
            let value = format!("value{:08}", i);
            db.put(key, value).await.unwrap();
        }
        db.close().await.unwrap();
    }
    let write_time = start.elapsed();

    let start = Instant::now();
    {
        let db = Db::open(dir.path(), Options::default()).await.unwrap();
        for i in 0..count {
            let key = format!("key{:08}", i);
            let _ = db.get(key).await.unwrap();
        }
        db.close().await.unwrap();
    }
    let read_time = start.elapsed();

    println!(
        "  Write {} keys: {:.2}ms ({:.0} ops/sec)",
        count,
        write_time.as_secs_f64() * 1000.0,
        count as f64 / write_time.as_secs_f64()
    );
    println!(
        "  Read {} keys:  {:.2}ms ({:.0} ops/sec)",
        count,
        read_time.as_secs_f64() * 1000.0,
        count as f64 / read_time.as_secs_f64()
    );
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
async fn benchmark_iouring(count: usize) {
    let dir = tempdir().unwrap();
    let opts = Options::default()
        .sync_writes(false)
        .io_engine(IoEngine::IoUring);

    let start = Instant::now();
    {
        let db = Db::open(dir.path(), opts).await.unwrap();
        for i in 0..count {
            let key = format!("key{:08}", i);
            let value = format!("value{:08}", i);
            db.put(key, value).await.unwrap();
        }
        db.close().await.unwrap();
    }
    let write_time = start.elapsed();

    let start = Instant::now();
    {
        let opts = Options::default().io_engine(IoEngine::IoUring);
        let db = Db::open(dir.path(), opts).await.unwrap();
        for i in 0..count {
            let key = format!("key{:08}", i);
            let _ = db.get(key).await.unwrap();
        }
        db.close().await.unwrap();
    }
    let read_time = start.elapsed();

    println!(
        "  Write {} keys: {:.2}ms ({:.0} ops/sec)",
        count,
        write_time.as_secs_f64() * 1000.0,
        count as f64 / write_time.as_secs_f64()
    );
    println!(
        "  Read {} keys:  {:.2}ms ({:.0} ops/sec)",
        count,
        read_time.as_secs_f64() * 1000.0,
        count as f64 / read_time.as_secs_f64()
    );
}
