use gneissdb::{Db, Options};
use std::time::Instant;
use tempfile::tempdir;

#[tokio::main]
async fn main() {
    let dir = tempdir().unwrap();
    let count = 10000;

    println!("=== Write Profiling ({} ops) ===\n", count);

    let start = Instant::now();
    let db = Db::open(dir.path(), Options::default().sync_writes(false))
        .await
        .unwrap();
    println!("db_open: {:.3}ms", start.elapsed().as_secs_f64() * 1000.0);

    let start = Instant::now();
    for i in 0..count {
        let key = format!("key{:08}", i);
        let value = format!("value{:08}", i);
        db.put(key, value).await.unwrap();
    }
    let write_time = start.elapsed();
    println!(
        "writes:  {:.3}ms ({:.0} ops/sec)",
        write_time.as_secs_f64() * 1000.0,
        count as f64 / write_time.as_secs_f64()
    );

    let start = Instant::now();
    db.flush().await.unwrap();
    println!("flush:   {:.3}ms", start.elapsed().as_secs_f64() * 1000.0);

    let start = Instant::now();
    db.close().await.unwrap();
    println!("close:   {:.3}ms", start.elapsed().as_secs_f64() * 1000.0);

    println!("\n=== Read Profiling ({} ops) ===\n", count);

    let start = Instant::now();
    let db = Db::open(dir.path(), Options::default()).await.unwrap();
    println!(
        "db_open (recovery): {:.3}ms",
        start.elapsed().as_secs_f64() * 1000.0
    );

    let start = Instant::now();
    for i in 0..count {
        let key = format!("key{:08}", i);
        let _ = db.get(&key).await.unwrap();
    }
    let read_time = start.elapsed();
    println!(
        "reads:   {:.3}ms ({:.0} ops/sec)",
        read_time.as_secs_f64() * 1000.0,
        count as f64 / read_time.as_secs_f64()
    );

    let start = Instant::now();
    db.close().await.unwrap();
    println!("close:   {:.3}ms", start.elapsed().as_secs_f64() * 1000.0);

    println!("\n=== Compare: sync vs async overhead ===\n");

    let dir2 = tempdir().unwrap();
    let db = Db::open(dir2.path(), Options::default().sync_writes(false))
        .await
        .unwrap();

    let mut latencies = Vec::with_capacity(count);
    for i in 0..count {
        let key = format!("key{:08}", i);
        let value = format!("value{:08}", i);
        let start = Instant::now();
        db.put(key, value).await.unwrap();
        latencies.push(start.elapsed());
    }

    latencies.sort();
    let p50 = latencies[count / 2];
    let p99 = latencies[count * 99 / 100];
    let p999 = latencies[count * 999 / 1000];
    let avg: std::time::Duration = latencies.iter().sum::<std::time::Duration>() / count as u32;

    println!("Per-op latency:");
    println!("  avg:  {:>8.3}µs", avg.as_secs_f64() * 1_000_000.0);
    println!("  p50:  {:>8.3}µs", p50.as_secs_f64() * 1_000_000.0);
    println!("  p99:  {:>8.3}µs", p99.as_secs_f64() * 1_000_000.0);
    println!("  p999: {:>8.3}µs", p999.as_secs_f64() * 1_000_000.0);

    db.close().await.unwrap();
}
