use gneissdb::{Db, Options};
use std::time::Instant;
use tempfile::tempdir;

#[tokio::main]
async fn main() {
    let dir = tempdir().unwrap();
    let count = 10000;

    println!("=== Write Path Profiling ({} ops) ===\n", count);

    let db = Db::open(dir.path(), Options::default().sync_writes(false))
        .await
        .unwrap();

    let mut latencies = Vec::with_capacity(count);

    let start = Instant::now();
    for i in 0..count {
        let key = format!("key{:08}", i);
        let value = format!("value{:08}", i);
        let op_start = Instant::now();
        db.put(key, value).await.unwrap();
        latencies.push(op_start.elapsed());
    }
    let total = start.elapsed();

    latencies.sort();
    let p50 = latencies[count / 2];
    let p99 = latencies[count * 99 / 100];
    let p999 = latencies[count * 999 / 1000];
    let min = latencies[0];
    let max = latencies[count - 1];

    println!(
        "Total: {:.3}ms = {:.0} ops/sec\n",
        total.as_secs_f64() * 1000.0,
        count as f64 / total.as_secs_f64()
    );

    println!("Latency distribution:");
    println!("  min:  {:>10.3}µs", min.as_secs_f64() * 1_000_000.0);
    println!("  p50:  {:>10.3}µs", p50.as_secs_f64() * 1_000_000.0);
    println!("  p99:  {:>10.3}µs", p99.as_secs_f64() * 1_000_000.0);
    println!("  p999: {:>10.3}µs", p999.as_secs_f64() * 1_000_000.0);
    println!("  max:  {:>10.3}µs", max.as_secs_f64() * 1_000_000.0);

    let slow_count = latencies.iter().filter(|l| l.as_micros() > 100).count();
    let very_slow_count = latencies.iter().filter(|l| l.as_micros() > 1000).count();
    println!(
        "\nSlow ops (>100µs): {} ({:.1}%)",
        slow_count,
        slow_count as f64 / count as f64 * 100.0
    );
    println!(
        "Very slow ops (>1ms): {} ({:.1}%)",
        very_slow_count,
        very_slow_count as f64 / count as f64 * 100.0
    );

    db.close().await.unwrap();

    println!("\n=== Comparison with RocksDB ===\n");

    use rocksdb::{Options as RocksOptions, DB};

    let dir2 = tempdir().unwrap();
    let mut opts = RocksOptions::default();
    opts.create_if_missing(true);
    let rocks_db = DB::open(&opts, dir2.path()).unwrap();

    let mut rocks_latencies = Vec::with_capacity(count);

    let start = Instant::now();
    for i in 0..count {
        let key = format!("key{:08}", i);
        let value = format!("value{:08}", i);
        let op_start = Instant::now();
        rocks_db.put(key.as_bytes(), value.as_bytes()).unwrap();
        rocks_latencies.push(op_start.elapsed());
    }
    let rocks_total = start.elapsed();

    rocks_latencies.sort();
    let rocks_p50 = rocks_latencies[count / 2];
    let rocks_p99 = rocks_latencies[count * 99 / 100];

    println!(
        "RocksDB Total: {:.3}ms = {:.0} ops/sec\n",
        rocks_total.as_secs_f64() * 1000.0,
        count as f64 / rocks_total.as_secs_f64()
    );

    println!("RocksDB Latency:");
    println!("  p50:  {:>10.3}µs", rocks_p50.as_secs_f64() * 1_000_000.0);
    println!("  p99:  {:>10.3}µs", rocks_p99.as_secs_f64() * 1_000_000.0);

    println!("\n=== Comparison ===");
    println!(
        "Throughput: rocksdb-rs is {:.2}x slower",
        total.as_secs_f64() / rocks_total.as_secs_f64()
    );
    println!(
        "p50 latency: rocksdb-rs is {:.2}x higher",
        p50.as_secs_f64() / rocks_p50.as_secs_f64()
    );
}
