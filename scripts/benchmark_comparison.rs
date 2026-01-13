use rocksdb::{DB, Options as RocksOptions};
use gneissdb::{Db, Options};
use std::time::{Duration, Instant};
use tempfile::tempdir;

const ITERATIONS: usize = 100;
const WARMUP_ITERATIONS: usize = 10;

struct BenchResult {
    name: String,
    gneissdb_time: Duration,
    rocksdb_time: Duration,
}

impl BenchResult {
    fn print(&self) {
        let ratio = self.gneissdb_time.as_secs_f64() / self.rocksdb_time.as_secs_f64();
        let faster_slower = if ratio < 1.0 {
            format!("{:.2}x faster", 1.0 / ratio)
        } else {
            format!("{:.2}x slower", ratio)
        };

        println!(
            "{:<28} | {:>10.2}ms | {:>10.2}ms | {}",
            self.name,
            self.gneissdb_time.as_secs_f64() * 1000.0,
            self.rocksdb_time.as_secs_f64() * 1000.0,
            faster_slower
        );
    }
}

fn median(times: &mut [Duration]) -> Duration {
    times.sort();
    let mid = times.len() / 2;
    if times.len() % 2 == 0 {
        (times[mid - 1] + times[mid]) / 2
    } else {
        times[mid]
    }
}

async fn bench_writes(count: usize) -> BenchResult {
    let mut rs_times = Vec::with_capacity(ITERATIONS);
    let mut rocks_times = Vec::with_capacity(ITERATIONS);

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        let dir1 = tempdir().unwrap();
        let dir2 = tempdir().unwrap();

        let start = Instant::now();
        {
            let db = Db::open(dir1.path(), Options::default().sync_writes(false)).await.unwrap();
            for i in 0..count {
                let key = format!("key{:08}", i);
                let value = format!("value{:08}", i);
                db.put(key, value).await.unwrap();
            }
            db.close().await.unwrap();
        }
        let rs_time = start.elapsed();

        let start = Instant::now();
        {
            let mut opts = RocksOptions::default();
            opts.create_if_missing(true);
            let db = DB::open(&opts, dir2.path()).unwrap();
            for i in 0..count {
                let key = format!("key{:08}", i);
                let value = format!("value{:08}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        }
        let rocks_time = start.elapsed();

        if iter >= WARMUP_ITERATIONS {
            rs_times.push(rs_time);
            rocks_times.push(rocks_time);
        }
    }

    BenchResult {
        name: format!("writes_{}", count),
        gneissdb_time: median(&mut rs_times),
        rocksdb_time: median(&mut rocks_times),
    }
}

async fn bench_reads(count: usize) -> BenchResult {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();

    {
        let db = Db::open(dir1.path(), Options::default().sync_writes(false)).await.unwrap();
        for i in 0..count {
            let key = format!("key{:08}", i);
            let value = format!("value{:08}", i);
            db.put(key, value).await.unwrap();
        }
        db.flush().await.unwrap();
        db.close().await.unwrap();
    }
    {
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, dir2.path()).unwrap();
        for i in 0..count {
            let key = format!("key{:08}", i);
            let value = format!("value{:08}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        db.flush().unwrap();
    }

    let mut rs_times = Vec::with_capacity(ITERATIONS);
    let mut rocks_times = Vec::with_capacity(ITERATIONS);

    let db_rs = Db::open(dir1.path(), Options::default()).await.unwrap();
    let db_rocks = DB::open_for_read_only(&RocksOptions::default(), dir2.path(), false).unwrap();

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        let start = Instant::now();
        for i in 0..count {
            let key = format!("key{:08}", i);
            let _ = db_rs.get(&key).await.unwrap();
        }
        let rs_time = start.elapsed();

        let start = Instant::now();
        for i in 0..count {
            let key = format!("key{:08}", i);
            let _ = db_rocks.get(key.as_bytes()).unwrap();
        }
        let rocks_time = start.elapsed();

        if iter >= WARMUP_ITERATIONS {
            rs_times.push(rs_time);
            rocks_times.push(rocks_time);
        }
    }

    drop(db_rocks);
    db_rs.close().await.unwrap();

    BenchResult {
        name: format!("reads_{}", count),
        gneissdb_time: median(&mut rs_times),
        rocksdb_time: median(&mut rocks_times),
    }
}

async fn bench_random_reads(count: usize) -> BenchResult {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();

    {
        let db = Db::open(dir1.path(), Options::default().sync_writes(false)).await.unwrap();
        for i in 0..count {
            let key = format!("key{:08}", i);
            let value = format!("value{:08}", i);
            db.put(key, value).await.unwrap();
        }
        db.flush().await.unwrap();
        db.close().await.unwrap();
    }
    {
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, dir2.path()).unwrap();
        for i in 0..count {
            let key = format!("key{:08}", i);
            let value = format!("value{:08}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        db.flush().unwrap();
    }

    let indices: Vec<usize> = (0..count).map(|i| (i * 7919) % count).collect();

    let mut rs_times = Vec::with_capacity(ITERATIONS);
    let mut rocks_times = Vec::with_capacity(ITERATIONS);

    let db_rs = Db::open(dir1.path(), Options::default()).await.unwrap();
    let db_rocks = DB::open_for_read_only(&RocksOptions::default(), dir2.path(), false).unwrap();

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        let start = Instant::now();
        for &i in &indices {
            let key = format!("key{:08}", i);
            let _ = db_rs.get(&key).await.unwrap();
        }
        let rs_time = start.elapsed();

        let start = Instant::now();
        for &i in &indices {
            let key = format!("key{:08}", i);
            let _ = db_rocks.get(key.as_bytes()).unwrap();
        }
        let rocks_time = start.elapsed();

        if iter >= WARMUP_ITERATIONS {
            rs_times.push(rs_time);
            rocks_times.push(rocks_time);
        }
    }

    drop(db_rocks);
    db_rs.close().await.unwrap();

    BenchResult {
        name: format!("random_reads_{}", count),
        gneissdb_time: median(&mut rs_times),
        rocksdb_time: median(&mut rocks_times),
    }
}

async fn bench_mixed(count: usize) -> BenchResult {
    let mut rs_times = Vec::with_capacity(ITERATIONS);
    let mut rocks_times = Vec::with_capacity(ITERATIONS);

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        let dir1 = tempdir().unwrap();
        let dir2 = tempdir().unwrap();

        let start = Instant::now();
        {
            let db = Db::open(dir1.path(), Options::default().sync_writes(false)).await.unwrap();
            for i in 0..count {
                let key = format!("key{:08}", i);
                let value = format!("value{:08}", i);
                db.put(key, value).await.unwrap();
                if i > 0 && i % 10 == 0 {
                    let read_key = format!("key{:08}", i / 2);
                    let _ = db.get(&read_key).await.unwrap();
                }
            }
            db.close().await.unwrap();
        }
        let rs_time = start.elapsed();

        let start = Instant::now();
        {
            let mut opts = RocksOptions::default();
            opts.create_if_missing(true);
            let db = DB::open(&opts, dir2.path()).unwrap();
            for i in 0..count {
                let key = format!("key{:08}", i);
                let value = format!("value{:08}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
                if i > 0 && i % 10 == 0 {
                    let read_key = format!("key{:08}", i / 2);
                    let _ = db.get(read_key.as_bytes()).unwrap();
                }
            }
        }
        let rocks_time = start.elapsed();

        if iter >= WARMUP_ITERATIONS {
            rs_times.push(rs_time);
            rocks_times.push(rocks_time);
        }
    }

    BenchResult {
        name: format!("mixed_{}", count),
        gneissdb_time: median(&mut rs_times),
        rocksdb_time: median(&mut rocks_times),
    }
}

async fn bench_large_values(count: usize, value_size: usize) -> BenchResult {
    let value = "x".repeat(value_size);
    let mut rs_times = Vec::with_capacity(ITERATIONS);
    let mut rocks_times = Vec::with_capacity(ITERATIONS);

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        let dir1 = tempdir().unwrap();
        let dir2 = tempdir().unwrap();

        let start = Instant::now();
        {
            let db = Db::open(dir1.path(), Options::default().sync_writes(false)).await.unwrap();
            for i in 0..count {
                let key = format!("key{:08}", i);
                db.put(key, value.clone()).await.unwrap();
            }
            db.close().await.unwrap();
        }
        let rs_time = start.elapsed();

        let start = Instant::now();
        {
            let mut opts = RocksOptions::default();
            opts.create_if_missing(true);
            let db = DB::open(&opts, dir2.path()).unwrap();
            for i in 0..count {
                let key = format!("key{:08}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        }
        let rocks_time = start.elapsed();

        if iter >= WARMUP_ITERATIONS {
            rs_times.push(rs_time);
            rocks_times.push(rocks_time);
        }
    }

    BenchResult {
        name: format!("large_{}x{}B", count, value_size),
        gneissdb_time: median(&mut rs_times),
        rocksdb_time: median(&mut rocks_times),
    }
}

#[tokio::main]
async fn main() {
    println!("╔════════════════════════════════════════════════════════════════════════════╗");
    println!("║         RocksDB vs rocksdb-rs Benchmark (median of {} iterations)         ║", ITERATIONS);
    println!("╠════════════════════════════════════════════════════════════════════════════╣");
    println!("║ {:<28} │ {:>10} │ {:>10} │ {:>12} ║", "Benchmark", "rocksdb-rs", "RocksDB", "Comparison");
    println!("╠════════════════════════════════════════════════════════════════════════════╣");

    let results = vec![
        bench_writes(1000).await,
        bench_writes(10000).await,
        bench_reads(1000).await,
        bench_reads(10000).await,
        bench_random_reads(1000).await,
        bench_random_reads(10000).await,
        bench_mixed(1000).await,
        bench_mixed(10000).await,
        bench_large_values(100, 1024).await,
        bench_large_values(100, 4096).await,
    ];

    for result in &results {
        print!("║ ");
        result.print();
        println!(" ║");
    }

    println!("╚════════════════════════════════════════════════════════════════════════════╝");

    println!("\nSummary:");
    let total_rs: Duration = results.iter().map(|r| r.gneissdb_time).sum();
    let total_rocks: Duration = results.iter().map(|r| r.rocksdb_time).sum();
    let avg_ratio = total_rs.as_secs_f64() / total_rocks.as_secs_f64();

    println!(
        "  Total time - rocksdb-rs: {:.2}ms, RocksDB: {:.2}ms",
        total_rs.as_secs_f64() * 1000.0,
        total_rocks.as_secs_f64() * 1000.0
    );

    if avg_ratio > 1.0 {
        println!("  rocksdb-rs is {:.2}x slower on average", avg_ratio);
    } else {
        println!("  rocksdb-rs is {:.2}x faster on average", 1.0 / avg_ratio);
    }
}
