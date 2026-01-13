use gneissdb::{Db, Options};
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[tokio::main]
async fn main() {
    let dir = tempdir().unwrap();
    let count = 10000;

    println!("=== Setup: Write {} keys ===\n", count);
    {
        let db = Db::open(dir.path(), Options::default().sync_writes(false))
            .await
            .unwrap();
        for i in 0..count {
            let key = format!("key{:08}", i);
            let value = format!("value{:08}", i);
            db.put(key, value).await.unwrap();
        }
        db.flush().await.unwrap();
        db.close().await.unwrap();
    }

    println!("=== Profile db_open (10 iterations) ===\n");
    let mut open_times = Vec::new();
    let mut close_times = Vec::new();

    for _ in 0..10 {
        let start = Instant::now();
        let db = Db::open(dir.path(), Options::default()).await.unwrap();
        open_times.push(start.elapsed());

        let start = Instant::now();
        db.close().await.unwrap();
        close_times.push(start.elapsed());
    }

    let open_total: Duration = open_times.iter().sum();
    let close_total: Duration = close_times.iter().sum();

    println!(
        "db_open:  avg {:.3}ms (total {:.2}ms over {} calls)",
        open_total.as_secs_f64() * 1000.0 / open_times.len() as f64,
        open_total.as_secs_f64() * 1000.0,
        open_times.len()
    );
    println!(
        "db_close: avg {:.3}ms (total {:.2}ms over {} calls)",
        close_total.as_secs_f64() * 1000.0 / close_times.len() as f64,
        close_total.as_secs_f64() * 1000.0,
        close_times.len()
    );

    println!("\n=== Profile get (50000 reads, DB kept open) ===\n");
    let db = Db::open(dir.path(), Options::default()).await.unwrap();

    let start = Instant::now();
    for i in 0..count {
        let key = format!("key{:08}", i);
        let _ = db.get(&key).await.unwrap();
    }
    let read_time = start.elapsed();

    println!(
        "get: {:.3}ms for {} reads = {:.0} ops/sec",
        read_time.as_secs_f64() * 1000.0,
        count,
        count as f64 / read_time.as_secs_f64()
    );

    db.close().await.unwrap();

    println!("\n=== Comparison with benchmark methodology ===");
    println!("(includes db_open + reads + db_close per iteration)\n");

    let mut total_times = Vec::new();
    for _ in 0..10 {
        let start = Instant::now();
        let db = Db::open(dir.path(), Options::default()).await.unwrap();
        for i in 0..count {
            let key = format!("key{:08}", i);
            let _ = db.get(&key).await.unwrap();
        }
        db.close().await.unwrap();
        total_times.push(start.elapsed());
    }

    let total: Duration = total_times.iter().sum();
    println!(
        "open+read+close: avg {:.3}ms = {:.0} effective ops/sec",
        total.as_secs_f64() * 1000.0 / total_times.len() as f64,
        (count * total_times.len()) as f64 / total.as_secs_f64()
    );
}
