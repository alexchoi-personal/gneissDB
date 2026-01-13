use gneissdb::{Db, Options};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

#[tokio::main]
async fn main() {
    let dir = tempdir().unwrap();
    let count = 10000;

    println!("=== Concurrent Benchmark ===\n");

    let db = Arc::new(
        Db::open(dir.path(), Options::default().sync_writes(false))
            .await
            .unwrap(),
    );

    for threads in [1, 2, 4, 8] {
        let ops_per_thread = count / threads;

        let start = Instant::now();
        let mut handles = Vec::new();

        for t in 0..threads {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..ops_per_thread {
                    let key = format!("key-{}-{:08}", t, i);
                    let value = format!("value-{}-{:08}", t, i);
                    db.put(key, value).await.unwrap();
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let write_time = start.elapsed();
        println!(
            "Write {} threads: {:.3}ms = {:.0} ops/sec",
            threads,
            write_time.as_secs_f64() * 1000.0,
            count as f64 / write_time.as_secs_f64()
        );

        let start = Instant::now();
        let mut handles = Vec::new();

        for t in 0..threads {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..ops_per_thread {
                    let key = format!("key-{}-{:08}", t, i);
                    let _ = db.get(&key).await.unwrap();
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let read_time = start.elapsed();
        println!(
            "Read  {} threads: {:.3}ms = {:.0} ops/sec\n",
            threads,
            read_time.as_secs_f64() * 1000.0,
            count as f64 / read_time.as_secs_f64()
        );
    }

    match Arc::try_unwrap(db) {
        Ok(db) => db.close().await.unwrap(),
        Err(_) => panic!("Failed to unwrap Arc"),
    }
}
