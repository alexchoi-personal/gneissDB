use gneissdb::{Db, Options};
use std::time::Instant;
use tempfile::tempdir;

#[tokio::main]
async fn main() {
    let dir = tempdir().unwrap();
    let db = Db::open(
        dir.path(),
        Options::default().memtable_size(1024).sync_writes(false),
    )
    .await
    .unwrap();

    for i in 0..1000 {
        db.put(format!("key{:08}", i), format!("value{:08}", i))
            .await
            .unwrap();
    }
    db.flush().await.unwrap();

    for i in 1000..1100 {
        db.put(format!("key{:08}", i), format!("value{:08}", i))
            .await
            .unwrap();
    }

    let start = Instant::now();
    for i in 0..1000 {
        let _ = db.get(format!("key{:08}", i)).await.unwrap();
    }
    let elapsed = start.elapsed();
    println!(
        "SSTable reads: {:.3}ms for 1000 = {:.0} ops/sec",
        elapsed.as_secs_f64() * 1000.0,
        1000.0 / elapsed.as_secs_f64()
    );

    db.close().await.unwrap();
}
