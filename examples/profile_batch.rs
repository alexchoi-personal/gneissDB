use bytes::Bytes;
use gneissdb::{Db, Options, WriteBatch};
use tempfile::tempdir;
use std::time::Instant;

#[tokio::main]
async fn main() {
    let batch_count = 1000;
    let ops_per_batch = 100;
    let value: Bytes = Bytes::from("x".repeat(256));

    let dir = tempdir().unwrap();
    let db = Db::open(
        dir.path(),
        Options::default()
            .sync_writes(false)
            .memtable_size(64 * 1024 * 1024),
    )
    .await
    .unwrap();

    let keys: Vec<Vec<Bytes>> = (0..batch_count)
        .map(|batch_id| {
            (0..ops_per_batch)
                .map(|i| Bytes::from(format!("key{:04}_{:06}", batch_id, i)))
                .collect()
        })
        .collect();

    let start = Instant::now();
    for batch_keys in &keys {
        let mut batch = WriteBatch::new();
        for key in batch_keys {
            batch.put(key.clone(), value.clone());
        }
        db.write(batch).await.unwrap();
    }
    let elapsed = start.elapsed();
    
    let total_ops = batch_count * ops_per_batch;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
    
    db.close().await.unwrap();
    println!("Completed {} batches ({} ops) in {:?}", batch_count, total_ops, elapsed);
    println!("{:.0} ops/s", ops_per_sec);
}
