use gneissdb::{Db, Options};
use tempfile::tempdir;

#[tokio::main]
async fn main() {
    let count = 10_000;
    let scan_count = 5_000;
    let scan_size = 100;
    let value = "x".repeat(256);

    let dir = tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default().sync_writes(false))
        .await
        .unwrap();

    for i in 0..count {
        let key = format!("key{:08}", i);
        db.put(key, value.clone()).await.unwrap();
    }

    for i in 0..scan_count {
        let start_key = format!("key{:08}", (i * 7) % (count - scan_size));
        let end_key = format!("key{:08}", (i * 7) % (count - scan_size) + scan_size);
        let results = db.scan(&start_key, &end_key, scan_size);
        std::hint::black_box(results);
    }

    db.close().await.unwrap();
    println!("Completed {} scans", scan_count);
}
