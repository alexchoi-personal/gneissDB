use gneissdb::{Db, Options};
use tempfile::tempdir;

#[tokio::main]
async fn main() {
    let dir = tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default().sync_writes(false))
        .await
        .unwrap();

    for i in 0..50000 {
        let key = format!("key{:08}", i);
        let value = format!("value{:08}", i);
        db.put(key, value).await.unwrap();
    }

    db.close().await.unwrap();
}
