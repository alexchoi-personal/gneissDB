mod cache;
mod compaction;
mod db;
mod error;
mod fs;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
mod fs_iouring;
mod group_commit;
mod manifest;
mod memtable;
mod options;
mod sstable;
mod table_cache;
mod types;
mod wal;

pub use error::{Error, Result};
pub use options::{IoEngine, Options, ReadOptions, WriteOptions};

use bytes::Bytes;
use std::path::Path;

pub struct Db {
    inner: db::DbInner,
    default_sync: bool,
}

impl Db {
    pub async fn open<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        tracing::info!(path = %path.as_ref().display(), "Opening database");
        let default_sync = options.sync_writes;
        let inner = db::DbInner::open(path.as_ref(), options).await?;
        Ok(Self {
            inner,
            default_sync,
        })
    }

    pub async fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        self.put_opt(
            key,
            value,
            WriteOptions {
                sync: self.default_sync,
            },
        )
        .await
    }

    pub async fn put_opt(
        &self,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
        options: WriteOptions,
    ) -> Result<()> {
        let key = key.into();
        let value = value.into();
        tracing::debug!(
            key_len = key.len(),
            value_len = value.len(),
            "Put operation"
        );
        self.inner.put(key, value, options).await
    }

    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        self.get_opt(key, ReadOptions::default()).await
    }

    pub async fn get_opt(
        &self,
        key: impl AsRef<[u8]>,
        options: ReadOptions,
    ) -> Result<Option<Bytes>> {
        tracing::debug!(key_len = key.as_ref().len(), "Get operation");
        self.inner.get(key.as_ref(), options).await
    }

    pub async fn delete(&self, key: impl AsRef<[u8]>) -> Result<()> {
        self.delete_opt(
            key,
            WriteOptions {
                sync: self.default_sync,
            },
        )
        .await
    }

    pub async fn delete_opt(&self, key: impl AsRef<[u8]>, options: WriteOptions) -> Result<()> {
        tracing::debug!(key_len = key.as_ref().len(), "Delete operation");
        self.inner.delete(key.as_ref(), options).await
    }

    pub async fn write(&self, batch: WriteBatch) -> Result<()> {
        self.write_opt(
            batch,
            WriteOptions {
                sync: self.default_sync,
            },
        )
        .await
    }

    pub async fn write_opt(&self, batch: WriteBatch, options: WriteOptions) -> Result<()> {
        tracing::debug!(op_count = batch.len(), "Batch write operation");
        self.inner.write_batch_buffer(batch, options).await
    }

    pub fn scan(
        &self,
        start: impl AsRef<[u8]>,
        end: impl AsRef<[u8]>,
        limit: usize,
    ) -> Vec<(Bytes, Bytes)> {
        self.inner.scan(start.as_ref(), end.as_ref(), limit)
    }

    pub async fn flush(&self) -> Result<()> {
        tracing::info!("Manual flush requested");
        self.inner.flush().await
    }

    pub async fn compact(&self) -> Result<()> {
        tracing::info!("Manual compaction requested");
        self.inner.compact().await
    }

    pub async fn close(self) -> Result<()> {
        tracing::info!("Closing database");
        self.inner.close().await
    }
}

pub(crate) const BATCH_TYPE_PUT: u8 = 1;
pub(crate) const BATCH_TYPE_DELETE: u8 = 2;

pub struct WriteBatch {
    rep: bytes::BytesMut,
    count: u32,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self {
            rep: bytes::BytesMut::with_capacity(4096),
            count: 0,
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            rep: bytes::BytesMut::with_capacity(cap),
            count: 0,
        }
    }

    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        use bytes::BufMut;
        let key = key.as_ref();
        let value = value.as_ref();
        self.rep.put_u8(BATCH_TYPE_PUT);
        self.rep.put_u32_le(key.len() as u32);
        self.rep.extend_from_slice(key);
        self.rep.put_u32_le(value.len() as u32);
        self.rep.extend_from_slice(value);
        self.count += 1;
    }

    pub fn delete(&mut self, key: impl AsRef<[u8]>) {
        use bytes::BufMut;
        let key = key.as_ref();
        self.rep.put_u8(BATCH_TYPE_DELETE);
        self.rep.put_u32_le(key.len() as u32);
        self.rep.extend_from_slice(key);
        self.count += 1;
    }

    pub fn len(&self) -> usize {
        self.count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub(crate) fn into_bytes(self) -> Bytes {
        self.rep.freeze()
    }

    pub(crate) fn count(&self) -> u32 {
        self.count
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_db_basic_operations() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), Options::default()).await.unwrap();

        db.put("key1", "value1").await.unwrap();
        db.put("key2", "value2").await.unwrap();

        let v1 = db.get("key1").await.unwrap().unwrap();
        assert_eq!(&v1[..], b"value1");

        db.delete("key1").await.unwrap();
        assert!(db.get("key1").await.unwrap().is_none());

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_write_batch() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), Options::default()).await.unwrap();

        let mut batch = WriteBatch::new();
        batch.put("k1", "v1");
        batch.put("k2", "v2");
        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());

        db.write(batch).await.unwrap();

        assert!(db.get("k1").await.unwrap().is_some());
        assert!(db.get("k2").await.unwrap().is_some());

        db.close().await.unwrap();
    }

    #[test]
    fn test_write_batch_default() {
        let batch = WriteBatch::default();
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn test_db_with_options() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), Options::default()).await.unwrap();

        db.put_opt("key1", "value1", WriteOptions { sync: true })
            .await
            .unwrap();

        let v = db
            .get_opt("key1", ReadOptions::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&v[..], b"value1");

        db.delete_opt("key1", WriteOptions::default())
            .await
            .unwrap();

        assert!(db.get("key1").await.unwrap().is_none());

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_batch_with_options() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), Options::default()).await.unwrap();

        let mut batch = WriteBatch::new();
        batch.put("k1", "v1");
        batch.delete("k1");

        db.write_opt(batch, WriteOptions { sync: true })
            .await
            .unwrap();

        assert!(db.get("k1").await.unwrap().is_none());

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_flush_and_compact() {
        let dir = tempdir().unwrap();
        let options = Options::default().memtable_size(256);
        let db = Db::open(dir.path(), options).await.unwrap();

        for i in 0..50 {
            db.put(format!("key{:04}", i), "x".repeat(10))
                .await
                .unwrap();
        }

        db.flush().await.unwrap();
        db.compact().await.unwrap();

        for i in 0..50 {
            let v = db.get(format!("key{:04}", i)).await.unwrap();
            assert!(v.is_some());
        }

        db.close().await.unwrap();
    }
}
