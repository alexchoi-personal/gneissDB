use crate::cache::BlockCache;
use crate::error::Result;
use crate::fs::FileSystem;
use crate::sstable::SstableReader;
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;

pub(crate) struct TableCache {
    fs: Arc<dyn FileSystem + Send + Sync>,
    db_path: PathBuf,
    block_cache: Arc<BlockCache>,
    cache: DashMap<u64, Arc<SstableReader>>,
    max_open_files: usize,
}

impl TableCache {
    pub(crate) fn new(
        fs: Arc<dyn FileSystem + Send + Sync>,
        db_path: PathBuf,
        block_cache: Arc<BlockCache>,
        max_open_files: usize,
    ) -> Self {
        Self {
            fs,
            db_path,
            block_cache,
            cache: DashMap::with_capacity(max_open_files),
            max_open_files,
        }
    }

    pub(crate) async fn get(&self, file_number: u64) -> Result<Arc<SstableReader>> {
        if let Some(reader) = self.cache.get(&file_number) {
            return Ok(reader.clone());
        }

        let path = self.db_path.join(format!("{:06}.sst", file_number));
        let file_handle = self.fs.open_file(&path).await?;
        let reader = SstableReader::open(file_handle, file_number, self.block_cache.clone()).await?;
        let reader = Arc::new(reader);

        if self.cache.len() >= self.max_open_files {
            if let Some(entry) = self.cache.iter().next() {
                let key = *entry.key();
                drop(entry);
                self.cache.remove(&key);
            }
        }

        self.cache.insert(file_number, reader.clone());

        Ok(reader)
    }

    pub(crate) fn evict(&self, file_number: u64) {
        self.cache.remove(&file_number);
    }

    pub(crate) fn clear(&self) {
        self.cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::RealFileSystem;
    use crate::sstable::SstableBuilder;
    use crate::types::{InternalKey, ValueType};
    use bytes::Bytes;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_table_cache_get() {
        let dir = tempdir().unwrap();
        let fs: Arc<dyn FileSystem + Send + Sync> = Arc::new(RealFileSystem::new());
        let block_cache = BlockCache::new(1024 * 1024);

        let path = dir.path().join("000001.sst");
        let file = fs.create_file(&path).await.unwrap();
        let mut builder = SstableBuilder::new(file, path.clone(), 4096, 10, 10);
        let key = InternalKey::new(Bytes::from("key"), 1, ValueType::Value);
        builder.add(&key, &Bytes::from("value")).await.unwrap();
        builder.finish().await.unwrap();

        let table_cache = TableCache::new(fs, dir.path().to_path_buf(), block_cache, 100);
        
        let reader1 = table_cache.get(1).await.unwrap();
        let reader2 = table_cache.get(1).await.unwrap();
        
        assert!(Arc::ptr_eq(&reader1, &reader2));
    }

    #[tokio::test]
    async fn test_table_cache_evict() {
        let dir = tempdir().unwrap();
        let fs: Arc<dyn FileSystem + Send + Sync> = Arc::new(RealFileSystem::new());
        let block_cache = BlockCache::new(1024 * 1024);

        let path = dir.path().join("000001.sst");
        let file = fs.create_file(&path).await.unwrap();
        let mut builder = SstableBuilder::new(file, path.clone(), 4096, 10, 10);
        let key = InternalKey::new(Bytes::from("key"), 1, ValueType::Value);
        builder.add(&key, &Bytes::from("value")).await.unwrap();
        builder.finish().await.unwrap();

        let table_cache = TableCache::new(fs, dir.path().to_path_buf(), block_cache, 100);
        
        let _reader = table_cache.get(1).await.unwrap();
        table_cache.evict(1);
        
        let reader2 = table_cache.get(1).await.unwrap();
        assert_eq!(Arc::strong_count(&reader2), 2);
    }

    #[tokio::test]
    async fn test_table_cache_concurrent_access() {
        let dir = tempdir().unwrap();
        let fs: Arc<dyn FileSystem + Send + Sync> = Arc::new(RealFileSystem::new());
        let block_cache = BlockCache::new(1024 * 1024);

        let path = dir.path().join("000001.sst");
        let file = fs.create_file(&path).await.unwrap();
        let mut builder = SstableBuilder::new(file, path.clone(), 4096, 10, 10);
        let key = InternalKey::new(Bytes::from("key"), 1, ValueType::Value);
        builder.add(&key, &Bytes::from("value")).await.unwrap();
        builder.finish().await.unwrap();

        let table_cache = Arc::new(TableCache::new(fs, dir.path().to_path_buf(), block_cache, 100));
        
        let mut handles = Vec::new();
        for _ in 0..8 {
            let cache = table_cache.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..100 {
                    let _reader = cache.get(1).await.unwrap();
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }
}
