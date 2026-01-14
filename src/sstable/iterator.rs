use crate::cache::BlockCache;
use crate::error::Result;
use crate::fs::RandomAccessFile;
use crate::sstable::{
    Block, BlockIterator, Footer, IndexBlock, IndexEntry, SstableHandle, FOOTER_SIZE,
};
use bytes::Bytes;
use std::sync::Arc;

pub(crate) struct SstableIterator {
    handle: Option<Arc<SstableHandle>>,
    file: Option<Box<dyn RandomAccessFile>>,
    cache: Arc<BlockCache>,
    file_number: u64,
    index_entries: Vec<IndexEntry>,
    current_block_idx: usize,
    current_block: Option<Block>,
    block_iter: Option<BlockIterator>,
}

impl SstableIterator {
    pub(crate) async fn open(
        file: Box<dyn RandomAccessFile>,
        file_number: u64,
        cache: Arc<BlockCache>,
    ) -> Result<Self> {
        let file_size = file.size().await?;
        let footer_data = file
            .read(file_size - FOOTER_SIZE as u64, FOOTER_SIZE)
            .await?;
        let footer = Footer::decode(&footer_data)?;

        let index_data = file
            .read(footer.index_offset, footer.index_size as usize)
            .await?;
        let index = IndexBlock::decode(index_data)?;

        Ok(Self {
            handle: None,
            file: Some(file),
            cache,
            file_number,
            index_entries: index.into_entries(),
            current_block_idx: 0,
            current_block: None,
            block_iter: None,
        })
    }

    pub(crate) fn from_handle(handle: Arc<SstableHandle>) -> Self {
        Self {
            file_number: handle.file_number,
            index_entries: handle.index_entries.clone(),
            cache: BlockCache::new(0),
            handle: Some(handle),
            file: None,
            current_block_idx: 0,
            current_block: None,
            block_iter: None,
        }
    }

    pub(crate) async fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.current_block_idx = 0;
        self.current_block = None;
        self.block_iter = None;

        for (idx, entry) in self.index_entries.iter().enumerate() {
            if entry.last_key.as_ref() >= target {
                self.current_block_idx = idx;
                break;
            }
        }

        if self.current_block_idx < self.index_entries.len() {
            self.load_current_block().await?;

            if let Some(ref block) = self.current_block {
                self.block_iter = Some(block.seek(target));
            }
        }

        Ok(())
    }

    async fn load_current_block(&mut self) -> Result<bool> {
        if self.current_block_idx >= self.index_entries.len() {
            self.current_block = None;
            self.block_iter = None;
            return Ok(false);
        }

        let entry = &self.index_entries[self.current_block_idx];
        let block = self.read_block(entry.offset, entry.size as usize).await?;

        self.block_iter = Some(block.iter());
        self.current_block = Some(block);

        Ok(true)
    }

    async fn read_block(&self, offset: u64, size: usize) -> Result<Block> {
        if let Some(ref handle) = self.handle {
            return handle.read_block(offset, size).await;
        }

        if let Some(data) = self.cache.get(self.file_number, offset) {
            return Ok(Block::new_unchecked(data));
        }

        if let Some(ref file) = self.file {
            let data = file.read(offset, size).await?;
            let block = Block::new(data.clone())?;
            self.cache.insert(self.file_number, offset, data);
            return Ok(block);
        }

        Err(crate::error::Error::Corruption("No file or handle".into()))
    }

    pub(crate) async fn next(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        loop {
            if self.block_iter.is_none() && !self.load_current_block().await? {
                return Ok(None);
            }

            if let Some(ref mut iter) = self.block_iter {
                if let Some(entry) = iter.next() {
                    return Ok(Some(entry));
                }
            }

            self.current_block_idx += 1;
            self.current_block = None;
            self.block_iter = None;
        }
    }

    #[allow(dead_code)]
    pub(crate) fn is_valid(&self) -> bool {
        self.current_block_idx < self.index_entries.len() || self.block_iter.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::{FileSystem, RealFileSystem};
    use crate::sstable::SstableBuilder;
    use crate::types::{InternalKey, ValueType};
    use tempfile::tempdir;

    async fn create_test_sstable(dir: &std::path::Path, count: usize) -> std::path::PathBuf {
        let path = dir.join("test.sst");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();

        let mut builder = SstableBuilder::new(file, path.clone(), 256, 10, count);

        for i in 0..count {
            let key = format!("key{:06}", i);
            let value = format!("value{:06}", i);
            let internal_key = InternalKey::new(Bytes::from(key), i as u64 + 1, ValueType::Value);
            builder
                .add(&internal_key, &Bytes::from(value))
                .await
                .unwrap();
        }

        builder.finish().await.unwrap();
        path
    }

    #[tokio::test]
    async fn test_sstable_iterator_basic() {
        let dir = tempdir().unwrap();
        let path = create_test_sstable(dir.path(), 10).await;

        let fs = RealFileSystem::new();
        let file = fs.open_file(&path).await.unwrap();
        let cache = BlockCache::new(1024 * 1024);

        let mut iter = SstableIterator::open(file, 1, cache).await.unwrap();

        let mut count = 0;
        while let Some((key, _value)) = iter.next().await.unwrap() {
            let expected_key = format!("key{:06}", count);
            let parsed_key = &key[..key.len() - 9];
            assert_eq!(parsed_key, expected_key.as_bytes());
            count += 1;
        }
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn test_sstable_iterator_seek() {
        let dir = tempdir().unwrap();
        let path = create_test_sstable(dir.path(), 100).await;

        let fs = RealFileSystem::new();
        let file = fs.open_file(&path).await.unwrap();
        let cache = BlockCache::new(1024 * 1024);

        let mut iter = SstableIterator::open(file, 1, cache).await.unwrap();

        let seek_key = InternalKey::new(Bytes::from("key000050"), u64::MAX, ValueType::Value);
        iter.seek(&seek_key.encode()).await.unwrap();

        let (key, _) = iter.next().await.unwrap().unwrap();
        let parsed_key = &key[..key.len() - 9];
        assert!(parsed_key >= b"key000050".as_slice());
    }

    #[tokio::test]
    async fn test_sstable_iterator_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.sst");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();

        let builder = SstableBuilder::new(file, path.clone(), 4096, 10, 0);
        builder.finish().await.unwrap();

        let file = fs.open_file(&path).await.unwrap();
        let cache = BlockCache::new(1024 * 1024);

        let mut iter = SstableIterator::open(file, 1, cache).await.unwrap();
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_sstable_iterator_many_blocks() {
        let dir = tempdir().unwrap();
        let path = create_test_sstable(dir.path(), 1000).await;

        let fs = RealFileSystem::new();
        let file = fs.open_file(&path).await.unwrap();
        let cache = BlockCache::new(1024 * 1024);

        let mut iter = SstableIterator::open(file, 1, cache).await.unwrap();

        let mut count = 0;
        while iter.next().await.unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 1000);
    }
}
