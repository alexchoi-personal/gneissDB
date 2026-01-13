use crate::cache::BlockCache;
use crate::error::Result;
use crate::fs::RandomAccessFile;
use crate::sstable::{Block, Footer, IndexBlock, IndexEntry, FOOTER_SIZE};
use bytes::Bytes;
use std::sync::Arc;

#[allow(dead_code)]
pub(crate) struct SstableIterator {
    file: Box<dyn RandomAccessFile>,
    cache: Arc<BlockCache>,
    file_number: u64,
    index_entries: Vec<IndexEntry>,
    current_block_idx: usize,
    current_block: Option<BlockState>,
}

#[allow(dead_code)]
struct BlockState {
    iter: std::vec::IntoIter<(Bytes, Bytes)>,
}

#[allow(dead_code)]
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
            file,
            cache,
            file_number,
            index_entries: index.into_entries(),
            current_block_idx: 0,
            current_block: None,
        })
    }

    pub(crate) async fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.current_block_idx = 0;
        self.current_block = None;

        for (idx, entry) in self.index_entries.iter().enumerate() {
            if entry.last_key.as_ref() >= target {
                self.current_block_idx = idx;
                break;
            }
        }

        if self.current_block_idx < self.index_entries.len() {
            self.load_current_block().await?;

            if let Some(ref mut state) = self.current_block {
                let entries: Vec<_> = std::mem::take(&mut state.iter).collect();
                let mut filtered = Vec::new();
                let mut found_start = false;

                for (k, v) in entries {
                    if !found_start && k.as_ref() >= target {
                        found_start = true;
                    }
                    if found_start {
                        filtered.push((k, v));
                    }
                }
                state.iter = filtered.into_iter();
            }
        }

        Ok(())
    }

    async fn load_current_block(&mut self) -> Result<bool> {
        if self.current_block_idx >= self.index_entries.len() {
            self.current_block = None;
            return Ok(false);
        }

        let entry = &self.index_entries[self.current_block_idx];
        let block = self.read_block(entry.offset, entry.size as usize).await?;

        let entries: Vec<_> = block.iter().collect();
        self.current_block = Some(BlockState {
            iter: entries.into_iter(),
        });

        Ok(true)
    }

    async fn read_block(&self, offset: u64, size: usize) -> Result<Block> {
        if let Some(data) = self.cache.get(self.file_number, offset) {
            return Ok(Block::new_unchecked(data));
        }

        let data = self.file.read(offset, size).await?;
        let block = Block::new(data.clone())?;
        self.cache.insert(self.file_number, offset, data);
        Ok(block)
    }

    pub(crate) async fn next(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        loop {
            if self.current_block.is_none() && !self.load_current_block().await? {
                return Ok(None);
            }

            if let Some(ref mut state) = self.current_block {
                if let Some(entry) = state.iter.next() {
                    return Ok(Some(entry));
                }
            }

            self.current_block_idx += 1;
            self.current_block = None;
        }
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.current_block_idx < self.index_entries.len() || self.current_block.is_some()
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
