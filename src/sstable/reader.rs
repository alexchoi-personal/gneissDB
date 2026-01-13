use crate::cache::BlockCache;
use crate::error::Result;
use crate::fs::RandomAccessFile;
use crate::memtable::LookupResult;
use crate::sstable::{Block, BloomFilter, Footer, IndexBlock, FOOTER_SIZE};
use crate::types::{encode_seek_key, ParsedInternalKey, SequenceNumber, ValueType};
use bytes::Bytes;
use std::sync::Arc;

pub(crate) struct SstableReader {
    file: Box<dyn RandomAccessFile>,
    file_number: u64,
    footer: Footer,
    index: IndexBlock,
    bloom: Option<BloomFilter>,
    cache: Arc<BlockCache>,
}

impl SstableReader {
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

        let bloom = if footer.bloom_size > 0 {
            let bloom_data = file
                .read(footer.bloom_offset, footer.bloom_size as usize)
                .await?;
            BloomFilter::from_bytes(bloom_data)
        } else {
            None
        };

        Ok(Self {
            file,
            file_number,
            footer,
            index,
            bloom,
            cache,
        })
    }

    pub(crate) async fn get(
        &self,
        user_key: &[u8],
        sequence: SequenceNumber,
    ) -> Result<LookupResult> {
        if let Some(bloom) = &self.bloom {
            if !bloom.may_contain(user_key) {
                return Ok(LookupResult::NotFound);
            }
        }

        let mut seek_buf = [0u8; 512];
        let seek_len = encode_seek_key(user_key, sequence, &mut seek_buf);
        let encoded_seek = &seek_buf[..seek_len];

        let entry = match self.index.find_block(encoded_seek) {
            Some(e) => e,
            None => return Ok(LookupResult::NotFound),
        };

        let block = self.read_block(entry.offset, entry.size as usize).await?;

        let mut iter = block.seek(encoded_seek);

        while let Some((value_start, value_end)) = iter.advance() {
            let key = iter.current_key();
            if let Some(parsed) = ParsedInternalKey::parse(key) {
                if parsed.user_key != user_key {
                    if parsed.user_key > user_key {
                        break;
                    }
                    continue;
                }
                if parsed.sequence <= sequence {
                    return match parsed.value_type {
                        ValueType::Value => Ok(LookupResult::Found(
                            iter.value_slice(value_start, value_end),
                        )),
                        ValueType::Deletion => Ok(LookupResult::Deleted),
                    };
                }
            }
        }

        Ok(LookupResult::NotFound)
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

    #[allow(dead_code)]
    pub(crate) fn min_key(&self) -> &Bytes {
        &self.footer.min_key
    }

    #[allow(dead_code)]
    pub(crate) fn max_key(&self) -> &Bytes {
        &self.footer.max_key
    }

    #[allow(dead_code)]
    pub(crate) fn file_number(&self) -> u64 {
        self.file_number
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::{FileSystem, RealFileSystem};
    use crate::sstable::SstableBuilder;
    use crate::types::InternalKey;
    use tempfile::tempdir;

    async fn create_test_sstable(
        dir: &std::path::Path,
        entries: Vec<(&str, &str)>,
    ) -> std::path::PathBuf {
        let path = dir.join("test.sst");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();

        let mut builder = SstableBuilder::new(file, path.clone(), 4096, 10, entries.len());

        for (i, (key, value)) in entries.iter().enumerate() {
            let internal_key =
                InternalKey::new(Bytes::from(key.to_string()), i as u64 + 1, ValueType::Value);
            builder
                .add(&internal_key, &Bytes::from(value.to_string()))
                .await
                .unwrap();
        }

        builder.finish().await.unwrap();
        path
    }

    #[tokio::test]
    async fn test_sstable_reader_get() {
        let dir = tempdir().unwrap();
        let path = create_test_sstable(
            dir.path(),
            vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3")],
        )
        .await;

        let fs = RealFileSystem::new();
        let file = fs.open_file(&path).await.unwrap();
        let cache = BlockCache::new(1024 * 1024);

        let reader = SstableReader::open(file, 1, cache).await.unwrap();

        match reader.get(b"key1", 10).await.unwrap() {
            LookupResult::Found(v) => assert_eq!(v.as_ref(), b"value1"),
            _ => panic!("Expected Found"),
        }

        match reader.get(b"key2", 10).await.unwrap() {
            LookupResult::Found(v) => assert_eq!(v.as_ref(), b"value2"),
            _ => panic!("Expected Found"),
        }
    }

    #[tokio::test]
    async fn test_sstable_reader_not_found() {
        let dir = tempdir().unwrap();
        let path = create_test_sstable(dir.path(), vec![("key1", "value1")]).await;

        let fs = RealFileSystem::new();
        let file = fs.open_file(&path).await.unwrap();
        let cache = BlockCache::new(1024 * 1024);

        let reader = SstableReader::open(file, 1, cache).await.unwrap();

        match reader.get(b"nonexistent", 10).await.unwrap() {
            LookupResult::NotFound => {}
            _ => panic!("Expected NotFound"),
        }
    }

    #[tokio::test]
    async fn test_sstable_reader_sequence_visibility() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();

        let mut builder = SstableBuilder::new(file, path.clone(), 4096, 10, 10);

        let key2 = InternalKey::new(Bytes::from("key"), 10, ValueType::Value);
        builder.add(&key2, &Bytes::from("value10")).await.unwrap();

        let key1 = InternalKey::new(Bytes::from("key"), 5, ValueType::Value);
        builder.add(&key1, &Bytes::from("value5")).await.unwrap();

        builder.finish().await.unwrap();

        let file = fs.open_file(&path).await.unwrap();
        let cache = BlockCache::new(1024 * 1024);
        let reader = SstableReader::open(file, 1, cache).await.unwrap();

        match reader.get(b"key", 7).await.unwrap() {
            LookupResult::Found(v) => assert_eq!(v.as_ref(), b"value5"),
            _ => panic!("Expected value5"),
        }

        match reader.get(b"key", 15).await.unwrap() {
            LookupResult::Found(v) => assert_eq!(v.as_ref(), b"value10"),
            _ => panic!("Expected value10"),
        }
    }

    #[tokio::test]
    async fn test_sstable_reader_min_max_keys() {
        let dir = tempdir().unwrap();
        let path = create_test_sstable(
            dir.path(),
            vec![("aaa", "v1"), ("mmm", "v2"), ("zzz", "v3")],
        )
        .await;

        let fs = RealFileSystem::new();
        let file = fs.open_file(&path).await.unwrap();
        let cache = BlockCache::new(1024 * 1024);

        let reader = SstableReader::open(file, 1, cache).await.unwrap();
        assert!(!reader.min_key().is_empty());
        assert!(!reader.max_key().is_empty());
    }

    #[tokio::test]
    async fn test_sstable_reader_file_number() {
        let dir = tempdir().unwrap();
        let path = create_test_sstable(dir.path(), vec![("key", "value")]).await;

        let fs = RealFileSystem::new();
        let file = fs.open_file(&path).await.unwrap();
        let cache = BlockCache::new(1024 * 1024);

        let reader = SstableReader::open(file, 42, cache).await.unwrap();
        assert_eq!(reader.file_number(), 42);
    }

    #[tokio::test]
    async fn test_sstable_reader_many_keys() {
        let dir = tempdir().unwrap();
        let entries: Vec<_> = (0..1000)
            .map(|i| (format!("key{:06}", i), format!("value{:06}", i)))
            .collect();
        let _entries_ref: Vec<_> = entries
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let path = dir.path().join("test.sst");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();

        let mut builder = SstableBuilder::new(file, path.clone(), 4096, 10, entries.len());

        for (i, (key, value)) in entries.iter().enumerate() {
            let internal_key =
                InternalKey::new(Bytes::from(key.clone()), i as u64 + 1, ValueType::Value);
            builder
                .add(&internal_key, &Bytes::from(value.clone()))
                .await
                .unwrap();
        }
        builder.finish().await.unwrap();

        let file = fs.open_file(&path).await.unwrap();
        let cache = BlockCache::new(1024 * 1024);
        let reader = SstableReader::open(file, 1, cache).await.unwrap();

        for i in [0, 100, 500, 999] {
            let key = format!("key{:06}", i);
            let expected_value = format!("value{:06}", i);
            match reader.get(key.as_bytes(), 10000).await.unwrap() {
                LookupResult::Found(v) => assert_eq!(v.as_ref(), expected_value.as_bytes()),
                _ => panic!("Expected Found for {}", key),
            }
        }
    }
}
