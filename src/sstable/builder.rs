use crate::error::Result;
use crate::fs::WritableFile;
use crate::sstable::{BlockBuilder, BloomFilter, Footer, IndexBlock, FOOTER_SIZE};
use crate::types::InternalKey;
use bytes::Bytes;
use std::path::PathBuf;

pub(crate) struct SstableBuilder {
    file: Box<dyn WritableFile>,
    path: PathBuf,
    block_builder: BlockBuilder,
    index: IndexBlock,
    bloom: BloomFilter,
    block_size: usize,
    current_offset: u64,
    first_key: Option<Bytes>,
    last_key: Option<Bytes>,
    entry_count: usize,
}

pub(crate) struct SstableMetadata {
    pub(crate) path: PathBuf,
    pub(crate) file_size: u64,
    pub(crate) entry_count: usize,
    pub(crate) min_key: Bytes,
    pub(crate) max_key: Bytes,
}

impl SstableBuilder {
    pub(crate) fn new(
        file: Box<dyn WritableFile>,
        path: PathBuf,
        block_size: usize,
        bloom_bits_per_key: usize,
        estimated_entries: usize,
    ) -> Self {
        Self {
            file,
            path,
            block_builder: BlockBuilder::new(16),
            index: IndexBlock::new(),
            bloom: BloomFilter::new(bloom_bits_per_key, estimated_entries),
            block_size,
            current_offset: 0,
            first_key: None,
            last_key: None,
            entry_count: 0,
        }
    }

    pub(crate) async fn add(&mut self, key: &InternalKey, value: &Bytes) -> Result<()> {
        let encoded_key = key.encode();

        if self.first_key.is_none() {
            self.first_key = Some(encoded_key.clone());
        }
        self.last_key = Some(encoded_key.clone());

        self.bloom.add(&key.user_key);
        self.block_builder.add(&encoded_key, value);
        self.entry_count += 1;

        if self.block_builder.estimated_size() >= self.block_size {
            self.flush_block().await?;
        }

        Ok(())
    }

    async fn flush_block(&mut self) -> Result<()> {
        if self.block_builder.is_empty() {
            return Ok(());
        }

        let last_key = Bytes::copy_from_slice(self.block_builder.last_key());
        let block_data = std::mem::replace(
            &mut self.block_builder,
            BlockBuilder::new(16),
        )
        .finish();

        let block_size = block_data.len() as u32;
        self.file.append(&block_data).await?;

        self.index
            .add(last_key, self.current_offset, block_size);
        self.current_offset += block_size as u64;

        Ok(())
    }

    pub(crate) async fn finish(mut self) -> Result<SstableMetadata> {
        self.flush_block().await?;

        let bloom_offset = self.current_offset;
        let bloom_data = self.bloom.encode();
        let bloom_size = bloom_data.len() as u32;
        self.file.append(&bloom_data).await?;
        self.current_offset += bloom_size as u64;

        let index_offset = self.current_offset;
        let index_data = self.index.encode();
        let index_size = index_data.len() as u32;
        self.file.append(&index_data).await?;
        self.current_offset += index_size as u64;

        let footer = Footer {
            index_offset,
            index_size,
            bloom_offset,
            bloom_size,
            min_key: self.first_key.clone().unwrap_or_default(),
            max_key: self.last_key.clone().unwrap_or_default(),
        };
        let footer_data = footer.encode();
        self.file.append(&footer_data).await?;
        self.current_offset += FOOTER_SIZE as u64;

        self.file.sync().await?;
        self.file.close().await?;

        Ok(SstableMetadata {
            path: self.path,
            file_size: self.current_offset,
            entry_count: self.entry_count,
            min_key: self.first_key.unwrap_or_default(),
            max_key: self.last_key.unwrap_or_default(),
        })
    }

    pub(crate) fn estimated_size(&self) -> u64 {
        self.current_offset + self.block_builder.estimated_size() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::{FileSystem, RealFileSystem};
    use crate::types::ValueType;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_sstable_builder_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();

        let mut builder = SstableBuilder::new(file, path.clone(), 4096, 10, 100);

        for i in 0..10 {
            let key = InternalKey::new(
                Bytes::from(format!("key{:04}", i)),
                i as u64,
                ValueType::Value,
            );
            let value = Bytes::from(format!("value{}", i));
            builder.add(&key, &value).await.unwrap();
        }

        let metadata = builder.finish().await.unwrap();
        assert_eq!(metadata.entry_count, 10);
        assert!(metadata.file_size > 0);
    }

    #[tokio::test]
    async fn test_sstable_builder_multiple_blocks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();

        let mut builder = SstableBuilder::new(file, path.clone(), 100, 10, 1000);

        for i in 0..100 {
            let key = InternalKey::new(
                Bytes::from(format!("key{:04}", i)),
                i as u64,
                ValueType::Value,
            );
            let value = Bytes::from("x".repeat(50));
            builder.add(&key, &value).await.unwrap();
        }

        let metadata = builder.finish().await.unwrap();
        assert_eq!(metadata.entry_count, 100);
    }

    #[tokio::test]
    async fn test_sstable_builder_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();

        let builder = SstableBuilder::new(file, path.clone(), 4096, 10, 100);
        let metadata = builder.finish().await.unwrap();
        assert_eq!(metadata.entry_count, 0);
    }

    #[tokio::test]
    async fn test_sstable_builder_estimated_size() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();

        let mut builder = SstableBuilder::new(file, path.clone(), 4096, 10, 100);
        let initial_size = builder.estimated_size();

        let key = InternalKey::new(Bytes::from("key"), 1, ValueType::Value);
        builder.add(&key, &Bytes::from("value")).await.unwrap();
        assert!(builder.estimated_size() > initial_size);
    }
}
