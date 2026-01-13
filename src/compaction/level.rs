use crate::compaction::CompactionTask;
use crate::error::Result;
use crate::fs::FileSystem;
use crate::manifest::VersionEdit;
use crate::sstable::{SstableBuilder, SstableReader};
use crate::cache::BlockCache;
use crate::types::InternalKey;
use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

pub(crate) struct LevelCompactor {
    fs: Arc<dyn FileSystem + Send + Sync>,
    db_path: std::path::PathBuf,
    cache: Arc<BlockCache>,
    block_size: usize,
    bloom_bits_per_key: usize,
}

impl LevelCompactor {
    pub(crate) fn new(
        fs: Arc<dyn FileSystem + Send + Sync>,
        db_path: std::path::PathBuf,
        cache: Arc<BlockCache>,
        block_size: usize,
        bloom_bits_per_key: usize,
    ) -> Self {
        Self {
            fs,
            db_path,
            cache,
            block_size,
            bloom_bits_per_key,
        }
    }

    pub(crate) async fn compact(
        &self,
        task: &CompactionTask,
        next_file_number: u64,
    ) -> Result<VersionEdit> {
        let mut edit = VersionEdit::new();

        let mut readers = Vec::new();
        for file in &task.input_files {
            let path = self.sst_path(file.file_number);
            let file_handle = self.fs.open_file(&path).await?;
            let reader = SstableReader::open(file_handle, file.file_number, self.cache.clone()).await?;
            readers.push(reader);
        }

        let output_path = self.sst_path(next_file_number);
        let output_file = self.fs.create_file(&output_path).await?;
        let mut builder = SstableBuilder::new(
            output_file,
            output_path,
            self.block_size,
            self.bloom_bits_per_key,
            1000,
        );

        let mut heap: BinaryHeap<MergeEntry> = BinaryHeap::new();
        let mut iterators: Vec<_> = Vec::new();

        for (idx, _reader) in readers.iter().enumerate() {
            iterators.push(idx);
        }

        let metadata = builder.finish().await?;

        if metadata.entry_count > 0 {
            edit.add_file(
                task.output_level,
                next_file_number,
                metadata.file_size,
                metadata.min_key,
                metadata.max_key,
            );
        }

        for file in &task.input_files {
            edit.delete_file(task.level, file.file_number);
        }

        Ok(edit)
    }

    fn sst_path(&self, file_number: u64) -> std::path::PathBuf {
        self.db_path.join(format!("{:06}.sst", file_number))
    }
}

struct MergeEntry {
    key: InternalKey,
    value: Bytes,
    reader_index: usize,
}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for MergeEntry {}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.key.cmp(&self.key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::RealFileSystem;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_level_compactor_empty() {
        let dir = tempdir().unwrap();
        let fs: Arc<dyn FileSystem + Send + Sync> = Arc::new(RealFileSystem::new());
        let cache = BlockCache::new(1024 * 1024);

        let compactor = LevelCompactor::new(
            fs,
            dir.path().to_path_buf(),
            cache,
            4096,
            10,
        );

        let task = CompactionTask {
            level: 0,
            input_files: vec![],
            output_level: 1,
        };

        let edit = compactor.compact(&task, 1).await.unwrap();
        assert!(edit.deleted_files.is_empty());
    }

    #[test]
    fn test_merge_entry_ordering() {
        use crate::types::ValueType;

        let entry1 = MergeEntry {
            key: InternalKey::new(Bytes::from("aaa"), 10, ValueType::Value),
            value: Bytes::from("v1"),
            reader_index: 0,
        };

        let entry2 = MergeEntry {
            key: InternalKey::new(Bytes::from("bbb"), 10, ValueType::Value),
            value: Bytes::from("v2"),
            reader_index: 1,
        };

        assert!(entry1 > entry2);
    }
}
