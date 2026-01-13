use crate::cache::BlockCache;
use crate::compaction::CompactionTask;
use crate::error::Result;
use crate::fs::FileSystem;
use crate::manifest::VersionEdit;
use crate::sstable::{SstableBuilder, SstableIterator};
use crate::types::{ParsedInternalKey, ValueType};
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

        if task.input_files.is_empty() {
            return Ok(edit);
        }

        let mut iterators = Vec::with_capacity(task.input_files.len());
        for file in &task.input_files {
            let path = self.sst_path(file.file_number);
            let file_handle = self.fs.open_file(&path).await?;
            let iter =
                SstableIterator::open(file_handle, file.file_number, self.cache.clone()).await?;
            iterators.push(iter);
        }

        let mut heap: BinaryHeap<MergeEntry> = BinaryHeap::new();

        for (idx, iter) in iterators.iter_mut().enumerate() {
            if let Some((key, value)) = iter.next().await? {
                heap.push(MergeEntry {
                    key,
                    value,
                    reader_index: idx,
                });
            }
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

        let mut last_user_key: Option<Bytes> = None;

        while let Some(entry) = heap.pop() {
            let parsed = ParsedInternalKey::parse(&entry.key);

            let should_write = if let Some(parsed) = &parsed {
                match &last_user_key {
                    Some(last) if last.as_ref() == parsed.user_key => false,
                    _ => {
                        if parsed.value_type == ValueType::Value {
                            last_user_key = Some(Bytes::copy_from_slice(parsed.user_key));
                            true
                        } else {
                            last_user_key = Some(Bytes::copy_from_slice(parsed.user_key));
                            false
                        }
                    }
                }
            } else {
                true
            };

            if should_write {
                builder.add_raw(&entry.key, &entry.value).await?;
            }

            if let Some((next_key, next_value)) = iterators[entry.reader_index].next().await? {
                heap.push(MergeEntry {
                    key: next_key,
                    value: next_value,
                    reader_index: entry.reader_index,
                });
            }
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
    key: Bytes,
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

        let compactor = LevelCompactor::new(fs, dir.path().to_path_buf(), cache, 4096, 10);

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
        use crate::types::InternalKey;

        let entry1 = MergeEntry {
            key: InternalKey::new(Bytes::from("aaa"), 10, ValueType::Value).encode(),
            value: Bytes::from("v1"),
            reader_index: 0,
        };

        let entry2 = MergeEntry {
            key: InternalKey::new(Bytes::from("bbb"), 10, ValueType::Value).encode(),
            value: Bytes::from("v2"),
            reader_index: 1,
        };

        assert!(entry1 > entry2);
    }
}
