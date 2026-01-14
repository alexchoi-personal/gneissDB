use crate::cache::BlockCache;
use crate::compaction::{CompactionPicker, LevelCompactor};
use crate::error::{Error, Result};
use crate::fs::{FileSystem, RealFileSystem};
#[cfg(all(target_os = "linux", feature = "io_uring"))]
use crate::fs_iouring::IoUringFileSystem;
use crate::group_commit::{GroupCommitQueue, WriteOp};
use crate::manifest::{write_current, ManifestWriter, VersionEdit, VersionSet};
use crate::memtable::{ImmutableMemtable, LookupResult, Memtable};
use crate::options::{IoEngine, Options, ReadOptions, WriteOptions};
use crate::sstable::{SstableBuilder, SstableHandleCache};
use crate::table_cache::TableCache;
use crate::wal::{BatchOp, WalReader, WalRecord, WalWriter};
use crate::{WriteBatch, BATCH_TYPE_PUT};
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) struct DbInner {
    path: PathBuf,
    options: Options,
    fs: Arc<dyn FileSystem + Send + Sync>,
    memtable: Arc<RwLock<Arc<Memtable>>>,
    immutable_memtables: Arc<Mutex<VecDeque<ImmutableMemtable>>>,
    version_set: Arc<RwLock<VersionSet>>,
    manifest: Arc<tokio::sync::Mutex<ManifestWriter>>,
    wal: Arc<tokio::sync::Mutex<Option<WalWriter>>>,
    group_commit: Option<GroupCommitQueue>,
    cache: Arc<BlockCache>,
    table_cache: Arc<TableCache>,
    handle_cache: Arc<SstableHandleCache>,
    #[allow(dead_code)]
    write_lock: Mutex<()>,
    compaction_picker: CompactionPicker,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) enum DbBatchOp {
    Put(Bytes, Bytes),
    Delete(Bytes),
}

impl DbInner {
    pub(crate) async fn open(path: &Path, options: Options) -> Result<Self> {
        let fs: Arc<dyn FileSystem + Send + Sync> = match options.io_engine {
            IoEngine::Standard => Arc::new(RealFileSystem::new()),
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            IoEngine::IoUring => Arc::new(IoUringFileSystem::new()?),
        };

        if !fs.exists(path).await {
            fs.create_dir_all(path).await?;
        }

        let cache = BlockCache::new(options.block_cache_size);

        let (version_set, existing_manifest) =
            VersionSet::recover(path, options.max_levels).await?;

        let manifest_writer = if let Some(manifest_name) = existing_manifest {
            let manifest_path = path.join(&manifest_name);
            ManifestWriter::open_append(&manifest_path).await?
        } else {
            let manifest_number = version_set.peek_file_number();
            let manifest_name = format!("MANIFEST-{:06}", manifest_number);
            let manifest_path = path.join(&manifest_name);
            let mut writer = ManifestWriter::create(&manifest_path).await?;

            let snapshot = version_set.encode_snapshot();
            writer.write_edit(&snapshot).await?;
            writer.sync().await?;

            write_current(path, &manifest_name).await?;
            writer
        };

        let compaction_picker = CompactionPicker::new(
            options.l0_compaction_trigger,
            options.level_size_multiplier,
            10 * 1024 * 1024,
            options.max_levels,
        );

        let wal = Arc::new(tokio::sync::Mutex::new(None));

        let table_cache = Arc::new(TableCache::new(
            fs.clone(),
            path.to_path_buf(),
            cache.clone(),
            1000,
        ));

        let handle_cache = Arc::new(SstableHandleCache::new(
            fs.clone(),
            path.to_path_buf(),
            cache.clone(),
            1000,
        ));

        let mut db = Self {
            path: path.to_path_buf(),
            options: options.clone(),
            fs,
            memtable: Arc::new(RwLock::new(Arc::new(Memtable::new(4 * 1024 * 1024)))),
            immutable_memtables: Arc::new(Mutex::new(VecDeque::new())),
            version_set: Arc::new(RwLock::new(version_set)),
            manifest: Arc::new(tokio::sync::Mutex::new(manifest_writer)),
            wal: wal.clone(),
            group_commit: None,
            cache,
            table_cache,
            handle_cache,
            write_lock: Mutex::new(()),
            compaction_picker,
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        db.recover().await?;

        let wal_path = path.join(format!(
            "{:06}.wal",
            db.version_set.read().next_file_number()
        ));
        let wal_file = db.fs.create_file(&wal_path).await?;
        let wal_writer = WalWriter::new(wal_file, wal_path, false);
        *db.wal.lock().await = Some(wal_writer);

        db.group_commit = Some(GroupCommitQueue::new(wal.clone()));

        Ok(db)
    }

    async fn recover(&self) -> Result<()> {
        let entries = self.fs.read_dir(&self.path).await?;
        let mut wal_files: Vec<PathBuf> = entries
            .into_iter()
            .filter(|p| p.extension().map(|e| e == "wal").unwrap_or(false))
            .collect();
        wal_files.sort();

        for wal_path in wal_files {
            let mut reader = WalReader::open(&wal_path).await?;
            let records = reader.read_all().await?;

            for record in records {
                match record {
                    WalRecord::Put {
                        sequence,
                        key,
                        value,
                    } => {
                        let memtable = self.memtable.read().clone();
                        memtable.put(key, sequence, value);
                        self.version_set.write().set_last_sequence(sequence);
                    }
                    WalRecord::Delete { sequence, key } => {
                        let memtable = self.memtable.read().clone();
                        memtable.delete(key, sequence);
                        self.version_set.write().set_last_sequence(sequence);
                    }
                    WalRecord::Batch { sequence, ops } => {
                        let memtable = self.memtable.read().clone();
                        let ops_len = ops.len();
                        for (i, op) in ops.into_iter().enumerate() {
                            let seq = sequence + i as u64;
                            match op {
                                BatchOp::Put { key, value } => {
                                    memtable.put(key, seq, value);
                                }
                                BatchOp::Delete { key } => {
                                    memtable.delete(key, seq);
                                }
                            }
                        }
                        if ops_len > 0 {
                            self.version_set
                                .write()
                                .set_last_sequence(sequence + ops_len as u64 - 1);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn put(&self, key: Bytes, value: Bytes, options: WriteOptions) -> Result<()> {
        self.check_write_stall().await?;

        let sequence = self.version_set.read().increment_sequence();

        {
            let memtable = self.memtable.read().clone();
            memtable.put(key.clone(), sequence, value.clone());
        }

        if let Some(ref gc) = self.group_commit {
            gc.submit(
                WriteOp::Put {
                    sequence,
                    key,
                    value,
                },
                options.sync,
            )
            .await?;
        } else {
            let mut wal = self.wal.lock().await;
            if let Some(ref mut w) = *wal {
                w.append(&WalRecord::Put {
                    sequence,
                    key,
                    value,
                })
                .await?;

                if options.sync {
                    w.sync().await?;
                }
            }
        }

        self.maybe_schedule_flush().await?;

        Ok(())
    }

    pub(crate) async fn get(&self, key: &[u8], _options: ReadOptions) -> Result<Option<Bytes>> {
        let sequence = self.version_set.read().last_sequence();

        {
            let memtable = self.memtable.read().clone();
            match memtable.get(key, sequence) {
                LookupResult::Found(value) => return Ok(Some(value)),
                LookupResult::Deleted => return Ok(None),
                LookupResult::NotFound => {}
            }
        }

        {
            let immutables = self.immutable_memtables.lock();
            for imm in immutables.iter().rev() {
                match imm.get(key, sequence) {
                    LookupResult::Found(value) => return Ok(Some(value)),
                    LookupResult::Deleted => return Ok(None),
                    LookupResult::NotFound => {}
                }
            }
        }

        let version = self.version_set.read().current().clone();

        for file in version.get_files_at_level(0) {
            if let Ok(reader) = self.table_cache.get(file.file_number).await {
                match reader.get(key, sequence).await? {
                    LookupResult::Found(value) => return Ok(Some(value)),
                    LookupResult::Deleted => return Ok(None),
                    LookupResult::NotFound => {}
                }
            }
        }

        for level in 1..self.options.max_levels {
            for file in version.get_files_at_level(level) {
                let min_user_key = if file.min_key.len() >= 9 {
                    &file.min_key[..file.min_key.len() - 9]
                } else {
                    file.min_key.as_ref()
                };
                let max_user_key = if file.max_key.len() >= 9 {
                    &file.max_key[..file.max_key.len() - 9]
                } else {
                    file.max_key.as_ref()
                };

                if key >= min_user_key && key <= max_user_key {
                    if let Ok(reader) = self.table_cache.get(file.file_number).await {
                        match reader.get(key, sequence).await? {
                            LookupResult::Found(value) => return Ok(Some(value)),
                            LookupResult::Deleted => return Ok(None),
                            LookupResult::NotFound => {}
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    pub(crate) async fn scan(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let sequence = self.version_set.read().last_sequence();
        let version = self.version_set.read().current().clone();
        let has_immutables = !self.immutable_memtables.lock().is_empty();
        let has_sstables = (0..self.options.max_levels).any(|l| version.num_files_at_level(l) > 0);

        if !has_immutables && !has_sstables {
            let memtable = self.memtable.read().clone();
            return Ok(memtable.scan_range(start, end, sequence, limit));
        }

        self.scan_with_merge(start, end, limit, sequence, &version)
            .await
    }

    async fn scan_with_merge(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        sequence: u64,
        version: &crate::manifest::Version,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        use crate::merge_iterator::MergeIterator;
        use crate::sstable::SstableIterator;
        use crate::types::{InternalKey, ValueType};

        let end_key = Bytes::copy_from_slice(end);
        let mut merge_iter = MergeIterator::new(end_key, sequence);
        let mut source_idx = 0;

        {
            let memtable = self.memtable.read().clone();
            let entries: Vec<_> = memtable.iter().collect();
            merge_iter.add_memtable_entries(entries, start, source_idx);
            source_idx += 1;
        }

        {
            let immutables = self.immutable_memtables.lock();
            for imm in immutables.iter() {
                let entries: Vec<_> = imm.inner().iter().collect();
                merge_iter.add_memtable_entries(entries, start, source_idx);
                source_idx += 1;
            }
        }

        let seek_key = InternalKey::new(Bytes::copy_from_slice(start), u64::MAX, ValueType::Value);
        let encoded_seek = seek_key.encode();

        for file in version.get_files_at_level(0) {
            if let Ok(handle) = self.handle_cache.get(file.file_number).await {
                let mut iter = SstableIterator::from_handle(handle);
                iter.seek(&encoded_seek).await?;
                merge_iter.add_sstable(iter, source_idx).await?;
                source_idx += 1;
            }
        }

        for level in 1..self.options.max_levels {
            for file in version.get_files_at_level(level) {
                let min_user_key = if file.min_key.len() >= 9 {
                    &file.min_key[..file.min_key.len() - 9]
                } else {
                    file.min_key.as_ref()
                };
                let max_user_key = if file.max_key.len() >= 9 {
                    &file.max_key[..file.max_key.len() - 9]
                } else {
                    file.max_key.as_ref()
                };

                if end <= min_user_key || start > max_user_key {
                    continue;
                }

                if let Ok(handle) = self.handle_cache.get(file.file_number).await {
                    let mut iter = SstableIterator::from_handle(handle);
                    iter.seek(&encoded_seek).await?;
                    merge_iter.add_sstable(iter, source_idx).await?;
                    source_idx += 1;
                }
            }
        }

        let mut results = Vec::with_capacity(limit.min(1024));
        while let Some((key, value)) = merge_iter.next().await? {
            results.push((key, value));
            if results.len() >= limit {
                break;
            }
        }

        Ok(results)
    }

    pub(crate) async fn delete(&self, key: &[u8], options: WriteOptions) -> Result<()> {
        self.check_write_stall().await?;

        let sequence = self.version_set.read().increment_sequence();
        let key_bytes = Bytes::copy_from_slice(key);

        {
            let memtable = self.memtable.read().clone();
            memtable.delete(key_bytes.clone(), sequence);
        }

        if let Some(ref gc) = self.group_commit {
            gc.submit(
                WriteOp::Delete {
                    sequence,
                    key: key_bytes,
                },
                options.sync,
            )
            .await?;
        } else {
            let mut wal = self.wal.lock().await;
            if let Some(ref mut w) = *wal {
                w.append(&WalRecord::Delete {
                    sequence,
                    key: key_bytes,
                })
                .await?;

                if options.sync {
                    w.sync().await?;
                }
            }
        }

        self.maybe_schedule_flush().await?;

        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) async fn write_batch(
        &self,
        ops: Vec<DbBatchOp>,
        options: WriteOptions,
    ) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }

        let l0_count = self.version_set.read().current().num_files_at_level(0);
        if l0_count >= self.options.l0_stop_trigger {
            return Err(Error::WriteStall);
        }

        let ops_len = ops.len();
        let sequence = self.version_set.read().increment_sequence();

        let memtable = self.memtable.read().clone();

        if options.sync {
            let mut batch_ops = Vec::with_capacity(ops_len);
            for (i, op) in ops.into_iter().enumerate() {
                let seq = sequence + i as u64;
                match op {
                    DbBatchOp::Put(key, value) => {
                        memtable.put(key.clone(), seq, value.clone());
                        batch_ops.push(BatchOp::Put { key, value });
                    }
                    DbBatchOp::Delete(key) => {
                        memtable.delete(key.clone(), seq);
                        batch_ops.push(BatchOp::Delete { key });
                    }
                }
            }
            if let Some(ref gc) = self.group_commit {
                gc.submit(
                    WriteOp::Batch {
                        sequence,
                        ops: batch_ops,
                    },
                    true,
                )
                .await?;
            }
        } else {
            let mut wal = self.wal.lock().await;
            if let Some(ref mut w) = *wal {
                w.append_batch_owned(sequence, ops, &memtable).await?;
            } else {
                for (i, op) in ops.into_iter().enumerate() {
                    let seq = sequence + i as u64;
                    match op {
                        DbBatchOp::Put(key, value) => {
                            memtable.put(key, seq, value);
                        }
                        DbBatchOp::Delete(key) => {
                            memtable.delete(key, seq);
                        }
                    }
                }
            }
        }

        if ops_len > 1 {
            self.version_set
                .write()
                .set_last_sequence(sequence + ops_len as u64 - 1);
        }

        if memtable.is_full() {
            self.flush().await?;
        }

        Ok(())
    }

    pub(crate) async fn write_batch_buffer(
        &self,
        batch: WriteBatch,
        options: WriteOptions,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let l0_count = self.version_set.read().current().num_files_at_level(0);
        if l0_count >= self.options.l0_stop_trigger {
            return Err(Error::WriteStall);
        }

        let count = batch.count() as usize;
        let sequence = self.version_set.read().increment_sequence();
        let memtable = self.memtable.read().clone();

        let data = batch.into_bytes();
        let mut offset = 0;
        let mut seq = sequence;
        let mut total_size = 0;
        let mut key_buf = Vec::with_capacity(256);

        while offset < data.len() {
            let op_type = data[offset];
            offset += 1;

            let key_len = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;
            let key = &data[offset..offset + key_len];
            offset += key_len;

            if op_type == BATCH_TYPE_PUT {
                let val_len = u32::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]) as usize;
                offset += 4;
                let value = data.slice(offset..offset + val_len);
                offset += val_len;
                total_size += memtable.put_inline(key, seq, value, &mut key_buf);
            } else {
                total_size += memtable.delete_inline(key, seq, &mut key_buf);
            }
            seq += 1;
        }
        memtable.add_size(total_size);

        if !options.sync {
            let mut wal = self.wal.lock().await;
            if let Some(ref mut w) = *wal {
                w.append_batch_raw(sequence, &data, count as u32).await?;
            }
        } else if let Some(ref gc) = self.group_commit {
            let mut batch_ops = Vec::with_capacity(count);
            let mut offset = 0;

            while offset < data.len() {
                let op_type = data[offset];
                offset += 1;
                let key_len = u32::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]) as usize;
                offset += 4;
                let key = data.slice(offset..offset + key_len);
                offset += key_len;

                if op_type == BATCH_TYPE_PUT {
                    let val_len = u32::from_le_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ]) as usize;
                    offset += 4;
                    let value = data.slice(offset..offset + val_len);
                    offset += val_len;
                    batch_ops.push(BatchOp::Put { key, value });
                } else {
                    batch_ops.push(BatchOp::Delete { key });
                }
            }
            gc.submit(
                WriteOp::Batch {
                    sequence,
                    ops: batch_ops,
                },
                true,
            )
            .await?;
        }

        if count > 1 {
            self.version_set
                .write()
                .set_last_sequence(sequence + count as u64 - 1);
        }

        if memtable.is_full() {
            self.flush().await?;
        }

        Ok(())
    }

    async fn check_write_stall(&self) -> Result<()> {
        let version = self.version_set.read().current().clone();
        let l0_count = version.num_files_at_level(0);

        if l0_count >= self.options.l0_stop_trigger {
            return Err(Error::WriteStall);
        }

        if l0_count >= self.options.l0_slowdown_trigger {
            tokio::time::sleep(self.options.write_stall_delay).await;
        }

        Ok(())
    }

    async fn maybe_schedule_flush(&self) -> Result<()> {
        let should_flush = {
            let memtable = self.memtable.read().clone();
            memtable.is_full()
        };

        if should_flush {
            self.flush().await?;
        }

        Ok(())
    }

    pub(crate) async fn flush(&self) -> Result<()> {
        let old_wal_path = {
            let wal_guard = self.wal.lock().await;
            wal_guard.as_ref().map(|w| w.path().to_path_buf())
        };

        let old_memtable = {
            let mut memtable_guard = self.memtable.write();
            let old = memtable_guard.clone();
            *memtable_guard = Arc::new(Memtable::new(self.options.memtable_size));
            old
        };

        {
            let immutable = ImmutableMemtable::new(old_memtable.clone());
            self.immutable_memtables.lock().push_back(immutable);
        }

        let file_number = self.version_set.read().next_file_number();
        let sst_path = self.path.join(format!("{:06}.sst", file_number));
        let sst_file = self.fs.create_file(&sst_path).await?;

        let mut builder = SstableBuilder::new(
            sst_file,
            sst_path,
            self.options.block_size,
            self.options.bloom_bits_per_key,
            old_memtable.len(),
        );

        for (key, value) in old_memtable.iter() {
            builder.add(&key, &value).await?;
        }

        let metadata = builder.finish().await?;

        let mut edit = VersionEdit::new();
        edit.set_last_sequence(self.version_set.read().last_sequence());
        if metadata.entry_count > 0 {
            edit.add_file(
                0,
                file_number,
                metadata.file_size,
                metadata.min_key,
                metadata.max_key,
            );
        }

        self.log_and_apply(&edit).await?;

        {
            self.immutable_memtables.lock().pop_front();
        }

        {
            let wal_path = self.path.join(format!(
                "{:06}.wal",
                self.version_set.read().next_file_number()
            ));
            let wal_file = self.fs.create_file(&wal_path).await?;
            let new_wal = WalWriter::new(wal_file, wal_path, false);
            *self.wal.lock().await = Some(new_wal);
        }

        if let Some(old_path) = old_wal_path {
            if let Err(e) = self.fs.delete_file(&old_path).await {
                tracing::warn!(path = %old_path.display(), error = %e, "Failed to delete old WAL");
            }
        }

        Ok(())
    }

    pub(crate) async fn compact(&self) -> Result<()> {
        let task = {
            let version = self.version_set.read().current().clone();
            self.compaction_picker.pick_compaction(&version)
        };

        if let Some(task) = task {
            let files_to_delete: Vec<_> = task
                .input_files
                .iter()
                .map(|f| self.path.join(format!("{:06}.sst", f.file_number)))
                .collect();

            let file_number = self.version_set.read().next_file_number();
            let compactor = LevelCompactor::new(
                self.fs.clone(),
                self.path.clone(),
                self.cache.clone(),
                self.options.block_size,
                self.options.bloom_bits_per_key,
            );

            let mut edit = compactor.compact(&task, file_number).await?;
            edit.set_last_sequence(self.version_set.read().last_sequence());
            self.log_and_apply(&edit).await?;

            for sst_path in files_to_delete {
                self.table_cache.evict_by_path(&sst_path);
                if let Err(e) = self.fs.delete_file(&sst_path).await {
                    tracing::warn!(path = %sst_path.display(), error = %e, "Failed to delete old SST");
                }
            }
        }

        Ok(())
    }

    async fn log_and_apply(&self, edit: &VersionEdit) -> Result<()> {
        {
            let mut manifest = self.manifest.lock().await;
            manifest.write_edit(edit).await?;
            manifest.sync().await?;
        }

        self.version_set.write().apply(edit);
        Ok(())
    }

    pub(crate) async fn close(self) -> Result<()> {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);

        if let Some(group_commit) = self.group_commit {
            group_commit.shutdown().await;
        }

        {
            let mut wal = self.wal.lock().await;
            if let Some(w) = wal.take() {
                w.close().await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_db_open() {
        let dir = tempdir().unwrap();
        let db = DbInner::open(dir.path(), Options::default()).await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_put_get() {
        let dir = tempdir().unwrap();
        let db = DbInner::open(dir.path(), Options::default()).await.unwrap();

        db.put(
            Bytes::from("key1"),
            Bytes::from("value1"),
            WriteOptions::default(),
        )
        .await
        .unwrap();

        let value = db
            .get(b"key1", ReadOptions::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value.as_ref(), b"value1");

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_delete() {
        let dir = tempdir().unwrap();
        let db = DbInner::open(dir.path(), Options::default()).await.unwrap();

        db.put(
            Bytes::from("key1"),
            Bytes::from("value1"),
            WriteOptions::default(),
        )
        .await
        .unwrap();

        db.delete(b"key1", WriteOptions::default()).await.unwrap();

        let value = db.get(b"key1", ReadOptions::default()).await.unwrap();
        assert!(value.is_none());

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_batch_write() {
        let dir = tempdir().unwrap();
        let db = DbInner::open(dir.path(), Options::default()).await.unwrap();

        let ops = vec![
            DbBatchOp::Put(Bytes::from("k1"), Bytes::from("v1")),
            DbBatchOp::Put(Bytes::from("k2"), Bytes::from("v2")),
            DbBatchOp::Delete(Bytes::from("k1")),
        ];

        db.write_batch(ops, WriteOptions::default()).await.unwrap();

        assert!(db
            .get(b"k1", ReadOptions::default())
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            db.get(b"k2", ReadOptions::default())
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"v2"
        );

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_flush() {
        let dir = tempdir().unwrap();
        let options = Options::default().memtable_size(1024);
        let db = DbInner::open(dir.path(), options).await.unwrap();

        for i in 0..100 {
            db.put(
                Bytes::from(format!("key{:04}", i)),
                Bytes::from("x".repeat(100)),
                WriteOptions::default(),
            )
            .await
            .unwrap();
        }

        db.flush().await.unwrap();

        for i in 0..100 {
            let value = db
                .get(format!("key{:04}", i).as_bytes(), ReadOptions::default())
                .await
                .unwrap();
            assert!(value.is_some());
        }

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_compaction() {
        let dir = tempdir().unwrap();
        let options = Options::default().memtable_size(512);
        let db = DbInner::open(dir.path(), options).await.unwrap();

        for i in 0..200 {
            db.put(
                Bytes::from(format!("key{:04}", i)),
                Bytes::from("x".repeat(50)),
                WriteOptions::default(),
            )
            .await
            .unwrap();
        }

        db.compact().await.unwrap();

        for i in 0..200 {
            let value = db
                .get(format!("key{:04}", i).as_bytes(), ReadOptions::default())
                .await
                .unwrap();
            assert!(value.is_some());
        }

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_recovery() {
        let dir = tempdir().unwrap();

        {
            let db = DbInner::open(dir.path(), Options::default()).await.unwrap();
            db.put(
                Bytes::from("key1"),
                Bytes::from("value1"),
                WriteOptions::default(),
            )
            .await
            .unwrap();
            db.put(
                Bytes::from("key2"),
                Bytes::from("value2"),
                WriteOptions::default(),
            )
            .await
            .unwrap();
            db.close().await.unwrap();
        }

        {
            let db = DbInner::open(dir.path(), Options::default()).await.unwrap();
            let v1 = db.get(b"key1", ReadOptions::default()).await.unwrap();
            assert_eq!(v1.unwrap().as_ref(), b"value1");
            let v2 = db.get(b"key2", ReadOptions::default()).await.unwrap();
            assert_eq!(v2.unwrap().as_ref(), b"value2");
            db.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_db_sync_writes() {
        let dir = tempdir().unwrap();
        let db = DbInner::open(dir.path(), Options::default()).await.unwrap();

        db.put(
            Bytes::from("key1"),
            Bytes::from("value1"),
            WriteOptions { sync: true },
        )
        .await
        .unwrap();

        let value = db.get(b"key1", ReadOptions::default()).await.unwrap();
        assert!(value.is_some());

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_read_from_sstable() {
        let dir = tempdir().unwrap();
        let options = Options::default().memtable_size(64 * 1024);
        let db = DbInner::open(dir.path(), options).await.unwrap();

        for i in 0..50 {
            db.put(
                Bytes::from(format!("key{:04}", i)),
                Bytes::from("x".repeat(20)),
                WriteOptions::default(),
            )
            .await
            .unwrap();
        }

        db.flush().await.unwrap();

        for i in 50..100 {
            db.put(
                Bytes::from(format!("key{:04}", i)),
                Bytes::from("y".repeat(20)),
                WriteOptions::default(),
            )
            .await
            .unwrap();
        }

        for i in 0..50 {
            let value = db
                .get(format!("key{:04}", i).as_bytes(), ReadOptions::default())
                .await
                .unwrap();
            assert!(value.is_some());
        }

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_empty_batch() {
        let dir = tempdir().unwrap();
        let db = DbInner::open(dir.path(), Options::default()).await.unwrap();

        db.write_batch(vec![], WriteOptions::default())
            .await
            .unwrap();

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_single_op_batch() {
        let dir = tempdir().unwrap();
        let db = DbInner::open(dir.path(), Options::default()).await.unwrap();

        let ops = vec![DbBatchOp::Put(Bytes::from("k1"), Bytes::from("v1"))];
        db.write_batch(ops, WriteOptions::default()).await.unwrap();

        let value = db.get(b"k1", ReadOptions::default()).await.unwrap();
        assert!(value.is_some());

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_overwrite() {
        let dir = tempdir().unwrap();
        let db = DbInner::open(dir.path(), Options::default()).await.unwrap();

        db.put(
            Bytes::from("key1"),
            Bytes::from("value1"),
            WriteOptions::default(),
        )
        .await
        .unwrap();

        db.put(
            Bytes::from("key1"),
            Bytes::from("value2"),
            WriteOptions::default(),
        )
        .await
        .unwrap();

        let value = db
            .get(b"key1", ReadOptions::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value.as_ref(), b"value2");

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_not_found() {
        let dir = tempdir().unwrap();
        let db = DbInner::open(dir.path(), Options::default()).await.unwrap();

        let value = db
            .get(b"nonexistent", ReadOptions::default())
            .await
            .unwrap();
        assert!(value.is_none());

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_manifest_recovery() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        {
            let options = Options::default().memtable_size(64 * 1024);
            let db = DbInner::open(&path, options).await.unwrap();

            for i in 0..50 {
                db.put(
                    Bytes::from(format!("key{:04}", i)),
                    Bytes::from(format!("value{:04}", i)),
                    WriteOptions::default(),
                )
                .await
                .unwrap();
            }

            db.flush().await.unwrap();

            let l0_files = db.version_set.read().current().num_files_at_level(0);
            assert!(l0_files > 0, "Should have SST files after flush");

            db.close().await.unwrap();
        }

        {
            let options = Options::default().memtable_size(64 * 1024);
            let db = DbInner::open(&path, options).await.unwrap();

            let l0_files = db.version_set.read().current().num_files_at_level(0);
            assert!(l0_files > 0, "Should recover SST files from manifest");

            for i in 0..50 {
                let key = format!("key{:04}", i);
                let expected = format!("value{:04}", i);
                let value = db
                    .get(key.as_bytes(), ReadOptions::default())
                    .await
                    .unwrap();
                assert!(value.is_some(), "Key {} should exist after recovery", key);
                assert_eq!(
                    value.unwrap().as_ref(),
                    expected.as_bytes(),
                    "Value mismatch for key {}",
                    key
                );
            }

            db.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_db_manifest_recovery_after_compaction() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        {
            let options = Options::default().memtable_size(64 * 1024);
            let db = DbInner::open(&path, options).await.unwrap();

            for batch in 0..3 {
                for i in 0..30 {
                    db.put(
                        Bytes::from(format!("key{:04}", batch * 30 + i)),
                        Bytes::from(format!("value{:04}", batch * 30 + i)),
                        WriteOptions::default(),
                    )
                    .await
                    .unwrap();
                }
                db.flush().await.unwrap();
            }

            db.compact().await.unwrap();
            db.close().await.unwrap();
        }

        {
            let options = Options::default().memtable_size(64 * 1024);
            let db = DbInner::open(&path, options).await.unwrap();

            for i in 0..90 {
                let key = format!("key{:04}", i);
                let expected = format!("value{:04}", i);
                let value = db
                    .get(key.as_bytes(), ReadOptions::default())
                    .await
                    .unwrap();
                assert!(
                    value.is_some(),
                    "Key {} should exist after compaction recovery",
                    key
                );
                assert_eq!(
                    value.unwrap().as_ref(),
                    expected.as_bytes(),
                    "Value mismatch for key {}",
                    key
                );
            }

            db.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_db_scan_memtable_only() {
        let dir = tempdir().unwrap();
        let db = DbInner::open(dir.path(), Options::default()).await.unwrap();

        for i in 0..100 {
            db.put(
                Bytes::from(format!("key{:04}", i)),
                Bytes::from(format!("value{:04}", i)),
                WriteOptions::default(),
            )
            .await
            .unwrap();
        }

        let results = db.scan(b"key0020", b"key0030", 100).await.unwrap();
        assert_eq!(results.len(), 10);
        assert_eq!(results[0].0.as_ref(), b"key0020");
        assert_eq!(results[9].0.as_ref(), b"key0029");

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_scan_with_limit() {
        let dir = tempdir().unwrap();
        let db = DbInner::open(dir.path(), Options::default()).await.unwrap();

        for i in 0..100 {
            db.put(
                Bytes::from(format!("key{:04}", i)),
                Bytes::from(format!("value{:04}", i)),
                WriteOptions::default(),
            )
            .await
            .unwrap();
        }

        let results = db.scan(b"key0000", b"key0100", 5).await.unwrap();
        assert_eq!(results.len(), 5);

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_scan_across_sstable() {
        let dir = tempdir().unwrap();
        let options = Options::default().memtable_size(64 * 1024);
        let db = DbInner::open(dir.path(), options).await.unwrap();

        for i in 0..50 {
            db.put(
                Bytes::from(format!("key{:04}", i)),
                Bytes::from(format!("sst_value{:04}", i)),
                WriteOptions::default(),
            )
            .await
            .unwrap();
        }
        db.flush().await.unwrap();

        for i in 50..100 {
            db.put(
                Bytes::from(format!("key{:04}", i)),
                Bytes::from(format!("mem_value{:04}", i)),
                WriteOptions::default(),
            )
            .await
            .unwrap();
        }

        let results = db.scan(b"key0040", b"key0060", 100).await.unwrap();
        assert_eq!(results.len(), 20);

        for (i, (key, value)) in results.iter().enumerate() {
            let expected_key = format!("key{:04}", 40 + i);
            assert_eq!(key.as_ref(), expected_key.as_bytes());

            if 40 + i < 50 {
                let expected_value = format!("sst_value{:04}", 40 + i);
                assert_eq!(value.as_ref(), expected_value.as_bytes());
            } else {
                let expected_value = format!("mem_value{:04}", 40 + i);
                assert_eq!(value.as_ref(), expected_value.as_bytes());
            }
        }

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_scan_with_updates() {
        let dir = tempdir().unwrap();
        let options = Options::default().memtable_size(64 * 1024);
        let db = DbInner::open(dir.path(), options).await.unwrap();

        for i in 0..50 {
            db.put(
                Bytes::from(format!("key{:04}", i)),
                Bytes::from("old_value"),
                WriteOptions::default(),
            )
            .await
            .unwrap();
        }
        db.flush().await.unwrap();

        for i in 20..30 {
            db.put(
                Bytes::from(format!("key{:04}", i)),
                Bytes::from("new_value"),
                WriteOptions::default(),
            )
            .await
            .unwrap();
        }

        let results = db.scan(b"key0015", b"key0035", 100).await.unwrap();
        assert_eq!(results.len(), 20);

        for (key, value) in &results {
            let key_str = std::str::from_utf8(key).unwrap();
            let key_num: usize = key_str[3..].parse().unwrap();
            if (20..30).contains(&key_num) {
                assert_eq!(
                    value.as_ref(),
                    b"new_value",
                    "Key {} should have new value",
                    key_str
                );
            } else {
                assert_eq!(
                    value.as_ref(),
                    b"old_value",
                    "Key {} should have old value",
                    key_str
                );
            }
        }

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_scan_with_deletes() {
        let dir = tempdir().unwrap();
        let options = Options::default().memtable_size(64 * 1024);
        let db = DbInner::open(dir.path(), options).await.unwrap();

        for i in 0..50 {
            db.put(
                Bytes::from(format!("key{:04}", i)),
                Bytes::from(format!("value{:04}", i)),
                WriteOptions::default(),
            )
            .await
            .unwrap();
        }
        db.flush().await.unwrap();

        for i in 20..30 {
            db.delete(format!("key{:04}", i).as_bytes(), WriteOptions::default())
                .await
                .unwrap();
        }

        let results = db.scan(b"key0015", b"key0035", 100).await.unwrap();
        assert_eq!(results.len(), 10);

        for (key, _) in &results {
            let key_str = std::str::from_utf8(key).unwrap();
            let key_num: usize = key_str[3..].parse().unwrap();
            assert!(
                !(20..30).contains(&key_num),
                "Deleted key {} should not appear",
                key_str
            );
        }

        db.close().await.unwrap();
    }
}
