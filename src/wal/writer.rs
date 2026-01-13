use crate::db::DbBatchOp;
use crate::error::Result;
use crate::fs::WritableFile;
use crate::memtable::Memtable;
use crate::wal::{
    BatchOp, WalRecord, BATCH_OP_DELETE, BATCH_OP_PUT, RECORD_TYPE_BATCH, RECORD_TYPE_DELETE,
    RECORD_TYPE_PUT,
};
use bytes::{BufMut, BytesMut};
use std::path::PathBuf;
use std::sync::Arc;

pub(crate) struct WalWriter {
    file: Box<dyn WritableFile>,
    #[allow(dead_code)]
    path: PathBuf,
    sync_on_write: bool,
    encode_buf: Vec<u8>,
    write_buf: BytesMut,
}

impl WalWriter {
    pub(crate) fn new(file: Box<dyn WritableFile>, path: PathBuf, sync_on_write: bool) -> Self {
        Self {
            file,
            path,
            sync_on_write,
            encode_buf: Vec::with_capacity(4096),
            write_buf: BytesMut::with_capacity(4096),
        }
    }

    pub(crate) fn path(&self) -> &PathBuf {
        &self.path
    }

    pub(crate) async fn append(&mut self, record: &WalRecord) -> Result<()> {
        self.append_no_sync(record).await?;

        if self.sync_on_write {
            self.file.sync().await?;
        }

        Ok(())
    }

    pub(crate) async fn append_no_sync(&mut self, record: &WalRecord) -> Result<()> {
        self.encode_buf.clear();
        Self::encode_record_into(record, &mut self.encode_buf);

        let crc = crc32fast::hash(&self.encode_buf);
        let len = self.encode_buf.len() as u32;

        self.write_buf.clear();
        self.write_buf.reserve(8 + self.encode_buf.len());
        self.write_buf.put_u32_le(crc);
        self.write_buf.put_u32_le(len);
        self.write_buf.extend_from_slice(&self.encode_buf);

        self.file.append(&self.write_buf).await?;

        Ok(())
    }

    pub(crate) async fn append_batch(&mut self, records: &[WalRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let total_size: usize = records
            .iter()
            .map(|r| 8 + Self::estimate_record_size(r))
            .sum();

        let mut buf = BytesMut::with_capacity(total_size);

        for record in records {
            let encoded = Self::encode_record(record);
            let crc = crc32fast::hash(&encoded);
            let len = encoded.len() as u32;
            buf.put_u32_le(crc);
            buf.put_u32_le(len);
            buf.extend_from_slice(&encoded);
        }

        self.file.append(&buf).await?;

        Ok(())
    }

    fn estimate_record_size(record: &WalRecord) -> usize {
        match record {
            WalRecord::Put { key, value, .. } => 1 + 8 + 4 + key.len() + 4 + value.len(),
            WalRecord::Delete { key, .. } => 1 + 8 + 4 + key.len(),
            WalRecord::Batch { ops, .. } => {
                1 + 8
                    + 4
                    + ops
                        .iter()
                        .map(|op| match op {
                            BatchOp::Put { key, value } => 1 + 4 + key.len() + 4 + value.len(),
                            BatchOp::Delete { key } => 1 + 4 + key.len(),
                        })
                        .sum::<usize>()
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn append_batch_owned(
        &mut self,
        sequence: u64,
        ops: Vec<DbBatchOp>,
        memtable: &Arc<Memtable>,
    ) -> Result<()> {
        self.encode_buf.clear();
        self.encode_buf.push(RECORD_TYPE_BATCH);
        self.encode_buf.extend_from_slice(&sequence.to_le_bytes());
        self.encode_buf
            .extend_from_slice(&(ops.len() as u32).to_le_bytes());

        for (i, op) in ops.into_iter().enumerate() {
            let seq = sequence + i as u64;
            match op {
                DbBatchOp::Put(key, value) => {
                    self.encode_buf.push(BATCH_OP_PUT);
                    self.encode_buf
                        .extend_from_slice(&(key.len() as u32).to_le_bytes());
                    self.encode_buf.extend_from_slice(&key);
                    self.encode_buf
                        .extend_from_slice(&(value.len() as u32).to_le_bytes());
                    self.encode_buf.extend_from_slice(&value);
                    memtable.put(key, seq, value);
                }
                DbBatchOp::Delete(key) => {
                    self.encode_buf.push(BATCH_OP_DELETE);
                    self.encode_buf
                        .extend_from_slice(&(key.len() as u32).to_le_bytes());
                    self.encode_buf.extend_from_slice(&key);
                    memtable.delete(key, seq);
                }
            }
        }

        let crc = crc32fast::hash(&self.encode_buf);
        let len = self.encode_buf.len() as u32;

        self.write_buf.clear();
        self.write_buf.reserve(8 + self.encode_buf.len());
        self.write_buf.put_u32_le(crc);
        self.write_buf.put_u32_le(len);
        self.write_buf.extend_from_slice(&self.encode_buf);

        self.file.append(&self.write_buf).await?;
        Ok(())
    }

    pub(crate) async fn append_batch_raw(
        &mut self,
        sequence: u64,
        data: &[u8],
        count: u32,
    ) -> Result<()> {
        self.encode_buf.clear();
        self.encode_buf.push(RECORD_TYPE_BATCH);
        self.encode_buf.extend_from_slice(&sequence.to_le_bytes());
        self.encode_buf.extend_from_slice(&count.to_le_bytes());
        self.encode_buf.extend_from_slice(data);

        let crc = crc32fast::hash(&self.encode_buf);
        let len = self.encode_buf.len() as u32;

        self.write_buf.clear();
        self.write_buf.reserve(8 + self.encode_buf.len());
        self.write_buf.put_u32_le(crc);
        self.write_buf.put_u32_le(len);
        self.write_buf.extend_from_slice(&self.encode_buf);

        self.file.append(&self.write_buf).await?;
        Ok(())
    }

    pub(crate) async fn sync(&mut self) -> Result<()> {
        self.file.sync().await
    }

    pub(crate) async fn close(self) -> Result<()> {
        self.file.close().await
    }

    fn encode_record_into(record: &WalRecord, buf: &mut Vec<u8>) {
        let size = Self::estimate_record_size(record);
        buf.reserve(size);

        match record {
            WalRecord::Put {
                sequence,
                key,
                value,
            } => {
                buf.push(RECORD_TYPE_PUT);
                buf.extend_from_slice(&sequence.to_le_bytes());
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                buf.extend_from_slice(value);
            }
            WalRecord::Delete { sequence, key } => {
                buf.push(RECORD_TYPE_DELETE);
                buf.extend_from_slice(&sequence.to_le_bytes());
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
            }
            WalRecord::Batch { sequence, ops } => {
                buf.push(RECORD_TYPE_BATCH);
                buf.extend_from_slice(&sequence.to_le_bytes());
                buf.extend_from_slice(&(ops.len() as u32).to_le_bytes());
                for op in ops {
                    match op {
                        BatchOp::Put { key, value } => {
                            buf.push(BATCH_OP_PUT);
                            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                            buf.extend_from_slice(key);
                            buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                            buf.extend_from_slice(value);
                        }
                        BatchOp::Delete { key } => {
                            buf.push(BATCH_OP_DELETE);
                            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                            buf.extend_from_slice(key);
                        }
                    }
                }
            }
        }
    }

    fn encode_record(record: &WalRecord) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::estimate_record_size(record));
        Self::encode_record_into(record, &mut buf);
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::FileSystem;
    use crate::fs::RealFileSystem;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_wal_writer_put() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();

        let mut writer = WalWriter::new(file, path.clone(), true);
        let record = WalRecord::Put {
            sequence: 1,
            key: bytes::Bytes::from("key"),
            value: bytes::Bytes::from("value"),
        };
        writer.append(&record).await.unwrap();
        writer.close().await.unwrap();

        let data = fs.read_file(&path).await.unwrap();
        assert!(data.len() > 8);
    }

    #[tokio::test]
    async fn test_wal_writer_delete() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();

        let mut writer = WalWriter::new(file, path.clone(), true);
        let record = WalRecord::Delete {
            sequence: 1,
            key: bytes::Bytes::from("key"),
        };
        writer.append(&record).await.unwrap();
        writer.close().await.unwrap();

        let data = fs.read_file(&path).await.unwrap();
        assert!(data.len() > 8);
    }

    #[tokio::test]
    async fn test_wal_writer_batch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();

        let mut writer = WalWriter::new(file, path.clone(), true);
        let record = WalRecord::Batch {
            sequence: 1,
            ops: vec![
                BatchOp::Put {
                    key: bytes::Bytes::from("k1"),
                    value: bytes::Bytes::from("v1"),
                },
                BatchOp::Delete {
                    key: bytes::Bytes::from("k2"),
                },
            ],
        };
        writer.append(&record).await.unwrap();
        writer.close().await.unwrap();

        let data = fs.read_file(&path).await.unwrap();
        assert!(data.len() > 8);
    }

    #[tokio::test]
    async fn test_wal_writer_path() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();

        let writer = WalWriter::new(file, path.clone(), true);
        assert_eq!(writer.path(), &path);
    }

    #[tokio::test]
    async fn test_wal_writer_sync() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();

        let mut writer = WalWriter::new(file, path.clone(), false);
        let record = WalRecord::Put {
            sequence: 1,
            key: bytes::Bytes::from("key"),
            value: bytes::Bytes::from("value"),
        };
        writer.append(&record).await.unwrap();
        writer.sync().await.unwrap();
        writer.close().await.unwrap();
    }
}
