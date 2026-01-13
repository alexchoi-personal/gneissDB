use crate::error::{Error, Result};
use crate::wal::{
    BatchOp, WalRecord, BATCH_OP_DELETE, BATCH_OP_PUT, RECORD_TYPE_BATCH, RECORD_TYPE_DELETE,
    RECORD_TYPE_PUT,
};
use bytes::Bytes;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};

pub(crate) struct WalReader {
    reader: BufReader<File>,
}

impl WalReader {
    pub(crate) async fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).await?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }

    pub(crate) async fn read_all(&mut self) -> Result<Vec<WalRecord>> {
        let mut records = Vec::new();
        loop {
            match self.read_record().await {
                Ok(Some(record)) => records.push(record),
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!(error = %e, "WAL read error, stopping recovery");
                    break;
                }
            }
        }
        Ok(records)
    }

    async fn read_record(&mut self) -> Result<Option<WalRecord>> {
        let mut header = [0u8; 8];
        match self.reader.read_exact(&mut header).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }

        let expected_crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;

        let mut data = vec![0u8; len];
        self.reader.read_exact(&mut data).await?;

        let actual_crc = crc32fast::hash(&data);
        if expected_crc != actual_crc {
            return Err(Error::InvalidCrc {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        Self::decode_record(&data).map(Some)
    }

    fn decode_record(data: &[u8]) -> Result<WalRecord> {
        if data.is_empty() {
            return Err(Error::Corruption("Empty record".into()));
        }

        let record_type = data[0];
        let mut pos = 1;

        match record_type {
            RECORD_TYPE_PUT => {
                if data.len() < 13 {
                    return Err(Error::Corruption("Put record too short".into()));
                }
                let sequence = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let key_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                let key = Bytes::copy_from_slice(&data[pos..pos + key_len]);
                pos += key_len;
                let value_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                let value = Bytes::copy_from_slice(&data[pos..pos + value_len]);

                Ok(WalRecord::Put {
                    sequence,
                    key,
                    value,
                })
            }
            RECORD_TYPE_DELETE => {
                if data.len() < 13 {
                    return Err(Error::Corruption("Delete record too short".into()));
                }
                let sequence = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let key_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                let key = Bytes::copy_from_slice(&data[pos..pos + key_len]);

                Ok(WalRecord::Delete { sequence, key })
            }
            RECORD_TYPE_BATCH => {
                if data.len() < 13 {
                    return Err(Error::Corruption("Batch record too short".into()));
                }
                let sequence = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let op_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;

                let mut ops = Vec::with_capacity(op_count);
                for _ in 0..op_count {
                    let op_type = data[pos];
                    pos += 1;

                    match op_type {
                        BATCH_OP_PUT => {
                            let key_len =
                                u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                            pos += 4;
                            let key = Bytes::copy_from_slice(&data[pos..pos + key_len]);
                            pos += key_len;
                            let value_len =
                                u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                            pos += 4;
                            let value = Bytes::copy_from_slice(&data[pos..pos + value_len]);
                            pos += value_len;
                            ops.push(BatchOp::Put { key, value });
                        }
                        BATCH_OP_DELETE => {
                            let key_len =
                                u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                            pos += 4;
                            let key = Bytes::copy_from_slice(&data[pos..pos + key_len]);
                            pos += key_len;
                            ops.push(BatchOp::Delete { key });
                        }
                        _ => {
                            return Err(Error::Corruption(format!(
                                "Unknown batch op type: {}",
                                op_type
                            )))
                        }
                    }
                }

                Ok(WalRecord::Batch { sequence, ops })
            }
            _ => Err(Error::Corruption(format!(
                "Unknown record type: {}",
                record_type
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::{FileSystem, RealFileSystem};
    use crate::wal::WalWriter;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_wal_reader_put() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();

        let file = fs.create_file(&path).await.unwrap();
        let mut writer = WalWriter::new(file, path.clone(), true);
        writer
            .append(&WalRecord::Put {
                sequence: 42,
                key: Bytes::from("key"),
                value: Bytes::from("value"),
            })
            .await
            .unwrap();
        writer.close().await.unwrap();

        let mut reader = WalReader::open(&path).await.unwrap();
        let records = reader.read_all().await.unwrap();
        assert_eq!(records.len(), 1);

        match &records[0] {
            WalRecord::Put {
                sequence,
                key,
                value,
            } => {
                assert_eq!(*sequence, 42);
                assert_eq!(key.as_ref(), b"key");
                assert_eq!(value.as_ref(), b"value");
            }
            _ => panic!("Expected Put record"),
        }
    }

    #[tokio::test]
    async fn test_wal_reader_delete() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();

        let file = fs.create_file(&path).await.unwrap();
        let mut writer = WalWriter::new(file, path.clone(), true);
        writer
            .append(&WalRecord::Delete {
                sequence: 42,
                key: Bytes::from("key"),
            })
            .await
            .unwrap();
        writer.close().await.unwrap();

        let mut reader = WalReader::open(&path).await.unwrap();
        let records = reader.read_all().await.unwrap();
        assert_eq!(records.len(), 1);

        match &records[0] {
            WalRecord::Delete { sequence, key } => {
                assert_eq!(*sequence, 42);
                assert_eq!(key.as_ref(), b"key");
            }
            _ => panic!("Expected Delete record"),
        }
    }

    #[tokio::test]
    async fn test_wal_reader_batch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();

        let file = fs.create_file(&path).await.unwrap();
        let mut writer = WalWriter::new(file, path.clone(), true);
        writer
            .append(&WalRecord::Batch {
                sequence: 42,
                ops: vec![
                    BatchOp::Put {
                        key: Bytes::from("k1"),
                        value: Bytes::from("v1"),
                    },
                    BatchOp::Delete {
                        key: Bytes::from("k2"),
                    },
                ],
            })
            .await
            .unwrap();
        writer.close().await.unwrap();

        let mut reader = WalReader::open(&path).await.unwrap();
        let records = reader.read_all().await.unwrap();
        assert_eq!(records.len(), 1);

        match &records[0] {
            WalRecord::Batch { sequence, ops } => {
                assert_eq!(*sequence, 42);
                assert_eq!(ops.len(), 2);
            }
            _ => panic!("Expected Batch record"),
        }
    }

    #[tokio::test]
    async fn test_wal_reader_multiple_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();

        let file = fs.create_file(&path).await.unwrap();
        let mut writer = WalWriter::new(file, path.clone(), true);

        for i in 0..10 {
            writer
                .append(&WalRecord::Put {
                    sequence: i,
                    key: Bytes::from(format!("key{}", i)),
                    value: Bytes::from(format!("value{}", i)),
                })
                .await
                .unwrap();
        }
        writer.close().await.unwrap();

        let mut reader = WalReader::open(&path).await.unwrap();
        let records = reader.read_all().await.unwrap();
        assert_eq!(records.len(), 10);
    }

    #[tokio::test]
    async fn test_wal_reader_empty_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();

        fs.create_file(&path).await.unwrap();

        let mut reader = WalReader::open(&path).await.unwrap();
        let records = reader.read_all().await.unwrap();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn test_wal_reader_corrupted_crc() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();

        let file = fs.create_file(&path).await.unwrap();
        let mut writer = WalWriter::new(file, path.clone(), true);
        writer
            .append(&WalRecord::Put {
                sequence: 1,
                key: Bytes::from("key"),
                value: Bytes::from("value"),
            })
            .await
            .unwrap();
        writer.close().await.unwrap();

        let mut data = fs.read_file(&path).await.unwrap().to_vec();
        data[0] ^= 0xff;
        fs.write_file(&path, &data).await.unwrap();

        let mut reader = WalReader::open(&path).await.unwrap();
        let records = reader.read_all().await.unwrap();
        assert!(records.is_empty());
    }
}
