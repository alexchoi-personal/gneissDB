use crate::error::{Error, Result};
use crate::manifest::VersionEdit;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};

#[allow(dead_code)]
pub(crate) struct ManifestReader {
    reader: BufReader<File>,
}

impl ManifestReader {
    pub(crate) async fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).await?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }

    pub(crate) async fn read_all(&mut self) -> Result<Vec<VersionEdit>> {
        let mut edits = Vec::new();
        loop {
            match self.read_edit().await {
                Ok(Some(edit)) => edits.push(edit),
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!(error = %e, "Manifest read error, stopping");
                    break;
                }
            }
        }
        Ok(edits)
    }

    async fn read_edit(&mut self) -> Result<Option<VersionEdit>> {
        let mut header = [0u8; 8];
        match self.reader.read_exact(&mut header).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }

        let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
        let expected_crc = u32::from_le_bytes(header[4..8].try_into().unwrap());

        let mut data = vec![0u8; len];
        self.reader.read_exact(&mut data).await?;

        let actual_crc = crc32fast::hash(&data);
        if expected_crc != actual_crc {
            return Err(Error::InvalidCrc {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        match VersionEdit::decode(&data) {
            Some(edit) => Ok(Some(edit)),
            None => Err(Error::Corruption("Invalid manifest edit".into())),
        }
    }
}

#[allow(dead_code)]
pub(crate) async fn read_current(db_path: &Path) -> Result<Option<String>> {
    let current_path = db_path.join("CURRENT");
    match tokio::fs::read_to_string(&current_path).await {
        Ok(contents) => Ok(Some(contents.trim().to_string())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

#[allow(dead_code)]
pub(crate) async fn write_current(db_path: &Path, manifest_name: &str) -> Result<()> {
    let current_path = db_path.join("CURRENT");
    let temp_path = db_path.join("CURRENT.tmp");

    tokio::fs::write(&temp_path, format!("{}\n", manifest_name)).await?;
    tokio::fs::rename(&temp_path, &current_path).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::ManifestWriter;
    use bytes::Bytes;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_manifest_reader_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("MANIFEST-000001");

        {
            let mut writer = ManifestWriter::create(&path).await.unwrap();

            let mut edit1 = VersionEdit::new();
            edit1.set_next_file_number(10);
            edit1.set_last_sequence(100);
            writer.write_edit(&edit1).await.unwrap();

            let mut edit2 = VersionEdit::new();
            edit2.add_file(0, 1, 1000, Bytes::from("a"), Bytes::from("z"));
            writer.write_edit(&edit2).await.unwrap();

            writer.sync().await.unwrap();
        }

        let mut reader = ManifestReader::open(&path).await.unwrap();
        let edits = reader.read_all().await.unwrap();

        assert_eq!(edits.len(), 2);
        assert_eq!(edits[0].next_file_number, Some(10));
        assert_eq!(edits[0].last_sequence, Some(100));
        assert_eq!(edits[1].new_files.len(), 1);
    }

    #[tokio::test]
    async fn test_manifest_reader_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("MANIFEST-000001");

        ManifestWriter::create(&path).await.unwrap();

        let mut reader = ManifestReader::open(&path).await.unwrap();
        let edits = reader.read_all().await.unwrap();

        assert!(edits.is_empty());
    }

    #[tokio::test]
    async fn test_current_file() {
        let dir = tempdir().unwrap();
        let db_path = dir.path();

        assert!(read_current(db_path).await.unwrap().is_none());

        write_current(db_path, "MANIFEST-000001").await.unwrap();

        let current = read_current(db_path).await.unwrap();
        assert_eq!(current, Some("MANIFEST-000001".to_string()));

        write_current(db_path, "MANIFEST-000002").await.unwrap();

        let current = read_current(db_path).await.unwrap();
        assert_eq!(current, Some("MANIFEST-000002".to_string()));
    }

    #[tokio::test]
    async fn test_manifest_reader_corrupted() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("MANIFEST-000001");

        {
            let mut writer = ManifestWriter::create(&path).await.unwrap();
            let mut edit = VersionEdit::new();
            edit.set_next_file_number(10);
            writer.write_edit(&edit).await.unwrap();
            writer.sync().await.unwrap();
        }

        let mut data = tokio::fs::read(&path).await.unwrap();
        if let Some(last) = data.last_mut() {
            *last ^= 0xff;
        }
        tokio::fs::write(&path, &data).await.unwrap();

        let mut reader = ManifestReader::open(&path).await.unwrap();
        let edits = reader.read_all().await.unwrap();
        assert!(edits.is_empty());
    }
}
