use crate::error::Result;
use crate::manifest::VersionEdit;
use std::path::Path;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

#[allow(dead_code)]
pub(crate) struct ManifestWriter {
    file: File,
}

impl ManifestWriter {
    pub(crate) async fn create(path: &Path) -> Result<Self> {
        let file = File::create(path).await?;
        Ok(Self { file })
    }

    pub(crate) async fn open_append(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().append(true).open(path).await?;
        Ok(Self { file })
    }

    pub(crate) async fn write_edit(&mut self, edit: &VersionEdit) -> Result<()> {
        let data = edit.encode();
        let len = data.len() as u32;
        let crc = crc32fast::hash(&data);

        self.file.write_all(&len.to_le_bytes()).await?;
        self.file.write_all(&crc.to_le_bytes()).await?;
        self.file.write_all(&data).await?;

        Ok(())
    }

    pub(crate) async fn sync(&mut self) -> Result<()> {
        self.file.sync_all().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_manifest_writer_create() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("MANIFEST-000001");

        let mut writer = ManifestWriter::create(&path).await.unwrap();

        let mut edit = VersionEdit::new();
        edit.set_next_file_number(10);
        edit.add_file(0, 1, 1000, Bytes::from("aaa"), Bytes::from("zzz"));

        writer.write_edit(&edit).await.unwrap();
        writer.sync().await.unwrap();

        assert!(path.exists());
    }

    #[tokio::test]
    async fn test_manifest_writer_append() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("MANIFEST-000001");

        {
            let mut writer = ManifestWriter::create(&path).await.unwrap();
            let mut edit = VersionEdit::new();
            edit.set_next_file_number(10);
            writer.write_edit(&edit).await.unwrap();
            writer.sync().await.unwrap();
        }

        {
            let mut writer = ManifestWriter::open_append(&path).await.unwrap();
            let mut edit = VersionEdit::new();
            edit.add_file(0, 1, 1000, Bytes::from("a"), Bytes::from("z"));
            writer.write_edit(&edit).await.unwrap();
            writer.sync().await.unwrap();
        }

        let metadata = tokio::fs::metadata(&path).await.unwrap();
        assert!(metadata.len() > 20);
    }
}
