use crate::error::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::path::{Path, PathBuf};
use tokio::fs;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[async_trait]
#[allow(dead_code)]
pub(crate) trait FileSystem: Send + Sync {
    async fn create_file(&self, path: &Path) -> Result<Box<dyn WritableFile>>;
    async fn open_file(&self, path: &Path) -> Result<Box<dyn RandomAccessFile>>;
    async fn open_writable(&self, path: &Path) -> Result<Box<dyn WritableFile>>;
    async fn delete_file(&self, path: &Path) -> Result<()>;
    async fn rename(&self, from: &Path, to: &Path) -> Result<()>;
    async fn exists(&self, path: &Path) -> bool;
    async fn create_dir_all(&self, path: &Path) -> Result<()>;
    async fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>>;
    async fn file_size(&self, path: &Path) -> Result<u64>;
    async fn read_file(&self, path: &Path) -> Result<Bytes>;
    async fn write_file(&self, path: &Path, data: &[u8]) -> Result<()>;
}

#[async_trait]
pub(crate) trait WritableFile: Send + Sync {
    async fn append(&mut self, data: &[u8]) -> Result<()>;
    async fn sync(&mut self) -> Result<()>;
    async fn close(self: Box<Self>) -> Result<()>;
}

#[async_trait]
pub(crate) trait RandomAccessFile: Send + Sync {
    async fn read(&self, offset: u64, len: usize) -> Result<Bytes>;
    async fn size(&self) -> Result<u64>;
}

pub(crate) struct RealFileSystem;

impl RealFileSystem {
    pub(crate) fn new() -> Self {
        Self
    }
}

#[async_trait]
impl FileSystem for RealFileSystem {
    async fn create_file(&self, path: &Path) -> Result<Box<dyn WritableFile>> {
        let file = std::fs::File::create(path)?;
        Ok(Box::new(RealWritableFile::new(file)))
    }

    async fn open_file(&self, path: &Path) -> Result<Box<dyn RandomAccessFile>> {
        let file = std::fs::File::open(path)?;
        let size = file.metadata()?.len();

        #[cfg(unix)]
        {
            if size > 0 {
                let mmap = unsafe { memmap2::MmapOptions::new().len(size as usize).map(&file) };
                if let Ok(mmap) = mmap {
                    return Ok(Box::new(MmapRandomAccessFile::new(mmap, size)));
                }
            }
        }

        Ok(Box::new(RealRandomAccessFile { file, size }))
    }

    async fn open_writable(&self, path: &Path) -> Result<Box<dyn WritableFile>> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Box::new(RealWritableFile::new(file)))
    }

    async fn delete_file(&self, path: &Path) -> Result<()> {
        fs::remove_file(path).await?;
        Ok(())
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        fs::rename(from, to).await?;
        Ok(())
    }

    async fn exists(&self, path: &Path) -> bool {
        fs::metadata(path).await.is_ok()
    }

    async fn create_dir_all(&self, path: &Path) -> Result<()> {
        fs::create_dir_all(path).await?;
        Ok(())
    }

    async fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let mut entries = Vec::new();
        let mut dir = fs::read_dir(path).await?;
        while let Some(entry) = dir.next_entry().await? {
            entries.push(entry.path());
        }
        Ok(entries)
    }

    async fn file_size(&self, path: &Path) -> Result<u64> {
        let metadata = fs::metadata(path).await?;
        Ok(metadata.len())
    }

    async fn read_file(&self, path: &Path) -> Result<Bytes> {
        let data = fs::read(path).await?;
        Ok(Bytes::from(data))
    }

    async fn write_file(&self, path: &Path, data: &[u8]) -> Result<()> {
        fs::write(path, data).await?;
        Ok(())
    }
}

struct RealWritableFile {
    writer: std::io::BufWriter<std::fs::File>,
}

impl RealWritableFile {
    fn new(file: std::fs::File) -> Self {
        Self {
            writer: std::io::BufWriter::with_capacity(64 * 1024, file),
        }
    }
}

#[async_trait]
impl WritableFile for RealWritableFile {
    async fn append(&mut self, data: &[u8]) -> Result<()> {
        use std::io::Write;
        self.writer.write_all(data)?;
        Ok(())
    }

    async fn sync(&mut self) -> Result<()> {
        use std::io::Write;
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }

    async fn close(self: Box<Self>) -> Result<()> {
        use std::io::Write;
        let mut writer = self.writer;
        writer.flush()?;
        let file = writer
            .into_inner()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        file.sync_data()?;
        Ok(())
    }
}

struct RealRandomAccessFile {
    file: std::fs::File,
    size: u64,
}

#[async_trait]
impl RandomAccessFile for RealRandomAccessFile {
    #[cfg(unix)]
    async fn read(&self, offset: u64, len: usize) -> Result<Bytes> {
        let mut buf = vec![0u8; len];
        self.file.read_at(&mut buf, offset)?;
        Ok(Bytes::from(buf))
    }

    #[cfg(not(unix))]
    async fn read(&self, offset: u64, len: usize) -> Result<Bytes> {
        use std::io::{Read, Seek, SeekFrom};
        let file = &self.file;
        let mut file = std::io::BufReader::new(file);
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf)?;
        Ok(Bytes::from(buf))
    }

    async fn size(&self) -> Result<u64> {
        Ok(self.size)
    }
}

#[cfg(unix)]
struct MmapRandomAccessFile {
    data: Bytes,
    size: u64,
}

#[cfg(unix)]
impl MmapRandomAccessFile {
    fn new(mmap: memmap2::Mmap, size: u64) -> Self {
        let data = Bytes::copy_from_slice(&mmap[..]);
        Self { data, size }
    }
}

#[cfg(unix)]
#[async_trait]
impl RandomAccessFile for MmapRandomAccessFile {
    async fn read(&self, offset: u64, len: usize) -> Result<Bytes> {
        let start = offset as usize;
        let end = std::cmp::min(start + len, self.data.len());
        Ok(self.data.slice(start..end))
    }

    async fn size(&self) -> Result<u64> {
        Ok(self.size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_real_filesystem_create_read() {
        let dir = tempdir().unwrap();
        let fs = RealFileSystem::new();
        let path = dir.path().join("test.txt");

        let mut file = fs.create_file(&path).await.unwrap();
        file.append(b"hello world").await.unwrap();
        file.sync().await.unwrap();
        file.close().await.unwrap();

        let data = fs.read_file(&path).await.unwrap();
        assert_eq!(&data[..], b"hello world");
    }

    #[tokio::test]
    async fn test_real_filesystem_exists() {
        let dir = tempdir().unwrap();
        let fs = RealFileSystem::new();
        let path = dir.path().join("test.txt");

        assert!(!fs.exists(&path).await);
        fs.create_file(&path).await.unwrap();
        assert!(fs.exists(&path).await);
    }

    #[tokio::test]
    async fn test_real_filesystem_delete() {
        let dir = tempdir().unwrap();
        let fs = RealFileSystem::new();
        let path = dir.path().join("test.txt");

        fs.create_file(&path).await.unwrap();
        assert!(fs.exists(&path).await);
        fs.delete_file(&path).await.unwrap();
        assert!(!fs.exists(&path).await);
    }

    #[tokio::test]
    async fn test_real_filesystem_rename() {
        let dir = tempdir().unwrap();
        let fs = RealFileSystem::new();
        let path1 = dir.path().join("test1.txt");
        let path2 = dir.path().join("test2.txt");

        fs.create_file(&path1).await.unwrap();
        fs.rename(&path1, &path2).await.unwrap();
        assert!(!fs.exists(&path1).await);
        assert!(fs.exists(&path2).await);
    }

    #[tokio::test]
    async fn test_real_filesystem_read_dir() {
        let dir = tempdir().unwrap();
        let fs = RealFileSystem::new();

        fs.create_file(&dir.path().join("a.txt")).await.unwrap();
        fs.create_file(&dir.path().join("b.txt")).await.unwrap();

        let entries = fs.read_dir(dir.path()).await.unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn test_random_access_file() {
        let dir = tempdir().unwrap();
        let fs = RealFileSystem::new();
        let path = dir.path().join("test.txt");

        fs.write_file(&path, b"hello world").await.unwrap();

        let file = fs.open_file(&path).await.unwrap();
        assert_eq!(file.size().await.unwrap(), 11);

        let data = file.read(6, 5).await.unwrap();
        assert_eq!(&data[..], b"world");
    }

    #[tokio::test]
    async fn test_file_size() {
        let dir = tempdir().unwrap();
        let fs = RealFileSystem::new();
        let path = dir.path().join("test.txt");

        fs.write_file(&path, b"12345").await.unwrap();
        assert_eq!(fs.file_size(&path).await.unwrap(), 5);
    }

    #[tokio::test]
    async fn test_create_dir_all() {
        let dir = tempdir().unwrap();
        let fs = RealFileSystem::new();
        let path = dir.path().join("a/b/c");

        fs.create_dir_all(&path).await.unwrap();
        assert!(fs.exists(&path).await);
    }

    #[tokio::test]
    async fn test_open_writable_append() {
        let dir = tempdir().unwrap();
        let fs = RealFileSystem::new();
        let path = dir.path().join("test.txt");

        fs.write_file(&path, b"hello").await.unwrap();

        let mut file = fs.open_writable(&path).await.unwrap();
        file.append(b" world").await.unwrap();
        file.close().await.unwrap();

        let data = fs.read_file(&path).await.unwrap();
        assert_eq!(&data[..], b"hello world");
    }
}
