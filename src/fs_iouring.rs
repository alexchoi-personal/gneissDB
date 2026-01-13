#![cfg(all(target_os = "linux", feature = "io_uring"))]

use crate::error::{Error, Result};
use crate::fs::{FileSystem, RandomAccessFile, WritableFile};
use async_trait::async_trait;
use bytes::Bytes;
use io_uring::{opcode, types, IoUring};
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const RING_SIZE: u32 = 1024;
const WRITE_BUFFER_SIZE: usize = 64 * 1024;

pub(crate) struct IoUringContext {
    inner: Mutex<IoUring>,
    next_user_data: AtomicU64,
}

impl IoUringContext {
    pub(crate) fn new() -> Result<Self> {
        let ring = IoUring::builder()
            .setup_sqpoll(1000)
            .build(RING_SIZE)
            .or_else(|_| IoUring::new(RING_SIZE))
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))?;

        Ok(Self {
            inner: Mutex::new(ring),
            next_user_data: AtomicU64::new(1),
        })
    }

    fn next_id(&self) -> u64 {
        self.next_user_data.fetch_add(1, Ordering::Relaxed)
    }

    fn write(&self, fd: RawFd, buf: &[u8], offset: u64) -> Result<usize> {
        let mut ring = self.inner.lock();
        let user_data = self.next_id();

        let write_op = opcode::Write::new(types::Fd(fd), buf.as_ptr(), buf.len() as u32)
            .offset(offset)
            .build()
            .user_data(user_data);

        unsafe {
            ring.submission()
                .push(&write_op)
                .map_err(|_| Error::Io(io::Error::new(io::ErrorKind::Other, "SQ full")))?;
        }

        ring.submit_and_wait(1)
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))?;

        let cqe = loop {
            let cqe = ring
                .completion()
                .next()
                .ok_or_else(|| Error::Io(io::Error::new(io::ErrorKind::Other, "No completion")))?;
            if cqe.user_data() == user_data {
                break cqe;
            }
        };

        let result = cqe.result();
        if result < 0 {
            return Err(Error::Io(io::Error::from_raw_os_error(-result)));
        }

        Ok(result as usize)
    }

    fn read(&self, fd: RawFd, buf: &mut [u8], offset: u64) -> Result<usize> {
        let mut ring = self.inner.lock();
        let user_data = self.next_id();

        let read_op = opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), buf.len() as u32)
            .offset(offset)
            .build()
            .user_data(user_data);

        unsafe {
            ring.submission()
                .push(&read_op)
                .map_err(|_| Error::Io(io::Error::new(io::ErrorKind::Other, "SQ full")))?;
        }

        ring.submit_and_wait(1)
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))?;

        let cqe = loop {
            let cqe = ring
                .completion()
                .next()
                .ok_or_else(|| Error::Io(io::Error::new(io::ErrorKind::Other, "No completion")))?;
            if cqe.user_data() == user_data {
                break cqe;
            }
        };

        let result = cqe.result();
        if result < 0 {
            return Err(Error::Io(io::Error::from_raw_os_error(-result)));
        }

        Ok(result as usize)
    }

    fn fsync(&self, fd: RawFd) -> Result<()> {
        let mut ring = self.inner.lock();
        let user_data = self.next_id();

        let fsync_op = opcode::Fsync::new(types::Fd(fd))
            .build()
            .user_data(user_data);

        unsafe {
            ring.submission()
                .push(&fsync_op)
                .map_err(|_| Error::Io(io::Error::new(io::ErrorKind::Other, "SQ full")))?;
        }

        ring.submit_and_wait(1)
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))?;

        let cqe = loop {
            let cqe = ring
                .completion()
                .next()
                .ok_or_else(|| Error::Io(io::Error::new(io::ErrorKind::Other, "No completion")))?;
            if cqe.user_data() == user_data {
                break cqe;
            }
        };

        let result = cqe.result();
        if result < 0 {
            return Err(Error::Io(io::Error::from_raw_os_error(-result)));
        }

        Ok(())
    }

    fn fdatasync(&self, fd: RawFd) -> Result<()> {
        let mut ring = self.inner.lock();
        let user_data = self.next_id();

        let fsync_op = opcode::Fsync::new(types::Fd(fd))
            .flags(types::FsyncFlags::DATASYNC)
            .build()
            .user_data(user_data);

        unsafe {
            ring.submission()
                .push(&fsync_op)
                .map_err(|_| Error::Io(io::Error::new(io::ErrorKind::Other, "SQ full")))?;
        }

        ring.submit_and_wait(1)
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))?;

        let cqe = loop {
            let cqe = ring
                .completion()
                .next()
                .ok_or_else(|| Error::Io(io::Error::new(io::ErrorKind::Other, "No completion")))?;
            if cqe.user_data() == user_data {
                break cqe;
            }
        };

        let result = cqe.result();
        if result < 0 {
            return Err(Error::Io(io::Error::from_raw_os_error(-result)));
        }

        Ok(())
    }
}

pub(crate) struct IoUringFileSystem {
    ctx: Arc<IoUringContext>,
}

impl IoUringFileSystem {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self {
            ctx: Arc::new(IoUringContext::new()?),
        })
    }
}

#[async_trait]
impl FileSystem for IoUringFileSystem {
    async fn create_file(&self, path: &Path) -> Result<Box<dyn WritableFile>> {
        let file = File::create(path)?;
        Ok(Box::new(IoUringWritableFile::new(file, self.ctx.clone())?))
    }

    async fn open_file(&self, path: &Path) -> Result<Box<dyn RandomAccessFile>> {
        let file = File::open(path)?;
        let size = file.metadata()?.len();
        Ok(Box::new(IoUringRandomAccessFile::new(
            file,
            size,
            self.ctx.clone(),
        )?))
    }

    async fn open_writable(&self, path: &Path) -> Result<Box<dyn WritableFile>> {
        let file = OpenOptions::new().create(true).write(true).open(path)?;
        let offset = file.metadata()?.len();
        Ok(Box::new(IoUringWritableFile::with_offset(
            file,
            self.ctx.clone(),
            offset,
        )?))
    }

    async fn delete_file(&self, path: &Path) -> Result<()> {
        std::fs::remove_file(path)?;
        Ok(())
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        std::fs::rename(from, to)?;
        Ok(())
    }

    async fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    async fn create_dir_all(&self, path: &Path) -> Result<()> {
        std::fs::create_dir_all(path)?;
        Ok(())
    }

    async fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let entries: Vec<PathBuf> = std::fs::read_dir(path)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect();
        Ok(entries)
    }

    async fn file_size(&self, path: &Path) -> Result<u64> {
        let metadata = std::fs::metadata(path)?;
        Ok(metadata.len())
    }

    async fn read_file(&self, path: &Path) -> Result<Bytes> {
        let file = File::open(path)?;
        let size = file.metadata()?.len() as usize;
        let fd = file.as_raw_fd();

        let mut buf = vec![0u8; size];
        let mut total_read = 0;

        while total_read < size {
            let n = self
                .ctx
                .read(fd, &mut buf[total_read..], total_read as u64)?;
            if n == 0 {
                break;
            }
            total_read += n;
        }

        buf.truncate(total_read);
        Ok(Bytes::from(buf))
    }

    async fn write_file(&self, path: &Path, data: &[u8]) -> Result<()> {
        let file = File::create(path)?;
        let fd = file.as_raw_fd();

        let mut total_written = 0;
        while total_written < data.len() {
            let n = self
                .ctx
                .write(fd, &data[total_written..], total_written as u64)?;
            total_written += n;
        }

        self.ctx.fdatasync(fd)?;
        Ok(())
    }
}

pub(crate) struct IoUringWritableFile {
    #[allow(dead_code)]
    file: File,
    fd: RawFd,
    ctx: Arc<IoUringContext>,
    offset: u64,
    buffer: Vec<u8>,
}

impl IoUringWritableFile {
    fn new(file: File, ctx: Arc<IoUringContext>) -> Result<Self> {
        Self::with_offset(file, ctx, 0)
    }

    fn with_offset(file: File, ctx: Arc<IoUringContext>, offset: u64) -> Result<Self> {
        let fd = file.as_raw_fd();
        Ok(Self {
            file,
            fd,
            ctx,
            offset,
            buffer: Vec::with_capacity(WRITE_BUFFER_SIZE),
        })
    }

    fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let mut written = 0;
        while written < self.buffer.len() {
            let n = self
                .ctx
                .write(self.fd, &self.buffer[written..], self.offset)?;
            if n == 0 {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "write returned 0",
                )));
            }
            written += n;
            self.offset += n as u64;
        }

        self.buffer.clear();
        Ok(())
    }
}

#[async_trait]
impl WritableFile for IoUringWritableFile {
    async fn append(&mut self, data: &[u8]) -> Result<()> {
        if self.buffer.len() + data.len() > WRITE_BUFFER_SIZE {
            self.flush_buffer()?;
        }

        if data.len() >= WRITE_BUFFER_SIZE {
            let mut written = 0;
            while written < data.len() {
                let n = self.ctx.write(self.fd, &data[written..], self.offset)?;
                if n == 0 {
                    return Err(Error::Io(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "write returned 0",
                    )));
                }
                written += n;
                self.offset += n as u64;
            }
        } else {
            self.buffer.extend_from_slice(data);
        }

        Ok(())
    }

    async fn sync(&mut self) -> Result<()> {
        self.flush_buffer()?;
        self.ctx.fdatasync(self.fd)?;
        Ok(())
    }

    async fn close(mut self: Box<Self>) -> Result<()> {
        self.flush_buffer()?;
        self.ctx.fsync(self.fd)?;
        Ok(())
    }
}

pub(crate) struct IoUringRandomAccessFile {
    #[allow(dead_code)]
    file: File,
    fd: RawFd,
    ctx: Arc<IoUringContext>,
    size: u64,
}

impl IoUringRandomAccessFile {
    fn new(file: File, size: u64, ctx: Arc<IoUringContext>) -> Result<Self> {
        let fd = file.as_raw_fd();
        Ok(Self {
            file,
            fd,
            ctx,
            size,
        })
    }
}

#[async_trait]
impl RandomAccessFile for IoUringRandomAccessFile {
    async fn read(&self, offset: u64, len: usize) -> Result<Bytes> {
        let actual_len = std::cmp::min(len, (self.size.saturating_sub(offset)) as usize);
        if actual_len == 0 {
            return Ok(Bytes::new());
        }

        let mut buf = vec![0u8; actual_len];
        let mut total_read = 0;

        while total_read < actual_len {
            let n = self
                .ctx
                .read(self.fd, &mut buf[total_read..], offset + total_read as u64)?;
            if n == 0 {
                break;
            }
            total_read += n;
        }

        buf.truncate(total_read);
        Ok(Bytes::from(buf))
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
    async fn test_iouring_filesystem_create_read() {
        let fs = IoUringFileSystem::new().unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.txt");

        let mut file = fs.create_file(&path).await.unwrap();
        file.append(b"hello world").await.unwrap();
        file.sync().await.unwrap();
        file.close().await.unwrap();

        let data = fs.read_file(&path).await.unwrap();
        assert_eq!(&data[..], b"hello world");
    }

    #[tokio::test]
    async fn test_iouring_random_access() {
        let fs = IoUringFileSystem::new().unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.txt");

        fs.write_file(&path, b"hello world").await.unwrap();

        let file = fs.open_file(&path).await.unwrap();
        assert_eq!(file.size().await.unwrap(), 11);

        let data = file.read(6, 5).await.unwrap();
        assert_eq!(&data[..], b"world");
    }

    #[tokio::test]
    async fn test_iouring_append() {
        let fs = IoUringFileSystem::new().unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.txt");

        fs.write_file(&path, b"hello").await.unwrap();

        let mut file = fs.open_writable(&path).await.unwrap();
        file.append(b" world").await.unwrap();
        file.close().await.unwrap();

        let data = fs.read_file(&path).await.unwrap();
        assert_eq!(&data[..], b"hello world");
    }

    #[tokio::test]
    async fn test_iouring_large_write() {
        let fs = IoUringFileSystem::new().unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.txt");

        let mut file = fs.create_file(&path).await.unwrap();

        let chunk = vec![b'x'; 100 * 1024];
        file.append(&chunk).await.unwrap();
        file.sync().await.unwrap();
        file.close().await.unwrap();

        let size = fs.file_size(&path).await.unwrap();
        assert_eq!(size, 100 * 1024);
    }

    #[tokio::test]
    async fn test_iouring_multiple_small_writes() {
        let fs = IoUringFileSystem::new().unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.txt");

        let mut file = fs.create_file(&path).await.unwrap();

        for i in 0..1000 {
            file.append(format!("line {}\n", i).as_bytes())
                .await
                .unwrap();
        }
        file.sync().await.unwrap();
        file.close().await.unwrap();

        let data = fs.read_file(&path).await.unwrap();
        assert!(data.len() > 5000);
    }

    #[tokio::test]
    async fn test_iouring_shared_context() {
        let fs = IoUringFileSystem::new().unwrap();
        let dir = tempdir().unwrap();

        let path1 = dir.path().join("test1.txt");
        let path2 = dir.path().join("test2.txt");

        let mut file1 = fs.create_file(&path1).await.unwrap();
        let mut file2 = fs.create_file(&path2).await.unwrap();

        file1.append(b"file1 content").await.unwrap();
        file2.append(b"file2 content").await.unwrap();

        file1.close().await.unwrap();
        file2.close().await.unwrap();

        let data1 = fs.read_file(&path1).await.unwrap();
        let data2 = fs.read_file(&path2).await.unwrap();

        assert_eq!(&data1[..], b"file1 content");
        assert_eq!(&data2[..], b"file2 content");
    }

    #[tokio::test]
    async fn test_iouring_read_beyond_eof() {
        let fs = IoUringFileSystem::new().unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.txt");

        fs.write_file(&path, b"hello").await.unwrap();

        let file = fs.open_file(&path).await.unwrap();

        let data = file.read(0, 100).await.unwrap();
        assert_eq!(&data[..], b"hello");

        let data = file.read(10, 100).await.unwrap();
        assert!(data.is_empty());
    }

    #[tokio::test]
    async fn test_iouring_write_zero_check() {
        let fs = IoUringFileSystem::new().unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.txt");

        let mut file = fs.create_file(&path).await.unwrap();
        file.append(b"").await.unwrap();
        file.close().await.unwrap();

        let size = fs.file_size(&path).await.unwrap();
        assert_eq!(size, 0);
    }
}
