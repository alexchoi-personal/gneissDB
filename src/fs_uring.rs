#![cfg(all(target_os = "linux", feature = "io_uring"))]

use crate::error::Result;
use crate::fs::{FileSystem, RandomAccessFile, WritableFile};
use async_trait::async_trait;
use bytes::Bytes;
use io_uring::{opcode, types, IoUring};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::sync::Arc;
use std::thread::JoinHandle;

const RING_SIZE: u32 = 256;
const BATCH_SIZE: usize = 32;
const SMALL_BUFFER_SIZE: usize = 4096;
const LARGE_BUFFER_SIZE: usize = 65536;
const BUFFER_POOL_SIZE: usize = 64;

struct BufferPool {
    small: Mutex<VecDeque<Vec<u8>>>,
    large: Mutex<VecDeque<Vec<u8>>>,
}

impl BufferPool {
    fn new() -> Self {
        let mut small = VecDeque::with_capacity(BUFFER_POOL_SIZE);
        let mut large = VecDeque::with_capacity(BUFFER_POOL_SIZE);

        for _ in 0..BUFFER_POOL_SIZE / 2 {
            small.push_back(vec![0u8; SMALL_BUFFER_SIZE]);
            large.push_back(vec![0u8; LARGE_BUFFER_SIZE]);
        }

        Self {
            small: Mutex::new(small),
            large: Mutex::new(large),
        }
    }

    fn acquire(&self, size: usize) -> Vec<u8> {
        if size <= SMALL_BUFFER_SIZE {
            self.small
                .lock()
                .pop_front()
                .map(|mut b| {
                    b.resize(size, 0);
                    b
                })
                .unwrap_or_else(|| vec![0u8; size])
        } else if size <= LARGE_BUFFER_SIZE {
            self.large
                .lock()
                .pop_front()
                .map(|mut b| {
                    b.resize(size, 0);
                    b
                })
                .unwrap_or_else(|| vec![0u8; size])
        } else {
            vec![0u8; size]
        }
    }

    fn release(&self, mut buf: Vec<u8>) {
        let cap = buf.capacity();
        buf.clear();
        if cap == SMALL_BUFFER_SIZE {
            let mut pool = self.small.lock();
            if pool.len() < BUFFER_POOL_SIZE {
                pool.push_back(buf);
            }
        } else if cap == LARGE_BUFFER_SIZE {
            let mut pool = self.large.lock();
            if pool.len() < BUFFER_POOL_SIZE {
                pool.push_back(buf);
            }
        }
    }
}

#[derive(Debug)]
enum IoOp {
    Read { fd: RawFd, offset: u64, len: usize },
    Write { fd: RawFd, offset: u64, data: Vec<u8> },
    Fsync { fd: RawFd },
}

#[derive(Debug)]
enum IoResult {
    Read(Vec<u8>),
    Write(usize),
    Fsync,
}

struct IoRequest {
    op: IoOp,
    result_tx: tokio::sync::oneshot::Sender<Result<IoResult>>,
}

struct PendingOp {
    request: IoRequest,
    buffer: Option<Vec<u8>>,
}

pub(crate) struct IoUringExecutor {
    sender: Sender<IoRequest>,
    _threads: Vec<JoinHandle<()>>,
    buffer_pool: Arc<BufferPool>,
}

impl IoUringExecutor {
    pub(crate) fn new() -> Result<Self> {
        let num_workers = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4)
            .min(4);

        let (tx, rx) = channel::<IoRequest>();
        let rx = Arc::new(Mutex::new(rx));
        let buffer_pool = Arc::new(BufferPool::new());

        let threads: Vec<_> = (0..num_workers)
            .map(|i| {
                let rx = rx.clone();
                let pool = buffer_pool.clone();
                std::thread::Builder::new()
                    .name(format!("uring-worker-{}", i))
                    .spawn(move || {
                        if let Err(e) = run_uring_loop(&rx, &pool) {
                            tracing::error!("io_uring worker {} failed: {}", i, e);
                        }
                    })
                    .expect("Failed to spawn io_uring worker thread")
            })
            .collect();

        Ok(Self {
            sender: tx,
            _threads: threads,
            buffer_pool,
        })
    }

    async fn submit(&self, op: IoOp) -> Result<IoResult> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let request = IoRequest { op, result_tx: tx };

        self.sender.send(request).map_err(|_| {
            crate::error::Error::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "io_uring executor shut down",
            ))
        })?;

        rx.await.map_err(|_| {
            crate::error::Error::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "io_uring operation cancelled",
            ))
        })?
    }
}

fn run_uring_loop(rx: &Arc<Mutex<Receiver<IoRequest>>>, pool: &BufferPool) -> Result<()> {
    let mut ring = IoUring::new(RING_SIZE)?;
    let mut pending: HashMap<u64, PendingOp> = HashMap::new();
    let next_id = AtomicU64::new(0);

    loop {
        let mut batch_count = 0;

        while batch_count < BATCH_SIZE {
            let recv_result = if pending.is_empty() && batch_count == 0 {
                let guard = rx.lock();
                match guard.recv() {
                    Ok(req) => Some(req),
                    Err(_) => return Ok(()),
                }
            } else {
                let guard = rx.lock();
                match guard.try_recv() {
                    Ok(req) => Some(req),
                    Err(TryRecvError::Empty) => None,
                    Err(TryRecvError::Disconnected) => {
                        if pending.is_empty() {
                            return Ok(());
                        }
                        None
                    }
                }
            };

            match recv_result {
                Some(request) => {
                    let id = next_id.fetch_add(1, Ordering::SeqCst);
                    let (entry, buffer) = prepare_sqe(id, &request.op, pool);

                    unsafe {
                        if ring.submission().push(&entry).is_err() {
                            ring.submit()?;
                            ring.submission()
                                .push(&entry)
                                .expect("submission queue full after submit");
                        }
                    }

                    pending.insert(id, PendingOp { request, buffer });
                    batch_count += 1;
                }
                None => break,
            }
        }

        if batch_count > 0 || !pending.is_empty() {
            let submitted = ring.submit()?;

            if submitted > 0 || !pending.is_empty() {
                ring.submit_and_wait(1)?;
            }

            for cqe in ring.completion() {
                let id = cqe.user_data();
                if let Some(mut pending_op) = pending.remove(&id) {
                    let result = cqe.result();
                    let response = if result < 0 {
                        Err(std::io::Error::from_raw_os_error(-result).into())
                    } else {
                        match &pending_op.request.op {
                            IoOp::Read { len, .. } => {
                                let mut buf = pending_op.buffer.take().unwrap();
                                let bytes_read = result as usize;
                                buf.truncate(bytes_read.min(*len));
                                Ok(IoResult::Read(buf))
                            }
                            IoOp::Write { .. } => {
                                if let Some(buf) = pending_op.buffer.take() {
                                    pool.release(buf);
                                }
                                Ok(IoResult::Write(result as usize))
                            }
                            IoOp::Fsync { .. } => Ok(IoResult::Fsync),
                        }
                    };
                    let _ = pending_op.request.result_tx.send(response);
                }
            }
        }
    }
}

fn prepare_sqe(
    id: u64,
    op: &IoOp,
    pool: &BufferPool,
) -> (io_uring::squeue::Entry, Option<Vec<u8>>) {
    match op {
        IoOp::Read { fd, offset, len } => {
            let mut buf = pool.acquire(*len);
            buf.resize(*len, 0);
            let ptr = buf.as_mut_ptr();
            let entry = opcode::Read::new(types::Fd(*fd), ptr, *len as u32)
                .offset(*offset)
                .build()
                .user_data(id);
            (entry, Some(buf))
        }
        IoOp::Write { fd, offset, data } => {
            let entry = opcode::Write::new(types::Fd(*fd), data.as_ptr(), data.len() as u32)
                .offset(*offset)
                .build()
                .user_data(id);
            (entry, None)
        }
        IoOp::Fsync { fd } => {
            let entry = opcode::Fsync::new(types::Fd(*fd)).build().user_data(id);
            (entry, None)
        }
    }
}

pub(crate) struct IoUringFileSystem {
    executor: Arc<IoUringExecutor>,
}

impl IoUringFileSystem {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self {
            executor: Arc::new(IoUringExecutor::new()?),
        })
    }
}

#[async_trait]
impl FileSystem for IoUringFileSystem {
    async fn create_file(&self, path: &Path) -> Result<Box<dyn WritableFile>> {
        let file = File::create(path)?;
        Ok(Box::new(IoUringWritableFile {
            file,
            executor: self.executor.clone(),
            offset: 0,
        }))
    }

    async fn open_file(&self, path: &Path) -> Result<Box<dyn RandomAccessFile>> {
        let file = File::open(path)?;
        Ok(Box::new(IoUringRandomAccessFile {
            fd: file.as_raw_fd(),
            _file: file,
            executor: self.executor.clone(),
        }))
    }

    async fn open_writable(&self, path: &Path) -> Result<Box<dyn WritableFile>> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let offset = file.metadata()?.len();
        Ok(Box::new(IoUringWritableFile {
            file,
            executor: self.executor.clone(),
            offset,
        }))
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
        let len = file.metadata()?.len() as usize;
        let fd = file.as_raw_fd();

        let result = self
            .executor
            .submit(IoOp::Read { fd, offset: 0, len })
            .await?;

        match result {
            IoResult::Read(buf) => Ok(Bytes::from(buf)),
            _ => unreachable!(),
        }
    }

    async fn write_file(&self, path: &Path, data: &[u8]) -> Result<()> {
        let file = File::create(path)?;
        let fd = file.as_raw_fd();

        self.executor
            .submit(IoOp::Write {
                fd,
                offset: 0,
                data: data.to_vec(),
            })
            .await?;

        self.executor.submit(IoOp::Fsync { fd }).await?;

        Ok(())
    }
}

struct IoUringWritableFile {
    file: File,
    executor: Arc<IoUringExecutor>,
    offset: u64,
}

unsafe impl Send for IoUringWritableFile {}
unsafe impl Sync for IoUringWritableFile {}

#[async_trait]
impl WritableFile for IoUringWritableFile {
    async fn append(&mut self, data: &[u8]) -> Result<()> {
        let fd = self.file.as_raw_fd();
        let offset = self.offset;

        let result = self
            .executor
            .submit(IoOp::Write {
                fd,
                offset,
                data: data.to_vec(),
            })
            .await?;

        if let IoResult::Write(written) = result {
            self.offset += written as u64;
        }

        Ok(())
    }

    async fn sync(&mut self) -> Result<()> {
        let fd = self.file.as_raw_fd();
        self.executor.submit(IoOp::Fsync { fd }).await?;
        Ok(())
    }

    async fn close(mut self: Box<Self>) -> Result<()> {
        self.sync().await?;
        Ok(())
    }
}

struct IoUringRandomAccessFile {
    fd: RawFd,
    _file: File,
    executor: Arc<IoUringExecutor>,
}

unsafe impl Send for IoUringRandomAccessFile {}
unsafe impl Sync for IoUringRandomAccessFile {}

#[async_trait]
impl RandomAccessFile for IoUringRandomAccessFile {
    async fn read(&self, offset: u64, len: usize) -> Result<Bytes> {
        let result = self
            .executor
            .submit(IoOp::Read {
                fd: self.fd,
                offset,
                len,
            })
            .await?;

        match result {
            IoResult::Read(buf) => Ok(Bytes::from(buf)),
            _ => unreachable!(),
        }
    }

    async fn size(&self) -> Result<u64> {
        let metadata = self._file.metadata()?;
        Ok(metadata.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_iouring_filesystem_create_read() {
        let dir = tempdir().unwrap();
        let fs = IoUringFileSystem::new().unwrap();
        let path = dir.path().join("test.txt");

        let mut file = fs.create_file(&path).await.unwrap();
        file.append(b"hello world").await.unwrap();
        file.sync().await.unwrap();
        file.close().await.unwrap();

        let data = fs.read_file(&path).await.unwrap();
        assert_eq!(&data[..], b"hello world");
    }

    #[tokio::test]
    async fn test_iouring_filesystem_write_file() {
        let dir = tempdir().unwrap();
        let fs = IoUringFileSystem::new().unwrap();
        let path = dir.path().join("test.txt");

        fs.write_file(&path, b"test data").await.unwrap();
        let data = fs.read_file(&path).await.unwrap();
        assert_eq!(&data[..], b"test data");
    }

    #[tokio::test]
    async fn test_iouring_random_access() {
        let dir = tempdir().unwrap();
        let fs = IoUringFileSystem::new().unwrap();
        let path = dir.path().join("test.txt");

        fs.write_file(&path, b"hello world").await.unwrap();

        let file = fs.open_file(&path).await.unwrap();
        let data = file.read(6, 5).await.unwrap();
        assert_eq!(&data[..], b"world");
    }

    #[tokio::test]
    async fn test_iouring_large_read() {
        let dir = tempdir().unwrap();
        let fs = IoUringFileSystem::new().unwrap();
        let path = dir.path().join("large.txt");

        let large_data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        fs.write_file(&path, &large_data).await.unwrap();

        let file = fs.open_file(&path).await.unwrap();
        let data = file.read(0, large_data.len()).await.unwrap();
        assert_eq!(&data[..], &large_data[..]);
    }

    #[tokio::test]
    async fn test_iouring_concurrent_reads() {
        let dir = tempdir().unwrap();
        let fs = Arc::new(IoUringFileSystem::new().unwrap());
        let path = dir.path().join("concurrent.txt");

        let data = b"hello concurrent world";
        fs.write_file(&path, data).await.unwrap();

        let file = Arc::new(fs.open_file(&path).await.unwrap());

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let file = file.clone();
                tokio::spawn(async move { file.read(i * 2, 2).await.unwrap() })
            })
            .collect();

        for handle in handles {
            let _ = handle.await.unwrap();
        }
    }
}
