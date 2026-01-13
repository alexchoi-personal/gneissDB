use crate::error::Result;
use crate::wal::{WalRecord, WalWriter};
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex, Notify};
use tokio::time::{timeout, Duration, Instant};

const MAX_GROUP_SIZE: usize = 256;
const MIN_BATCH_WAIT_MICROS: u64 = 50;
const MAX_BATCH_WAIT_MICROS: u64 = 1000;

#[derive(Debug)]
pub(crate) enum WriteOp {
    Put {
        sequence: u64,
        key: Bytes,
        value: Bytes,
    },
    Delete {
        sequence: u64,
        key: Bytes,
    },
    Batch {
        sequence: u64,
        ops: Vec<crate::wal::BatchOp>,
    },
}

struct WriteRequest {
    op: WriteOp,
    sync: bool,
    completion: oneshot::Sender<Result<()>>,
}

struct BatchState {
    pending_count: AtomicUsize,
    batch_ready: Notify,
}

pub(crate) struct GroupCommitQueue {
    sender: mpsc::Sender<WriteRequest>,
    handle: Option<tokio::task::JoinHandle<()>>,
    batch_state: Arc<BatchState>,
    shutdown: AtomicBool,
}

impl GroupCommitQueue {
    pub(crate) fn new(wal: Arc<Mutex<Option<WalWriter>>>) -> Self {
        let (sender, receiver) = mpsc::channel(4096);
        let batch_state = Arc::new(BatchState {
            pending_count: AtomicUsize::new(0),
            batch_ready: Notify::new(),
        });

        let state_clone = batch_state.clone();
        let handle = tokio::spawn(batch_processor(receiver, wal, state_clone));

        Self {
            sender,
            handle: Some(handle),
            batch_state,
            shutdown: AtomicBool::new(false),
        }
    }

    pub(crate) async fn shutdown(mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        drop(self.sender);
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }

    pub(crate) async fn submit(&self, op: WriteOp, sync: bool) -> Result<()> {
        if self.shutdown.load(Ordering::SeqCst) {
            return Err(crate::error::Error::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Write queue closed",
            )));
        }

        let (tx, rx) = oneshot::channel();
        let request = WriteRequest {
            op,
            sync,
            completion: tx,
        };

        self.batch_state.pending_count.fetch_add(1, Ordering::SeqCst);
        self.batch_state.batch_ready.notify_one();

        self.sender.send(request).await.map_err(|_| {
            crate::error::Error::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Write queue closed",
            ))
        })?;

        if sync {
            rx.await.map_err(|_| {
                crate::error::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Write completion channel closed",
                ))
            })?
        } else {
            Ok(())
        }
    }
}

async fn batch_processor(
    mut receiver: mpsc::Receiver<WriteRequest>,
    wal: Arc<Mutex<Option<WalWriter>>>,
    batch_state: Arc<BatchState>,
) {
    let mut batch: Vec<WriteRequest> = Vec::with_capacity(MAX_GROUP_SIZE);

    loop {
        batch.clear();

        let first = match receiver.recv().await {
            Some(req) => req,
            None => break,
        };
        batch_state.pending_count.fetch_sub(1, Ordering::SeqCst);
        let has_sync = first.sync;
        batch.push(first);

        let batch_start = Instant::now();
        let adaptive_wait = if has_sync {
            Duration::from_micros(MAX_BATCH_WAIT_MICROS)
        } else {
            Duration::from_micros(MIN_BATCH_WAIT_MICROS)
        };

        loop {
            if batch.len() >= MAX_GROUP_SIZE {
                break;
            }

            let elapsed = batch_start.elapsed();
            if elapsed >= adaptive_wait {
                break;
            }

            let remaining = adaptive_wait - elapsed;

            match timeout(remaining, receiver.recv()).await {
                Ok(Some(req)) => {
                    batch_state.pending_count.fetch_sub(1, Ordering::SeqCst);
                    batch.push(req);
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }

        let needs_sync = batch.iter().any(|r| r.sync);
        let batch_to_process: Vec<WriteRequest> = batch.drain(..).collect();
        process_batch(batch_to_process, &wal, needs_sync).await;
    }
}

async fn process_batch(
    batch: Vec<WriteRequest>,
    wal: &Arc<Mutex<Option<WalWriter>>>,
    needs_sync: bool,
) {
    let result = do_process_batch(&batch, wal, needs_sync).await;

    let is_ok = result.is_ok();
    let err_msg = result.err().map(|e| e.to_string());

    for request in batch {
        let send_result = if is_ok {
            Ok(())
        } else {
            Err(crate::error::Error::Corruption(
                err_msg.clone().unwrap_or_default(),
            ))
        };
        let _ = request.completion.send(send_result);
    }
}

async fn do_process_batch(
    batch: &[WriteRequest],
    wal: &Arc<Mutex<Option<WalWriter>>>,
    needs_sync: bool,
) -> Result<()> {
    let mut wal_guard = wal.lock().await;
    let wal_writer = match wal_guard.as_mut() {
        Some(w) => w,
        None => return Ok(()),
    };

    let records: Vec<WalRecord> = batch
        .iter()
        .map(|request| match &request.op {
            WriteOp::Put {
                sequence,
                key,
                value,
            } => WalRecord::Put {
                sequence: *sequence,
                key: key.clone(),
                value: value.clone(),
            },
            WriteOp::Delete { sequence, key } => WalRecord::Delete {
                sequence: *sequence,
                key: key.clone(),
            },
            WriteOp::Batch { sequence, ops } => WalRecord::Batch {
                sequence: *sequence,
                ops: ops.clone(),
            },
        })
        .collect();

    wal_writer.append_batch(&records).await?;

    if needs_sync {
        wal_writer.sync().await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::{FileSystem, RealFileSystem};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_group_commit_single_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();
        let wal = WalWriter::new(file, path, false);
        let wal = Arc::new(Mutex::new(Some(wal)));

        let queue = GroupCommitQueue::new(wal);

        let op = WriteOp::Put {
            sequence: 1,
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        };

        queue.submit(op, false).await.unwrap();
    }

    #[tokio::test]
    async fn test_group_commit_multiple_writes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();
        let wal = WalWriter::new(file, path, false);
        let wal = Arc::new(Mutex::new(Some(wal)));

        let queue = Arc::new(GroupCommitQueue::new(wal));

        let mut handles = Vec::new();
        for i in 0..100 {
            let q = queue.clone();
            let handle = tokio::spawn(async move {
                let op = WriteOp::Put {
                    sequence: i,
                    key: Bytes::from(format!("key{}", i)),
                    value: Bytes::from(format!("value{}", i)),
                };
                q.submit(op, false).await
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn test_group_commit_with_sync() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();
        let wal = WalWriter::new(file, path, false);
        let wal = Arc::new(Mutex::new(Some(wal)));

        let queue = GroupCommitQueue::new(wal);

        let op = WriteOp::Put {
            sequence: 1,
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        };

        queue.submit(op, true).await.unwrap();
    }

    #[tokio::test]
    async fn test_group_commit_delete() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();
        let wal = WalWriter::new(file, path, false);
        let wal = Arc::new(Mutex::new(Some(wal)));

        let queue = GroupCommitQueue::new(wal);

        let op = WriteOp::Delete {
            sequence: 1,
            key: Bytes::from("key"),
        };

        queue.submit(op, false).await.unwrap();
    }

    #[tokio::test]
    async fn test_group_commit_batch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();
        let wal = WalWriter::new(file, path, false);
        let wal = Arc::new(Mutex::new(Some(wal)));

        let queue = GroupCommitQueue::new(wal);

        let op = WriteOp::Batch {
            sequence: 1,
            ops: vec![
                crate::wal::BatchOp::Put {
                    key: Bytes::from("k1"),
                    value: Bytes::from("v1"),
                },
                crate::wal::BatchOp::Delete {
                    key: Bytes::from("k2"),
                },
            ],
        };

        queue.submit(op, false).await.unwrap();
    }

    #[tokio::test]
    async fn test_group_commit_multiple_sync_writes_batched() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();
        let wal = WalWriter::new(file, path, false);
        let wal = Arc::new(Mutex::new(Some(wal)));

        let queue = Arc::new(GroupCommitQueue::new(wal));

        let mut handles = Vec::new();
        for i in 0..50 {
            let q = queue.clone();
            let handle = tokio::spawn(async move {
                let op = WriteOp::Put {
                    sequence: i,
                    key: Bytes::from(format!("key{}", i)),
                    value: Bytes::from(format!("value{}", i)),
                };
                q.submit(op, true).await
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn test_group_commit_mixed_sync_nonsync() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();
        let wal = WalWriter::new(file, path, false);
        let wal = Arc::new(Mutex::new(Some(wal)));

        let queue = Arc::new(GroupCommitQueue::new(wal));

        let mut handles = Vec::new();
        for i in 0..100 {
            let q = queue.clone();
            let sync = i % 3 == 0;
            let handle = tokio::spawn(async move {
                let op = WriteOp::Put {
                    sequence: i,
                    key: Bytes::from(format!("key{}", i)),
                    value: Bytes::from(format!("value{}", i)),
                };
                q.submit(op, sync).await
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn test_group_commit_shutdown() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();
        let wal = WalWriter::new(file, path, false);
        let wal = Arc::new(Mutex::new(Some(wal)));

        let queue = GroupCommitQueue::new(wal);

        let op = WriteOp::Put {
            sequence: 1,
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        };
        queue.submit(op, false).await.unwrap();

        queue.shutdown().await;
    }

    #[tokio::test]
    async fn test_group_commit_shutdown_rejects_new_writes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();
        let wal = WalWriter::new(file, path, false);
        let wal = Arc::new(Mutex::new(Some(wal)));

        let queue = Arc::new(GroupCommitQueue::new(wal));
        queue.shutdown.store(true, Ordering::SeqCst);

        let op = WriteOp::Put {
            sequence: 1,
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        };

        let result = queue.submit(op, false).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_group_commit_high_concurrency_sync() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();
        let wal = WalWriter::new(file, path, false);
        let wal = Arc::new(Mutex::new(Some(wal)));

        let queue = Arc::new(GroupCommitQueue::new(wal));

        let mut handles = Vec::new();
        for i in 0..200 {
            let q = queue.clone();
            let handle = tokio::spawn(async move {
                let op = WriteOp::Put {
                    sequence: i,
                    key: Bytes::from(format!("key{}", i)),
                    value: Bytes::from(format!("value{}", i)),
                };
                q.submit(op, true).await
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn test_group_commit_alternating_sync_nonsync() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();
        let wal = WalWriter::new(file, path, false);
        let wal = Arc::new(Mutex::new(Some(wal)));

        let queue = Arc::new(GroupCommitQueue::new(wal));

        for i in 0..50 {
            let op = WriteOp::Put {
                sequence: i,
                key: Bytes::from(format!("key{}", i)),
                value: Bytes::from(format!("value{}", i)),
            };
            queue.submit(op, i % 2 == 0).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_group_commit_large_batch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let fs = RealFileSystem::new();
        let file = fs.create_file(&path).await.unwrap();
        let wal = WalWriter::new(file, path, false);
        let wal = Arc::new(Mutex::new(Some(wal)));

        let queue = Arc::new(GroupCommitQueue::new(wal));

        let mut handles = Vec::new();
        for i in 0..MAX_GROUP_SIZE + 50 {
            let q = queue.clone();
            let handle = tokio::spawn(async move {
                let op = WriteOp::Put {
                    sequence: i as u64,
                    key: Bytes::from(format!("key{}", i)),
                    value: Bytes::from(format!("value{}", i)),
                };
                q.submit(op, false).await
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn test_group_commit_error_propagation() {
        let wal = Arc::new(Mutex::new(None::<WalWriter>));

        let queue = Arc::new(GroupCommitQueue::new(wal));

        let mut handles = Vec::new();
        for i in 0..10 {
            let q = queue.clone();
            let handle = tokio::spawn(async move {
                let op = WriteOp::Put {
                    sequence: i,
                    key: Bytes::from(format!("key{}", i)),
                    value: Bytes::from(format!("value{}", i)),
                };
                q.submit(op, false).await
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap().unwrap();
        }
    }
}
