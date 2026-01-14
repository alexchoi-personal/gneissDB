use crate::cache::BlockCache;
use crate::error::Result;
use crate::fs::{FileSystem, RandomAccessFile};
use crate::sstable::{Block, BlockIterator, Footer, IndexBlock, IndexEntry, FOOTER_SIZE};
use bytes::Bytes;
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;

pub(crate) struct SstableHandle {
    file: Box<dyn RandomAccessFile>,
    pub(crate) file_number: u64,
    pub(crate) index_entries: Vec<IndexEntry>,
    cache: Arc<BlockCache>,
}

impl SstableHandle {
    pub(crate) async fn open(
        file: Box<dyn RandomAccessFile>,
        file_number: u64,
        cache: Arc<BlockCache>,
    ) -> Result<Self> {
        let file_size = file.size().await?;
        let footer_data = file
            .read(file_size - FOOTER_SIZE as u64, FOOTER_SIZE)
            .await?;
        let footer = Footer::decode(&footer_data)?;

        let index_data = file
            .read(footer.index_offset, footer.index_size as usize)
            .await?;
        let index = IndexBlock::decode(index_data)?;

        Ok(Self {
            file,
            file_number,
            index_entries: index.into_entries(),
            cache,
        })
    }

    pub(crate) async fn read_block(&self, offset: u64, size: usize) -> Result<Block> {
        if let Some(data) = self.cache.get(self.file_number, offset) {
            return Ok(Block::new_unchecked(data));
        }

        let data = self.file.read(offset, size).await?;
        let block = Block::new(data.clone())?;
        self.cache.insert(self.file_number, offset, data);
        Ok(block)
    }

    #[allow(dead_code)]
    fn new_iterator(&self) -> TableIterator<'_> {
        TableIterator {
            handle: self,
            current_block_idx: 0,
            current_block: None,
            block_iter: None,
        }
    }
}

#[allow(dead_code)]
struct TableIterator<'a> {
    handle: &'a SstableHandle,
    current_block_idx: usize,
    current_block: Option<Block>,
    block_iter: Option<BlockIterator>,
}

#[allow(dead_code)]
impl<'a> TableIterator<'a> {
    async fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.current_block_idx = 0;
        self.current_block = None;
        self.block_iter = None;

        for (idx, entry) in self.handle.index_entries.iter().enumerate() {
            if entry.last_key.as_ref() >= target {
                self.current_block_idx = idx;
                break;
            }
        }

        if self.current_block_idx < self.handle.index_entries.len() {
            self.load_current_block().await?;

            if let Some(ref block) = self.current_block {
                self.block_iter = Some(block.seek(target));
            }
        }

        Ok(())
    }

    async fn load_current_block(&mut self) -> Result<bool> {
        if self.current_block_idx >= self.handle.index_entries.len() {
            self.current_block = None;
            self.block_iter = None;
            return Ok(false);
        }

        let entry = &self.handle.index_entries[self.current_block_idx];
        let block = self
            .handle
            .read_block(entry.offset, entry.size as usize)
            .await?;

        self.block_iter = Some(block.iter());
        self.current_block = Some(block);

        Ok(true)
    }

    async fn next(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        loop {
            if self.block_iter.is_none() && !self.load_current_block().await? {
                return Ok(None);
            }

            if let Some(ref mut iter) = self.block_iter {
                if let Some(entry) = iter.next() {
                    return Ok(Some(entry));
                }
            }

            self.current_block_idx += 1;
            self.current_block = None;
            self.block_iter = None;
        }
    }
}

pub(crate) struct SstableHandleCache {
    fs: Arc<dyn FileSystem + Send + Sync>,
    db_path: PathBuf,
    block_cache: Arc<BlockCache>,
    handles: DashMap<u64, Arc<SstableHandle>>,
    max_handles: usize,
}

impl SstableHandleCache {
    pub(crate) fn new(
        fs: Arc<dyn FileSystem + Send + Sync>,
        db_path: PathBuf,
        block_cache: Arc<BlockCache>,
        max_handles: usize,
    ) -> Self {
        Self {
            fs,
            db_path,
            block_cache,
            handles: DashMap::with_capacity(max_handles),
            max_handles,
        }
    }

    pub(crate) async fn get(&self, file_number: u64) -> Result<Arc<SstableHandle>> {
        if let Some(handle) = self.handles.get(&file_number) {
            return Ok(handle.clone());
        }

        let path = self.db_path.join(format!("{:06}.sst", file_number));
        let file = self.fs.open_file(&path).await?;
        let handle = SstableHandle::open(file, file_number, self.block_cache.clone()).await?;
        let handle = Arc::new(handle);

        if self.handles.len() >= self.max_handles {
            if let Some(entry) = self.handles.iter().next() {
                let key = *entry.key();
                drop(entry);
                self.handles.remove(&key);
            }
        }

        self.handles.insert(file_number, handle.clone());
        Ok(handle)
    }

    #[allow(dead_code)]
    pub(crate) fn evict(&self, file_number: u64) {
        self.handles.remove(&file_number);
    }
}
