use std::time::Duration;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum IoEngine {
    #[default]
    Standard,
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    IoUring,
}

#[derive(Clone, Debug)]
pub struct Options {
    pub(crate) memtable_size: usize,
    pub(crate) block_size: usize,
    pub(crate) block_cache_size: usize,
    pub(crate) l0_compaction_trigger: usize,
    pub(crate) l0_slowdown_trigger: usize,
    pub(crate) l0_stop_trigger: usize,
    pub(crate) level_size_multiplier: usize,
    pub(crate) max_levels: usize,
    pub(crate) sync_writes: bool,
    pub(crate) bloom_bits_per_key: usize,
    #[allow(dead_code)]
    pub(crate) block_restart_interval: usize,
    pub(crate) write_stall_delay: Duration,
    pub(crate) io_engine: IoEngine,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            memtable_size: 4 * 1024 * 1024,
            block_size: 4 * 1024,
            block_cache_size: 8 * 1024 * 1024,
            l0_compaction_trigger: 4,
            l0_slowdown_trigger: 8,
            l0_stop_trigger: 12,
            level_size_multiplier: 10,
            max_levels: 7,
            sync_writes: true,
            bloom_bits_per_key: 10,
            block_restart_interval: 16,
            write_stall_delay: Duration::from_millis(1),
            io_engine: IoEngine::default(),
        }
    }
}

impl Options {
    pub fn memtable_size(mut self, size: usize) -> Self {
        self.memtable_size = size;
        self
    }

    pub fn block_size(mut self, size: usize) -> Self {
        self.block_size = size;
        self
    }

    pub fn block_cache_size(mut self, size: usize) -> Self {
        self.block_cache_size = size;
        self
    }

    pub fn sync_writes(mut self, sync: bool) -> Self {
        self.sync_writes = sync;
        self
    }

    pub fn bloom_bits_per_key(mut self, bits: usize) -> Self {
        self.bloom_bits_per_key = bits;
        self
    }

    pub fn io_engine(mut self, engine: IoEngine) -> Self {
        self.io_engine = engine;
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct WriteOptions {
    pub(crate) sync: bool,
}

impl WriteOptions {
    pub fn sync(mut self, sync: bool) -> Self {
        self.sync = sync;
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct ReadOptions {
    pub(crate) snapshot: Option<u64>,
}

impl ReadOptions {
    pub fn snapshot(mut self, seq: u64) -> Self {
        self.snapshot = Some(seq);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_options_default() {
        let opts = Options::default();
        assert_eq!(opts.memtable_size, 4 * 1024 * 1024);
        assert_eq!(opts.block_size, 4 * 1024);
        assert_eq!(opts.l0_compaction_trigger, 4);
        assert!(opts.sync_writes);
    }

    #[test]
    fn test_options_builder() {
        let opts = Options::default()
            .memtable_size(8 * 1024 * 1024)
            .block_size(8 * 1024)
            .sync_writes(false)
            .bloom_bits_per_key(15);

        assert_eq!(opts.memtable_size, 8 * 1024 * 1024);
        assert_eq!(opts.block_size, 8 * 1024);
        assert!(!opts.sync_writes);
        assert_eq!(opts.bloom_bits_per_key, 15);
    }

    #[test]
    fn test_write_options() {
        let opts = WriteOptions::default().sync(true);
        assert!(opts.sync);
    }

    #[test]
    fn test_read_options() {
        let opts = ReadOptions::default().snapshot(100);
        assert_eq!(opts.snapshot, Some(100));
    }

    #[test]
    fn test_options_block_cache_size() {
        let opts = Options::default().block_cache_size(16 * 1024 * 1024);
        assert_eq!(opts.block_cache_size, 16 * 1024 * 1024);
    }
}
