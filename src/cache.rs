use bytes::Bytes;
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub(crate) struct BlockCache {
    capacity: usize,
    cache: DashMap<CacheKey, Bytes>,
    current_size: AtomicUsize,
}

#[derive(Clone, Copy, Hash, Eq, PartialEq)]
struct CacheKey {
    file_number: u64,
    block_offset: u64,
}

impl BlockCache {
    pub(crate) fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            cache: DashMap::with_capacity(1024),
            current_size: AtomicUsize::new(0),
        })
    }

    pub(crate) fn get(&self, file_number: u64, block_offset: u64) -> Option<Bytes> {
        let key = CacheKey {
            file_number,
            block_offset,
        };
        self.cache.get(&key).map(|entry| entry.clone())
    }

    pub(crate) fn insert(&self, file_number: u64, block_offset: u64, data: Bytes) {
        let key = CacheKey {
            file_number,
            block_offset,
        };
        let size = data.len();

        if self.cache.contains_key(&key) {
            return;
        }

        let current = self.current_size.load(Ordering::Relaxed);
        if current + size >= self.capacity {
            self.evict_some(size);
        }

        self.cache.insert(key, data);
        self.current_size.fetch_add(size, Ordering::Relaxed);
    }

    fn evict_some(&self, needed: usize) {
        let mut evicted = 0;
        let target = needed.max(self.capacity / 10);

        let keys_to_remove: Vec<_> = self
            .cache
            .iter()
            .take(32)
            .map(|entry| (*entry.key(), entry.value().len()))
            .collect();

        for (key, size) in keys_to_remove {
            if evicted >= target {
                break;
            }
            if self.cache.remove(&key).is_some() {
                evicted += size;
                self.current_size.fetch_sub(size, Ordering::Relaxed);
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn size(&self) -> usize {
        self.current_size.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    pub(crate) fn clear(&self) {
        self.cache.clear();
        self.current_size.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_insert_get() {
        let cache = BlockCache::new(1024);
        cache.insert(1, 0, Bytes::from("hello"));
        let data = cache.get(1, 0).unwrap();
        assert_eq!(&data[..], b"hello");
    }

    #[test]
    fn test_cache_miss() {
        let cache = BlockCache::new(1024);
        assert!(cache.get(1, 0).is_none());
    }

    #[test]
    fn test_cache_eviction() {
        let cache = BlockCache::new(10);
        cache.insert(1, 0, Bytes::from("hello"));
        cache.insert(2, 0, Bytes::from("world!"));
        assert_eq!(cache.cache.len(), 1);
    }

    #[test]
    fn test_cache_size() {
        let cache = BlockCache::new(1024);
        cache.insert(1, 0, Bytes::from("hello"));
        assert_eq!(cache.size(), 5);
    }

    #[test]
    fn test_cache_clear() {
        let cache = BlockCache::new(1024);
        cache.insert(1, 0, Bytes::from("hello"));
        cache.clear();
        assert!(cache.get(1, 0).is_none());
        assert_eq!(cache.size(), 0);
    }

    #[test]
    fn test_cache_duplicate_insert() {
        let cache = BlockCache::new(1024);
        cache.insert(1, 0, Bytes::from("hello"));
        cache.insert(1, 0, Bytes::from("world"));
        let data = cache.get(1, 0).unwrap();
        assert_eq!(&data[..], b"hello");
    }

    #[test]
    fn test_cache_multiple_entries() {
        let cache = BlockCache::new(1024 * 1024);
        for i in 0..100 {
            cache.insert(1, i, Bytes::from(format!("value{}", i)));
        }
        for i in 0..100 {
            let data = cache.get(1, i).unwrap();
            assert_eq!(data.as_ref(), format!("value{}", i).as_bytes());
        }
    }

    #[test]
    fn test_cache_concurrent_access() {
        use std::thread;

        let cache = BlockCache::new(1024 * 1024);
        let mut handles = Vec::new();

        for t in 0..8 {
            let cache = cache.clone();
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    let key = (t * 1000 + i) as u64;
                    cache.insert(1, key, Bytes::from(format!("value{}", key)));
                    let _ = cache.get(1, key);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
