#[cfg(feature = "custom-memtable")]
mod skiplist;
#[cfg(feature = "custom-memtable")]
pub(crate) use skiplist::Memtable;

#[cfg(not(feature = "custom-memtable"))]
mod crossbeam_skiplist;
#[cfg(not(feature = "custom-memtable"))]
pub(crate) use crossbeam_skiplist::Memtable;

use bytes::Bytes;
use std::sync::Arc;

pub(crate) enum LookupResult {
    Found(Bytes),
    Deleted,
    NotFound,
}

pub(crate) struct ImmutableMemtable {
    inner: Arc<Memtable>,
}

impl ImmutableMemtable {
    pub(crate) fn new(memtable: Arc<Memtable>) -> Self {
        Self { inner: memtable }
    }

    pub(crate) fn get(&self, key: &[u8], sequence: u64) -> LookupResult {
        self.inner.get(key, sequence)
    }

    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> &Arc<Memtable> {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_immutable_memtable() {
        let memtable = Arc::new(Memtable::new(1024 * 1024));
        memtable.put(Bytes::from("key"), 1, Bytes::from("value"));
        let immutable = ImmutableMemtable::new(memtable);

        match immutable.get(b"key", 2) {
            LookupResult::Found(v) => assert_eq!(&v[..], b"value"),
            _ => panic!("Expected Found"),
        }
    }

    #[test]
    fn test_immutable_memtable_inner() {
        let memtable = Arc::new(Memtable::new(1024 * 1024));
        let immutable = ImmutableMemtable::new(memtable.clone());
        assert!(Arc::ptr_eq(immutable.inner(), &memtable));
    }
}
