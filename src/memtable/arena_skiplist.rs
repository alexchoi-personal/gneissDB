use crate::memtable::LookupResult;
use crate::types::{InternalKey, SequenceNumber, ValueType};
use bytes::Bytes;
use std::alloc::{alloc, dealloc, Layout};
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicUsize, Ordering};

const MAX_HEIGHT: usize = 12;
const BRANCHING_FACTOR: u32 = 4;

#[repr(C)]
struct Node {
    key_offset: u32,
    key_len: u32,
    value_offset: u32,
    value_len: u32,
    height: u8,
    _padding: [u8; 3],
    next: [AtomicPtr<Node>; MAX_HEIGHT],
}

impl Node {
    #[allow(clippy::declare_interior_mutable_const)]
    fn new(key_offset: u32, key_len: u32, value_offset: u32, value_len: u32, height: u8) -> Self {
        const NULL: AtomicPtr<Node> = AtomicPtr::new(ptr::null_mut());
        Self {
            key_offset,
            key_len,
            value_offset,
            value_len,
            height,
            _padding: [0; 3],
            next: [NULL; MAX_HEIGHT],
        }
    }

    fn head() -> Self {
        Self::new(0, 0, 0, 0, MAX_HEIGHT as u8)
    }
}

struct Arena {
    data: *mut u8,
    capacity: usize,
    offset: AtomicUsize,
    layout: Layout,
}

impl Arena {
    fn new(capacity: usize) -> Self {
        let layout = Layout::from_size_align(capacity, 8).unwrap();
        let data = unsafe { alloc(layout) };
        Self {
            data,
            capacity,
            offset: AtomicUsize::new(0),
            layout,
        }
    }

    #[inline]
    fn allocate(&self, size: usize, align: usize) -> Option<u32> {
        loop {
            let current = self.offset.load(Ordering::Relaxed);
            let aligned = (current + align - 1) & !(align - 1);
            let new_offset = aligned + size;

            if new_offset > self.capacity {
                return None;
            }

            if self
                .offset
                .compare_exchange_weak(current, new_offset, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return Some(aligned as u32);
            }
        }
    }

    #[inline]
    unsafe fn write(&self, offset: u32, data: &[u8]) {
        ptr::copy_nonoverlapping(data.as_ptr(), self.data.add(offset as usize), data.len());
    }

    #[inline]
    unsafe fn read(&self, offset: u32, len: u32) -> &[u8] {
        std::slice::from_raw_parts(self.data.add(offset as usize), len as usize)
    }

    #[allow(dead_code)]
    fn used(&self) -> usize {
        self.offset.load(Ordering::Relaxed)
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        unsafe { dealloc(self.data, self.layout) }
    }
}

unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

pub(crate) struct ArenaSkipList {
    head: Box<Node>,
    arena: Arena,
    height: AtomicUsize,
    len: AtomicUsize,
    rng: AtomicU32,
}

impl ArenaSkipList {
    pub(crate) fn new(arena_size: usize) -> Self {
        Self {
            head: Box::new(Node::head()),
            arena: Arena::new(arena_size),
            height: AtomicUsize::new(1),
            len: AtomicUsize::new(0),
            rng: AtomicU32::new(0xDEADBEEF),
        }
    }

    #[inline]
    fn random_height(&self) -> usize {
        let mut h = 1;
        loop {
            let mut rng = self.rng.load(Ordering::Relaxed);
            rng ^= rng << 13;
            rng ^= rng >> 17;
            rng ^= rng << 5;
            self.rng.store(rng, Ordering::Relaxed);

            if rng % BRANCHING_FACTOR != 0 || h >= MAX_HEIGHT {
                break;
            }
            h += 1;
        }
        h
    }

    #[allow(clippy::needless_range_loop)]
    pub(crate) fn insert(&self, key: &[u8], value: &[u8]) -> Option<usize> {
        let key_offset = self.arena.allocate(key.len(), 1)?;
        let value_offset = self.arena.allocate(value.len(), 1)?;
        let height = self.random_height();
        let node_offset = self.arena.allocate(std::mem::size_of::<Node>(), 8)?;

        unsafe {
            self.arena.write(key_offset, key);
            self.arena.write(value_offset, value);

            let node_ptr = self.arena.data.add(node_offset as usize) as *mut Node;
            ptr::write(
                node_ptr,
                Node::new(
                    key_offset,
                    key.len() as u32,
                    value_offset,
                    value.len() as u32,
                    height as u8,
                ),
            );

            let head_ptr = self.head.as_ref() as *const Node as *mut Node;
            let mut preds = [head_ptr; MAX_HEIGHT];
            let mut succs = [ptr::null_mut(); MAX_HEIGHT];
            self.find_position(key, &mut preds, &mut succs);

            for level in 0..height {
                (*node_ptr).next[level].store(succs[level], Ordering::Relaxed);
            }

            for level in 0..height {
                loop {
                    let pred_next = &(*preds[level]).next[level];
                    if pred_next
                        .compare_exchange(
                            succs[level],
                            node_ptr,
                            Ordering::SeqCst,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        break;
                    }
                    self.find_position(key, &mut preds, &mut succs);
                    (*node_ptr).next[level].store(succs[level], Ordering::Relaxed);
                }
            }

            let mut cur_height = self.height.load(Ordering::Relaxed);
            while height > cur_height {
                match self.height.compare_exchange(
                    cur_height,
                    height,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(h) => cur_height = h,
                }
            }
        }

        self.len.fetch_add(1, Ordering::Relaxed);
        Some(key.len() + value.len())
    }

    fn find_position(
        &self,
        key: &[u8],
        preds: &mut [*mut Node; MAX_HEIGHT],
        succs: &mut [*mut Node; MAX_HEIGHT],
    ) {
        let mut pred = self.head.as_ref() as *const Node as *mut Node;
        let height = self.height.load(Ordering::Relaxed);

        for level in (0..height).rev() {
            let mut curr = unsafe { (*pred).next[level].load(Ordering::Acquire) };

            while !curr.is_null() {
                let curr_key = self.node_key(curr);
                if curr_key < key {
                    pred = curr;
                    curr = unsafe { (*curr).next[level].load(Ordering::Acquire) };
                } else {
                    break;
                }
            }

            preds[level] = pred;
            succs[level] = curr;
        }
    }

    #[inline]
    fn node_key(&self, node: *mut Node) -> &[u8] {
        unsafe {
            let n = &*node;
            self.arena.read(n.key_offset, n.key_len)
        }
    }

    #[inline]
    fn node_value(&self, node: *mut Node) -> &[u8] {
        unsafe {
            let n = &*node;
            self.arena.read(n.value_offset, n.value_len)
        }
    }

    fn seek_ge(&self, key: &[u8]) -> *mut Node {
        let mut pred = self.head.as_ref() as *const Node as *mut Node;
        let height = self.height.load(Ordering::Relaxed);

        for level in (0..height).rev() {
            let mut curr = unsafe { (*pred).next[level].load(Ordering::Acquire) };

            while !curr.is_null() {
                let curr_key = self.node_key(curr);
                if curr_key < key {
                    pred = curr;
                    curr = unsafe { (*curr).next[level].load(Ordering::Acquire) };
                } else {
                    break;
                }
            }
        }

        unsafe { (*pred).next[0].load(Ordering::Acquire) }
    }

    #[inline]
    fn next(&self, node: *mut Node) -> *mut Node {
        if node.is_null() {
            return ptr::null_mut();
        }
        unsafe { (*node).next[0].load(Ordering::Acquire) }
    }

    fn first(&self) -> *mut Node {
        self.head.next[0].load(Ordering::Acquire)
    }

    pub(crate) fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    pub(crate) fn memory_usage(&self) -> usize {
        self.arena.used()
    }

    pub(crate) fn iter(&self) -> ArenaSkipListIter<'_> {
        ArenaSkipListIter {
            list: self,
            current: self.first(),
        }
    }

    pub(crate) fn range_from(&self, start: &[u8]) -> ArenaSkipListIter<'_> {
        ArenaSkipListIter {
            list: self,
            current: self.seek_ge(start),
        }
    }
}

impl Drop for ArenaSkipList {
    fn drop(&mut self) {
        let mut current = self.head.next[0].load(Ordering::Relaxed);
        while !current.is_null() {
            unsafe {
                let next = (*current).next[0].load(Ordering::Relaxed);
                ptr::drop_in_place(current);
                current = next;
            }
        }
    }
}

unsafe impl Send for ArenaSkipList {}
unsafe impl Sync for ArenaSkipList {}

pub(crate) struct ArenaSkipListIter<'a> {
    list: &'a ArenaSkipList,
    current: *mut Node,
}

unsafe impl Send for ArenaSkipListIter<'_> {}
unsafe impl Sync for ArenaSkipListIter<'_> {}

impl<'a> ArenaSkipListIter<'a> {
    #[inline]
    pub(crate) fn valid(&self) -> bool {
        !self.current.is_null()
    }

    #[inline]
    pub(crate) fn key(&self) -> &'a [u8] {
        debug_assert!(self.valid());
        self.list.node_key(self.current)
    }

    #[inline]
    pub(crate) fn value(&self) -> &'a [u8] {
        debug_assert!(self.valid());
        self.list.node_value(self.current)
    }

    #[inline]
    pub(crate) fn advance(&mut self) {
        self.current = self.list.next(self.current);
    }
}

pub(crate) struct ArenaMemtable {
    skiplist: ArenaSkipList,
    size: AtomicUsize,
    max_size: usize,
}

impl ArenaMemtable {
    pub(crate) fn new(max_size: usize) -> Self {
        let arena_size = max_size * 2;
        Self {
            skiplist: ArenaSkipList::new(arena_size),
            size: AtomicUsize::new(0),
            max_size,
        }
    }

    pub(crate) fn put(&self, user_key: Bytes, sequence: SequenceNumber, value: Bytes) {
        let mut key_buf = Vec::with_capacity(user_key.len() + 9);
        key_buf.extend_from_slice(&user_key);
        key_buf.extend_from_slice(&(!sequence).to_be_bytes());
        key_buf.push(ValueType::Value as u8);

        if let Some(entry_size) = self.skiplist.insert(&key_buf, &value) {
            self.size.fetch_add(entry_size, Ordering::Relaxed);
        }
    }

    pub(crate) fn delete(&self, user_key: Bytes, sequence: SequenceNumber) {
        let mut key_buf = Vec::with_capacity(user_key.len() + 9);
        key_buf.extend_from_slice(&user_key);
        key_buf.extend_from_slice(&(!sequence).to_be_bytes());
        key_buf.push(ValueType::Deletion as u8);

        if let Some(entry_size) = self.skiplist.insert(&key_buf, &[]) {
            self.size.fetch_add(entry_size, Ordering::Relaxed);
        }
    }

    pub(crate) fn get(&self, user_key: &[u8], sequence: SequenceNumber) -> LookupResult {
        let mut seek_key = Vec::with_capacity(user_key.len() + 8);
        seek_key.extend_from_slice(user_key);
        seek_key.extend_from_slice(&(!sequence).to_be_bytes());

        let mut iter = self.skiplist.range_from(&seek_key);

        while iter.valid() {
            let encoded_key = iter.key();
            if encoded_key.len() < 9 {
                iter.advance();
                continue;
            }

            let key_user_len = encoded_key.len() - 9;
            let key_user = &encoded_key[..key_user_len];

            if key_user != user_key {
                break;
            }

            let seq_bytes: [u8; 8] = encoded_key[key_user_len..key_user_len + 8]
                .try_into()
                .unwrap_or([0; 8]);
            let key_seq = !u64::from_be_bytes(seq_bytes);
            let value_type = encoded_key[key_user_len + 8];

            if key_seq <= sequence {
                return if value_type == ValueType::Value as u8 {
                    LookupResult::Found(Bytes::copy_from_slice(iter.value()))
                } else {
                    LookupResult::Deleted
                };
            }

            iter.advance();
        }

        LookupResult::NotFound
    }

    pub(crate) fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        sequence: SequenceNumber,
        limit: usize,
    ) -> Vec<(Bytes, Bytes)> {
        let mut results = Vec::with_capacity(limit.min(128));
        let mut iter = self.skiplist.range_from(start);
        let mut last_user_key_len = 0usize;
        let mut last_user_key_buf = [0u8; 256];

        while iter.valid() && results.len() < limit {
            let encoded_key = iter.key();
            if encoded_key.len() < 9 {
                iter.advance();
                continue;
            }

            let user_key_len = encoded_key.len() - 9;
            let user_key = &encoded_key[..user_key_len];

            if user_key >= end {
                break;
            }

            if last_user_key_len > 0
                && last_user_key_len == user_key_len
                && &last_user_key_buf[..last_user_key_len] == user_key
            {
                iter.advance();
                continue;
            }

            let seq_bytes: [u8; 8] = encoded_key[user_key_len..user_key_len + 8]
                .try_into()
                .unwrap_or([0; 8]);
            let key_seq = !u64::from_be_bytes(seq_bytes);

            if key_seq > sequence {
                iter.advance();
                continue;
            }

            let value_type = encoded_key[encoded_key.len() - 1];

            if user_key_len <= 256 {
                last_user_key_buf[..user_key_len].copy_from_slice(user_key);
                last_user_key_len = user_key_len;
            } else {
                last_user_key_len = 0;
            }

            if value_type == ValueType::Value as u8 {
                results.push((
                    Bytes::copy_from_slice(user_key),
                    Bytes::copy_from_slice(iter.value()),
                ));
            }

            iter.advance();
        }

        results
    }

    pub(crate) fn approximate_size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub(crate) fn is_full(&self) -> bool {
        self.approximate_size() >= self.max_size
    }

    pub(crate) fn iter(&self) -> ArenaMemtableIter<'_> {
        ArenaMemtableIter {
            inner: self.skiplist.iter(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.skiplist.len()
    }

    pub(crate) fn put_inline(
        &self,
        user_key: &[u8],
        sequence: SequenceNumber,
        value: Bytes,
        key_buf: &mut Vec<u8>,
    ) -> usize {
        key_buf.clear();
        key_buf.reserve(user_key.len() + 9);
        key_buf.extend_from_slice(user_key);
        key_buf.extend_from_slice(&(!sequence).to_be_bytes());
        key_buf.push(ValueType::Value as u8);

        self.skiplist.insert(key_buf, &value).unwrap_or(0)
    }

    pub(crate) fn delete_inline(
        &self,
        user_key: &[u8],
        sequence: SequenceNumber,
        key_buf: &mut Vec<u8>,
    ) -> usize {
        key_buf.clear();
        key_buf.reserve(user_key.len() + 9);
        key_buf.extend_from_slice(user_key);
        key_buf.extend_from_slice(&(!sequence).to_be_bytes());
        key_buf.push(ValueType::Deletion as u8);

        self.skiplist.insert(key_buf, &[]).unwrap_or(0)
    }

    pub(crate) fn add_size(&self, size: usize) {
        self.size.fetch_add(size, Ordering::Relaxed);
    }
}

pub(crate) struct ArenaMemtableIter<'a> {
    inner: ArenaSkipListIter<'a>,
}

unsafe impl Send for ArenaMemtableIter<'_> {}
unsafe impl Sync for ArenaMemtableIter<'_> {}

impl<'a> Iterator for ArenaMemtableIter<'a> {
    type Item = (InternalKey, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        while self.inner.valid() {
            let key = self.inner.key();
            let value = self.inner.value();
            self.inner.advance();

            if let Some(internal_key) = InternalKey::decode(key) {
                return Some((internal_key, Bytes::copy_from_slice(value)));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_skiplist_basic() {
        let list = ArenaSkipList::new(1024 * 1024);
        list.insert(b"key1", b"value1");
        list.insert(b"key2", b"value2");
        list.insert(b"key3", b"value3");

        assert_eq!(list.len(), 3);

        let mut iter = list.iter();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key1");
        assert_eq!(iter.value(), b"value1");

        iter.advance();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key2");

        iter.advance();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key3");

        iter.advance();
        assert!(!iter.valid());
    }

    #[test]
    fn test_arena_skiplist_seek() {
        let list = ArenaSkipList::new(1024 * 1024);
        list.insert(b"aaa", b"1");
        list.insert(b"ccc", b"3");
        list.insert(b"eee", b"5");

        let iter = list.range_from(b"bbb");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"ccc");

        let iter = list.range_from(b"ddd");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"eee");

        let iter = list.range_from(b"fff");
        assert!(!iter.valid());
    }

    #[test]
    fn test_arena_memtable_put_get() {
        let mt = ArenaMemtable::new(1024 * 1024);
        mt.put(Bytes::from("key1"), 1, Bytes::from("value1"));

        match mt.get(b"key1", 1) {
            LookupResult::Found(v) => assert_eq!(&v[..], b"value1"),
            _ => panic!("Expected Found"),
        }
    }

    #[test]
    fn test_arena_memtable_delete() {
        let mt = ArenaMemtable::new(1024 * 1024);
        mt.put(Bytes::from("key1"), 1, Bytes::from("value1"));
        mt.delete(Bytes::from("key1"), 2);

        match mt.get(b"key1", 2) {
            LookupResult::Deleted => {}
            _ => panic!("Expected Deleted"),
        }

        match mt.get(b"key1", 1) {
            LookupResult::Found(v) => assert_eq!(&v[..], b"value1"),
            _ => panic!("Expected Found"),
        }
    }

    #[test]
    fn test_arena_memtable_scan() {
        let mt = ArenaMemtable::new(1024 * 1024);
        for i in 0..100 {
            mt.put(
                Bytes::from(format!("key{:03}", i)),
                i as u64,
                Bytes::from(format!("val{:03}", i)),
            );
        }

        let results = mt.scan_range(b"key020", b"key030", 100, 100);
        assert_eq!(results.len(), 10);
        assert_eq!(&results[0].0[..], b"key020");
        assert_eq!(&results[9].0[..], b"key029");
    }

    #[test]
    fn test_arena_memtable_sequence_visibility() {
        let mt = ArenaMemtable::new(1024 * 1024);
        mt.put(Bytes::from("key1"), 5, Bytes::from("value5"));
        mt.put(Bytes::from("key1"), 10, Bytes::from("value10"));

        match mt.get(b"key1", 7) {
            LookupResult::Found(v) => assert_eq!(&v[..], b"value5"),
            _ => panic!("Expected value5"),
        }

        match mt.get(b"key1", 15) {
            LookupResult::Found(v) => assert_eq!(&v[..], b"value10"),
            _ => panic!("Expected value10"),
        }
    }

    #[test]
    fn test_arena_memtable_update_visibility() {
        let mt = ArenaMemtable::new(1024 * 1024);

        // Write with sequence 1
        mt.put(Bytes::from("key1"), 1, Bytes::from("old_value"));

        // Update with sequence 2
        mt.put(Bytes::from("key1"), 2, Bytes::from("new_value"));

        // Get should return new value
        match mt.get(b"key1", 10) {
            LookupResult::Found(v) => assert_eq!(&v[..], b"new_value", "Should get new value"),
            _ => panic!("Expected Found"),
        }

        // scan_range should also return new value
        let results = mt.scan_range(b"key0", b"key2", 10, 10);
        assert_eq!(results.len(), 1);
        assert_eq!(
            &results[0].1[..],
            b"new_value",
            "scan should return new value"
        );

        // iter() should return both entries, newer first
        let entries: Vec<_> = mt.iter().collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0.sequence, 2, "First entry should be newer");
        assert_eq!(entries[1].0.sequence, 1, "Second entry should be older");
    }

    #[test]
    fn test_arena_memtable_iter_order() {
        let mt = ArenaMemtable::new(1024 * 1024);

        // Write multiple keys with different sequences
        mt.put(Bytes::from("key0020"), 1, Bytes::from("old"));
        mt.put(Bytes::from("key0020"), 51, Bytes::from("new"));
        mt.put(Bytes::from("key0021"), 2, Bytes::from("old"));
        mt.put(Bytes::from("key0021"), 52, Bytes::from("new"));

        // iter() should return entries sorted by internal key
        // (user_key, then inverted_seq, so newer seq comes first)
        let entries: Vec<_> = mt.iter().collect();
        assert_eq!(entries.len(), 4);

        // key0020 should come before key0021
        // Within same key, newer seq should come first
        assert_eq!(entries[0].0.user_key.as_ref(), b"key0020");
        assert_eq!(entries[0].0.sequence, 51, "key0020 newer should be first");

        assert_eq!(entries[1].0.user_key.as_ref(), b"key0020");
        assert_eq!(entries[1].0.sequence, 1, "key0020 older should be second");

        assert_eq!(entries[2].0.user_key.as_ref(), b"key0021");
        assert_eq!(entries[2].0.sequence, 52);

        assert_eq!(entries[3].0.user_key.as_ref(), b"key0021");
        assert_eq!(entries[3].0.sequence, 2);
    }

    #[test]
    fn test_arena_memtable_separate_writes() {
        let mt1 = ArenaMemtable::new(1024 * 1024);

        for i in 20..30 {
            mt1.put(
                Bytes::from(format!("key{:04}", i)),
                51 + i as u64 - 20,
                Bytes::from("new_value"),
            );
        }

        let entries: Vec<_> = mt1.iter().collect();
        assert_eq!(entries.len(), 10);

        for (i, (key, value)) in entries.iter().enumerate() {
            let expected_key = format!("key{:04}", 20 + i);
            assert_eq!(
                key.user_key.as_ref(),
                expected_key.as_bytes(),
                "Entry {} key mismatch",
                i
            );
            assert_eq!(value.as_ref(), b"new_value", "Entry {} value mismatch", i);
            assert_eq!(key.sequence, 51 + i as u64, "Entry {} sequence mismatch", i);
        }
    }

    #[test]
    fn test_arena_memtable_sequence_decode() {
        let mt = ArenaMemtable::new(1024 * 1024);

        mt.put(Bytes::from("key1"), 100, Bytes::from("val1"));
        mt.put(Bytes::from("key2"), 200, Bytes::from("val2"));
        mt.put(Bytes::from("key3"), 300, Bytes::from("val3"));

        let entries: Vec<_> = mt.iter().collect();
        assert_eq!(entries.len(), 3);

        assert_eq!(entries[0].0.sequence, 100);
        assert_eq!(entries[1].0.sequence, 200);
        assert_eq!(entries[2].0.sequence, 300);
    }

    #[test]
    fn test_arena_memtable_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let mt = Arc::new(ArenaMemtable::new(10 * 1024 * 1024));
        let mut handles = vec![];

        for t in 0..4 {
            let mt = mt.clone();
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    let key = format!("key-{}-{:04}", t, i);
                    let value = format!("value-{}-{:04}", t, i);
                    mt.put(Bytes::from(key), (t * 1000 + i) as u64, Bytes::from(value));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(mt.len(), 4000);
    }
}
