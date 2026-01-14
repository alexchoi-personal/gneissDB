use crate::memtable::LookupResult;
use crate::types::{InternalKey, SequenceNumber, ValueType};
use bytes::Bytes;
use parking_lot::Mutex;
use std::alloc::{alloc, dealloc, Layout};
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

const MAX_HEIGHT: usize = 12;
const ARENA_BLOCK_SIZE: usize = 4 * 1024 * 1024;

struct Arena {
    blocks: Mutex<Vec<(*mut u8, Layout)>>,
    current: AtomicPtr<u8>,
    remaining: AtomicUsize,
}

impl Arena {
    fn new() -> Self {
        let (ptr, layout) = Self::alloc_block(ARENA_BLOCK_SIZE);
        Self {
            blocks: Mutex::new(vec![(ptr, layout)]),
            current: AtomicPtr::new(ptr),
            remaining: AtomicUsize::new(ARENA_BLOCK_SIZE),
        }
    }

    fn alloc_block(size: usize) -> (*mut u8, Layout) {
        let layout = Layout::from_size_align(size, 8).unwrap();
        let ptr = unsafe { alloc(layout) };
        (ptr, layout)
    }

    fn allocate(&self, size: usize, align: usize) -> *mut u8 {
        loop {
            let remaining = self.remaining.load(Ordering::Relaxed);
            let current = self.current.load(Ordering::Relaxed);

            let current_addr = current as usize;
            let alloc_start = current_addr + (remaining - size);
            let aligned_start = alloc_start & !(align - 1);
            let padding = alloc_start - aligned_start;
            let total_needed = size + padding;

            if remaining >= total_needed {
                let new_remaining = remaining - total_needed;
                if self
                    .remaining
                    .compare_exchange(
                        remaining,
                        new_remaining,
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return aligned_start as *mut u8;
                }
            } else {
                let new_block_size = ARENA_BLOCK_SIZE.max(size + align);
                let (new_ptr, new_layout) = Self::alloc_block(new_block_size);

                let mut blocks = self.blocks.lock();
                blocks.push((new_ptr, new_layout));

                self.current.store(new_ptr, Ordering::Release);
                self.remaining.store(new_block_size, Ordering::Release);
            }
        }
    }

    fn allocate_node_inline(&self, key: &[u8], value: Bytes, height: usize) -> *mut Node {
        let key_len = key.len();
        let node_size = std::mem::size_of::<Node>();
        let node_align = std::mem::align_of::<Node>();

        let node_ptr = self.allocate(node_size, node_align) as *mut Node;

        let key_ptr = self.allocate(key_len, 1);
        unsafe {
            ptr::copy_nonoverlapping(key.as_ptr(), key_ptr, key_len);
            ptr::write(
                node_ptr,
                Node::new_inline(key_ptr, key_len as u32, value, height),
            );
        }

        node_ptr
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        let blocks = self.blocks.get_mut();
        for (ptr, layout) in blocks.drain(..) {
            unsafe {
                dealloc(ptr, layout);
            }
        }
    }
}

unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

#[allow(dead_code)]
struct Node {
    key_ptr: *const u8,
    key_len: u32,
    value: Bytes,
    height: usize,
    next: [AtomicPtr<Node>; MAX_HEIGHT],
}

impl Node {
    #[allow(clippy::declare_interior_mutable_const)]
    fn new_inline(key_ptr: *const u8, key_len: u32, value: Bytes, height: usize) -> Self {
        const NULL: AtomicPtr<Node> = AtomicPtr::new(ptr::null_mut());
        Self {
            key_ptr,
            key_len,
            value,
            height,
            next: [NULL; MAX_HEIGHT],
        }
    }

    fn head() -> Self {
        Self::new_inline(ptr::null(), 0, Bytes::new(), MAX_HEIGHT)
    }

    #[inline]
    fn key(&self) -> &[u8] {
        if self.key_ptr.is_null() {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.key_ptr, self.key_len as usize) }
        }
    }
}

struct LockFreeSkipList {
    head: Box<Node>,
    arena: Arena,
    height: AtomicUsize,
    len: AtomicUsize,
    rng_state: AtomicUsize,
    last_insert: AtomicPtr<Node>,
}

impl LockFreeSkipList {
    fn new() -> Self {
        Self {
            head: Box::new(Node::head()),
            arena: Arena::new(),
            height: AtomicUsize::new(1),
            len: AtomicUsize::new(0),
            rng_state: AtomicUsize::new(0xDEADBEEF),
            last_insert: AtomicPtr::new(ptr::null_mut()),
        }
    }

    #[inline]
    fn try_insert_after_hint(&self, key: &[u8], hint: *mut Node) -> Option<(*mut Node, *mut Node)> {
        if hint.is_null() {
            return None;
        }

        unsafe {
            let hint_key = (*hint).key();
            if hint_key >= key {
                return None;
            }

            let next = (*hint).next[0].load(Ordering::Acquire);
            if next.is_null() {
                return Some((hint, next));
            }

            let next_key = (*next).key();
            if key < next_key {
                return Some((hint, next));
            }
        }

        None
    }

    fn random_height(&self) -> usize {
        let mut state = self.rng_state.load(Ordering::Relaxed);
        let mut height = 1;
        loop {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            self.rng_state.store(state, Ordering::Relaxed);

            if state & 3 != 0 {
                break;
            }
            height += 1;
            if height >= MAX_HEIGHT {
                break;
            }
        }
        height
    }

    fn insert_inline(&self, key: &[u8], value: Bytes) -> usize {
        let entry_size = key.len() + value.len();
        let head_ptr = self.head.as_ref() as *const Node as *mut Node;
        let height = self.random_height();

        let hint = self.last_insert.load(Ordering::Relaxed);
        if height == 1 {
            if let Some((pred, succ)) = self.try_insert_after_hint(key, hint) {
                let new_node = self.arena.allocate_node_inline(key, value.clone(), 1);
                unsafe {
                    (*new_node).next[0].store(succ, Ordering::Relaxed);
                    let pred_next = &(*pred).next[0];
                    if pred_next
                        .compare_exchange(succ, new_node, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        self.last_insert.store(new_node, Ordering::Relaxed);
                        self.len.fetch_add(1, Ordering::Relaxed);
                        return entry_size;
                    }
                }
            }
        }

        let new_node = self.arena.allocate_node_inline(key, value, height);

        loop {
            let mut preds: [*mut Node; MAX_HEIGHT] = [head_ptr; MAX_HEIGHT];
            let mut succs: [*mut Node; MAX_HEIGHT] = [ptr::null_mut(); MAX_HEIGHT];

            self.find(key, &mut preds, &mut succs);

            for (level, &succ) in succs.iter().enumerate().take(height) {
                unsafe {
                    (*new_node).next[level].store(succ, Ordering::Relaxed);
                }
            }

            unsafe {
                let pred = &(*preds[0]).next[0];
                match pred.compare_exchange(succs[0], new_node, Ordering::SeqCst, Ordering::SeqCst)
                {
                    Ok(_) => {
                        for level in 1..height {
                            loop {
                                let pred = &(*preds[level]).next[level];
                                let succ = succs[level];

                                if (*new_node).next[level].load(Ordering::SeqCst) != succ {
                                    self.find(key, &mut preds, &mut succs);
                                    (*new_node).next[level].store(succs[level], Ordering::Relaxed);
                                    continue;
                                }

                                match pred.compare_exchange(
                                    succ,
                                    new_node,
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                ) {
                                    Ok(_) => break,
                                    Err(_) => {
                                        self.find(key, &mut preds, &mut succs);
                                        (*new_node).next[level]
                                            .store(succs[level], Ordering::Relaxed);
                                    }
                                }
                            }
                        }

                        let mut current_height = self.height.load(Ordering::Relaxed);
                        while height > current_height {
                            match self.height.compare_exchange(
                                current_height,
                                height,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            ) {
                                Ok(_) => break,
                                Err(h) => current_height = h,
                            }
                        }

                        self.last_insert.store(new_node, Ordering::Relaxed);
                        self.len.fetch_add(1, Ordering::Relaxed);
                        return entry_size;
                    }
                    Err(_) => continue,
                }
            }
        }
    }

    fn find(
        &self,
        key: &[u8],
        preds: &mut [*mut Node; MAX_HEIGHT],
        succs: &mut [*mut Node; MAX_HEIGHT],
    ) {
        let mut pred = self.head.as_ref() as *const Node as *mut Node;
        let height = self.height.load(Ordering::Relaxed);

        for level in (0..height).rev() {
            let mut curr = unsafe { (*pred).next[level].load(Ordering::SeqCst) };

            while !curr.is_null() {
                let curr_key = unsafe { (*curr).key() };
                if curr_key < key {
                    pred = curr;
                    curr = unsafe { (*curr).next[level].load(Ordering::SeqCst) };
                } else {
                    break;
                }
            }

            preds[level] = pred;
            succs[level] = curr;
        }
    }

    fn seek_ge(&self, key: &Bytes) -> Option<(*const Node, Bytes, Bytes)> {
        let mut pred = self.head.as_ref() as *const Node as *mut Node;
        let height = self.height.load(Ordering::Relaxed);

        for level in (0..height).rev() {
            let mut curr = unsafe { (*pred).next[level].load(Ordering::SeqCst) };

            while !curr.is_null() {
                let curr_key = unsafe { (*curr).key() };
                if curr_key < key.as_ref() {
                    pred = curr;
                    curr = unsafe { (*curr).next[level].load(Ordering::SeqCst) };
                } else {
                    break;
                }
            }
        }

        let result = unsafe { (*pred).next[0].load(Ordering::SeqCst) };
        if result.is_null() {
            None
        } else {
            unsafe {
                Some((
                    result,
                    Bytes::copy_from_slice((*result).key()),
                    (*result).value.clone(),
                ))
            }
        }
    }

    fn next_node(&self, node: *const Node) -> Option<(*const Node, Bytes, Bytes)> {
        if node.is_null() {
            return None;
        }
        let next = unsafe { (*(node as *mut Node)).next[0].load(Ordering::SeqCst) };
        if next.is_null() {
            None
        } else {
            unsafe {
                Some((
                    next,
                    Bytes::copy_from_slice((*next).key()),
                    (*next).value.clone(),
                ))
            }
        }
    }

    fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    fn iter(&self) -> LockFreeSkipListIter<'_> {
        LockFreeSkipListIter {
            current: self.head.next[0].load(Ordering::SeqCst),
            _marker: std::marker::PhantomData,
        }
    }

    #[allow(dead_code)]
    fn seek_ge_ptr(&self, key: &[u8]) -> *mut Node {
        let mut pred = self.head.as_ref() as *const Node as *mut Node;
        let height = self.height.load(Ordering::Relaxed);

        for level in (0..height).rev() {
            let mut curr = unsafe { (*pred).next[level].load(Ordering::SeqCst) };

            while !curr.is_null() {
                let curr_key = unsafe { (*curr).key() };
                if curr_key < key {
                    pred = curr;
                    curr = unsafe { (*curr).next[level].load(Ordering::SeqCst) };
                } else {
                    break;
                }
            }
        }

        unsafe { (*pred).next[0].load(Ordering::SeqCst) }
    }

    #[allow(dead_code)]
    fn range_iter<'a>(&'a self, start: &[u8]) -> RangeIter<'a> {
        RangeIter {
            current: self.seek_ge_ptr(start),
            _marker: std::marker::PhantomData,
        }
    }
}

impl Drop for LockFreeSkipList {
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

#[allow(dead_code)]
struct RangeIter<'a> {
    current: *mut Node,
    _marker: std::marker::PhantomData<&'a LockFreeSkipList>,
}

#[allow(dead_code)]
impl<'a> RangeIter<'a> {
    #[inline]
    fn next_ref(&mut self) -> Option<(&'a [u8], &'a [u8])> {
        if self.current.is_null() {
            return None;
        }

        unsafe {
            let key = (*self.current).key();
            let value = (*self.current).value.as_ref();
            self.current = (*self.current).next[0].load(Ordering::SeqCst);
            Some((key, value))
        }
    }
}

unsafe impl Send for LockFreeSkipList {}
unsafe impl Sync for LockFreeSkipList {}

struct LockFreeSkipListIter<'a> {
    current: *mut Node,
    _marker: std::marker::PhantomData<&'a LockFreeSkipList>,
}

impl<'a> Iterator for LockFreeSkipListIter<'a> {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_null() {
            return None;
        }

        unsafe {
            let key = Bytes::copy_from_slice((*self.current).key());
            let value = (*self.current).value.clone();
            self.current = (*self.current).next[0].load(Ordering::SeqCst);
            Some((key, value))
        }
    }
}

pub(crate) struct Memtable {
    skiplist: LockFreeSkipList,
    size: AtomicUsize,
    max_size: usize,
}

impl Memtable {
    pub(crate) fn new(max_size: usize) -> Self {
        Self {
            skiplist: LockFreeSkipList::new(),
            size: AtomicUsize::new(0),
            max_size,
        }
    }

    pub(crate) fn put(&self, user_key: Bytes, sequence: SequenceNumber, value: Bytes) {
        thread_local! {
            static BUF: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::with_capacity(256));
        }

        BUF.with(|buf| {
            let mut buf = buf.borrow_mut();
            Self::encode_key_into(&mut buf, &user_key, sequence, ValueType::Value);
            let entry_size = self.skiplist.insert_inline(&buf, value);
            self.size.fetch_add(entry_size, Ordering::Relaxed);
        });
    }

    pub(crate) fn delete(&self, user_key: Bytes, sequence: SequenceNumber) {
        thread_local! {
            static BUF: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::with_capacity(256));
        }

        BUF.with(|buf| {
            let mut buf = buf.borrow_mut();
            Self::encode_key_into(&mut buf, &user_key, sequence, ValueType::Deletion);
            let entry_size = self.skiplist.insert_inline(&buf, Bytes::new());
            self.size.fetch_add(entry_size, Ordering::Relaxed);
        });
    }

    #[inline]
    fn encode_key_into(
        buf: &mut Vec<u8>,
        user_key: &[u8],
        sequence: SequenceNumber,
        value_type: ValueType,
    ) {
        buf.clear();
        buf.reserve(user_key.len() + 9);
        buf.extend_from_slice(user_key);
        buf.extend_from_slice(&(!sequence).to_be_bytes());
        buf.push(value_type as u8);
    }

    #[allow(dead_code)]
    pub(crate) fn put_batch(&self, ops: &[(Bytes, SequenceNumber, Option<Bytes>)]) {
        if ops.is_empty() {
            return;
        }

        let mut key_buf = Vec::with_capacity(256);
        let mut total_size = 0;

        for (user_key, sequence, value) in ops {
            let value_type = if value.is_some() {
                ValueType::Value
            } else {
                ValueType::Deletion
            };
            Self::encode_key_into(&mut key_buf, user_key, *sequence, value_type);
            let val = value.clone().unwrap_or_else(Bytes::new);
            total_size += self.skiplist.insert_inline(&key_buf, val);
        }
        self.size.fetch_add(total_size, Ordering::Relaxed);
    }

    pub(crate) fn put_inline(
        &self,
        user_key: &[u8],
        sequence: SequenceNumber,
        value: Bytes,
        key_buf: &mut Vec<u8>,
    ) -> usize {
        Self::encode_key_into(key_buf, user_key, sequence, ValueType::Value);
        self.skiplist.insert_inline(key_buf, value)
    }

    pub(crate) fn delete_inline(
        &self,
        user_key: &[u8],
        sequence: SequenceNumber,
        key_buf: &mut Vec<u8>,
    ) -> usize {
        Self::encode_key_into(key_buf, user_key, sequence, ValueType::Deletion);
        self.skiplist.insert_inline(key_buf, Bytes::new())
    }

    pub(crate) fn add_size(&self, size: usize) {
        self.size.fetch_add(size, Ordering::Relaxed);
    }

    pub(crate) fn get(&self, user_key: &[u8], sequence: SequenceNumber) -> LookupResult {
        let mut seek_prefix = Vec::with_capacity(user_key.len() + 8);
        seek_prefix.extend_from_slice(user_key);
        let inverted_seq = !sequence;
        seek_prefix.extend_from_slice(&inverted_seq.to_be_bytes());
        let seek_key = Bytes::from(seek_prefix);

        let mut current = self.skiplist.seek_ge(&seek_key);

        while let Some((node, encoded_key, value)) = current {
            let Some(key) = InternalKey::decode(&encoded_key) else {
                current = self.skiplist.next_node(node);
                continue;
            };

            if key.user_key.as_ref() != user_key {
                break;
            }

            if key.sequence <= sequence {
                return match key.value_type {
                    ValueType::Value => LookupResult::Found(value),
                    ValueType::Deletion => LookupResult::Deleted,
                };
            }

            current = self.skiplist.next_node(node);
        }

        LookupResult::NotFound
    }

    pub(crate) fn approximate_size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub(crate) fn is_full(&self) -> bool {
        self.approximate_size() >= self.max_size
    }

    pub(crate) fn iter(&self) -> MemtableIterator {
        let entries: Vec<_> = self
            .skiplist
            .iter()
            .filter_map(|(k, v)| InternalKey::decode(&k).map(|key| (key, v)))
            .collect();
        MemtableIterator {
            entries,
            position: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.skiplist.len()
    }

    #[allow(dead_code)]
    pub(crate) fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        sequence: SequenceNumber,
        limit: usize,
    ) -> Vec<(Bytes, Bytes)> {
        let mut results = Vec::with_capacity(limit.min(64));
        let mut iter = self.skiplist.range_iter(start);
        let mut last_user_key: Option<Vec<u8>> = None;

        while let Some((encoded_key, value)) = iter.next_ref() {
            if encoded_key.len() < 9 {
                continue;
            }

            let user_key_len = encoded_key.len() - 9;
            let user_key = &encoded_key[..user_key_len];

            if user_key >= end {
                break;
            }

            if user_key < start {
                continue;
            }

            if let Some(ref last) = last_user_key {
                if last.as_slice() == user_key {
                    continue;
                }
            }

            let seq_bytes: [u8; 8] = encoded_key[user_key_len..user_key_len + 8]
                .try_into()
                .unwrap_or([0; 8]);
            let inverted_seq = u64::from_be_bytes(seq_bytes);
            let key_sequence = !inverted_seq;
            let value_type = encoded_key[user_key_len + 8];

            if key_sequence > sequence {
                continue;
            }

            last_user_key = Some(user_key.to_vec());

            if value_type == ValueType::Value as u8 {
                results.push((
                    Bytes::copy_from_slice(user_key),
                    Bytes::copy_from_slice(value),
                ));
                if results.len() >= limit {
                    break;
                }
            }
        }

        results
    }
}

pub(crate) struct MemtableIterator {
    entries: Vec<(InternalKey, Bytes)>,
    position: usize,
}

impl Iterator for MemtableIterator {
    type Item = (InternalKey, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        if self.position < self.entries.len() {
            let entry = self.entries[self.position].clone();
            self.position += 1;
            Some(entry)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_put_get() {
        let memtable = Memtable::new(1024 * 1024);
        memtable.put(Bytes::from("key1"), 1, Bytes::from("value1"));

        match memtable.get(b"key1", 1) {
            LookupResult::Found(v) => assert_eq!(&v[..], b"value1"),
            _ => panic!("Expected Found"),
        }
    }

    #[test]
    fn test_memtable_get_not_found() {
        let memtable = Memtable::new(1024 * 1024);

        match memtable.get(b"key1", 1) {
            LookupResult::NotFound => {}
            _ => panic!("Expected NotFound"),
        }
    }

    #[test]
    fn test_memtable_delete() {
        let memtable = Memtable::new(1024 * 1024);
        memtable.put(Bytes::from("key1"), 1, Bytes::from("value1"));
        memtable.delete(Bytes::from("key1"), 2);

        match memtable.get(b"key1", 2) {
            LookupResult::Deleted => {}
            _ => panic!("Expected Deleted"),
        }

        match memtable.get(b"key1", 1) {
            LookupResult::Found(v) => assert_eq!(&v[..], b"value1"),
            _ => panic!("Expected Found at older sequence"),
        }
    }

    #[test]
    fn test_memtable_sequence_visibility() {
        let memtable = Memtable::new(1024 * 1024);
        memtable.put(Bytes::from("key1"), 5, Bytes::from("value5"));
        memtable.put(Bytes::from("key1"), 10, Bytes::from("value10"));

        match memtable.get(b"key1", 7) {
            LookupResult::Found(v) => assert_eq!(&v[..], b"value5"),
            _ => panic!("Expected value5"),
        }

        match memtable.get(b"key1", 15) {
            LookupResult::Found(v) => assert_eq!(&v[..], b"value10"),
            _ => panic!("Expected value10"),
        }

        match memtable.get(b"key1", 3) {
            LookupResult::NotFound => {}
            _ => panic!("Expected NotFound"),
        }
    }

    #[test]
    fn test_memtable_is_full() {
        let memtable = Memtable::new(100);
        assert!(!memtable.is_full());

        for i in 0..10 {
            memtable.put(
                Bytes::from(format!("key{}", i)),
                i as u64,
                Bytes::from("x".repeat(20)),
            );
        }
        assert!(memtable.is_full());
    }

    #[test]
    fn test_memtable_iterator() {
        let memtable = Memtable::new(1024 * 1024);
        memtable.put(Bytes::from("a"), 1, Bytes::from("va"));
        memtable.put(Bytes::from("b"), 1, Bytes::from("vb"));
        memtable.put(Bytes::from("c"), 1, Bytes::from("vc"));

        let entries: Vec<_> = memtable.iter().collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0.user_key.as_ref(), b"a");
        assert_eq!(entries[1].0.user_key.as_ref(), b"b");
        assert_eq!(entries[2].0.user_key.as_ref(), b"c");
    }

    #[test]
    fn test_memtable_len() {
        let memtable = Memtable::new(1024 * 1024);
        assert_eq!(memtable.len(), 0);
        memtable.put(Bytes::from("a"), 1, Bytes::from("va"));
        assert_eq!(memtable.len(), 1);
        memtable.put(Bytes::from("b"), 1, Bytes::from("vb"));
        assert_eq!(memtable.len(), 2);
    }

    #[test]
    fn test_memtable_approximate_size() {
        let memtable = Memtable::new(1024 * 1024);
        assert_eq!(memtable.approximate_size(), 0);
        memtable.put(Bytes::from("key"), 1, Bytes::from("value"));
        assert!(memtable.approximate_size() > 0);
    }

    #[test]
    fn test_memtable_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let memtable = Arc::new(Memtable::new(10 * 1024 * 1024));
        let mut handles = vec![];

        for t in 0..4 {
            let memtable = memtable.clone();
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    let key = format!("key-{}-{:04}", t, i);
                    let value = format!("value-{}-{:04}", t, i);
                    memtable.put(Bytes::from(key), (t * 1000 + i) as u64, Bytes::from(value));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(memtable.len(), 4000);

        for t in 0..4 {
            for i in 0..1000 {
                let key = format!("key-{}-{:04}", t, i);
                let seq = (t * 1000 + i) as u64;
                match memtable.get(key.as_bytes(), seq) {
                    LookupResult::Found(v) => {
                        let expected = format!("value-{}-{:04}", t, i);
                        assert_eq!(&v[..], expected.as_bytes());
                    }
                    _ => panic!("Expected Found for key {}", key),
                }
            }
        }
    }
}
