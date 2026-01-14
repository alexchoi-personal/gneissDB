use crate::memtable::LookupResult;
use crate::types::{InternalKey, SequenceNumber, ValueType};
use bytes::{BufMut, Bytes, BytesMut};
use crossbeam_skiplist::SkipMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) struct Memtable {
    skiplist: SkipMap<Bytes, Bytes>,
    size: AtomicUsize,
    max_size: usize,
}

impl Memtable {
    pub(crate) fn new(max_size: usize) -> Self {
        Self {
            skiplist: SkipMap::new(),
            size: AtomicUsize::new(0),
            max_size,
        }
    }

    pub(crate) fn put(&self, user_key: Bytes, sequence: SequenceNumber, value: Bytes) {
        let encoded_key = Self::encode_key(&user_key, sequence, ValueType::Value);
        let entry_size = encoded_key.len() + value.len();
        self.skiplist.insert(encoded_key, value);
        self.size.fetch_add(entry_size, Ordering::Relaxed);
    }

    pub(crate) fn delete(&self, user_key: Bytes, sequence: SequenceNumber) {
        let encoded_key = Self::encode_key(&user_key, sequence, ValueType::Deletion);
        let entry_size = encoded_key.len();
        self.skiplist.insert(encoded_key, Bytes::new());
        self.size.fetch_add(entry_size, Ordering::Relaxed);
    }

    #[inline]
    fn encode_key(user_key: &[u8], sequence: SequenceNumber, value_type: ValueType) -> Bytes {
        let total_len = user_key.len() + 9;
        let mut buf = BytesMut::with_capacity(total_len);
        buf.extend_from_slice(user_key);
        buf.extend_from_slice(&(!sequence).to_be_bytes());
        buf.put_u8(value_type as u8);
        buf.freeze()
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

        let mut total_size = 0;
        for (user_key, sequence, value) in ops {
            let value_type = if value.is_some() {
                ValueType::Value
            } else {
                ValueType::Deletion
            };
            let encoded_key = Self::encode_key(user_key, *sequence, value_type);
            let val = value.clone().unwrap_or_else(Bytes::new);
            total_size += encoded_key.len() + val.len();
            self.skiplist.insert(encoded_key, val);
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
        let encoded_key = Bytes::copy_from_slice(key_buf);
        let entry_size = encoded_key.len() + value.len();
        self.skiplist.insert(encoded_key, value);
        entry_size
    }

    pub(crate) fn delete_inline(
        &self,
        user_key: &[u8],
        sequence: SequenceNumber,
        key_buf: &mut Vec<u8>,
    ) -> usize {
        Self::encode_key_into(key_buf, user_key, sequence, ValueType::Deletion);
        let encoded_key = Bytes::copy_from_slice(key_buf);
        let entry_size = encoded_key.len();
        self.skiplist.insert(encoded_key, Bytes::new());
        entry_size
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

        let mut entry = self.skiplist.lower_bound(Bound::Included(&seek_key));

        while let Some(e) = entry {
            let encoded_key = e.key();
            let Some(key) = InternalKey::decode(encoded_key) else {
                entry = e.next();
                continue;
            };

            if key.user_key.as_ref() != user_key {
                break;
            }

            if key.sequence <= sequence {
                return match key.value_type {
                    ValueType::Value => LookupResult::Found(e.value().clone()),
                    ValueType::Deletion => LookupResult::Deleted,
                };
            }

            entry = e.next();
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
            .filter_map(|e| InternalKey::decode(e.key()).map(|key| (key, e.value().clone())))
            .collect();
        MemtableIterator {
            entries,
            position: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.skiplist.len()
    }

    pub(crate) fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        sequence: SequenceNumber,
        limit: usize,
    ) -> Vec<(Bytes, Bytes)> {
        let mut results = Vec::with_capacity(limit.min(128));
        let start_key = Bytes::copy_from_slice(start);
        let mut last_user_key_len = 0usize;
        let mut last_user_key_buf = [0u8; 256];

        for entry in self.skiplist.range(start_key..) {
            let encoded_key = entry.key();
            let key_len = encoded_key.len();
            if key_len < 9 {
                continue;
            }

            let user_key_len = key_len - 9;
            let user_key = &encoded_key[..user_key_len];

            if user_key >= end {
                break;
            }

            if last_user_key_len > 0
                && last_user_key_len == user_key_len
                && &last_user_key_buf[..last_user_key_len] == user_key
            {
                continue;
            }

            let seq_bytes: [u8; 8] = encoded_key[user_key_len..user_key_len + 8]
                .try_into()
                .unwrap_or([0; 8]);
            let key_sequence = !u64::from_be_bytes(seq_bytes);

            if key_sequence > sequence {
                continue;
            }

            let value_type = encoded_key[key_len - 1];

            if user_key_len <= 256 {
                last_user_key_buf[..user_key_len].copy_from_slice(user_key);
                last_user_key_len = user_key_len;
            } else {
                last_user_key_len = 0;
            }

            if value_type == ValueType::Value as u8 {
                results.push((
                    encoded_key[..user_key_len].to_vec().into(),
                    entry.value().clone(),
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
