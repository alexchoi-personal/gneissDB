use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::error::Result;
use crate::sstable::SstableIterator;
use crate::types::{InternalKey, ValueType};

struct HeapEntry {
    user_key: Bytes,
    sequence: u64,
    value_type: u8,
    value: Bytes,
    source_idx: usize,
}

impl Eq for HeapEntry {}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.user_key == other.user_key && self.sequence == other.sequence
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.user_key.cmp(&other.user_key) {
            Ordering::Equal => other.sequence.cmp(&self.sequence),
            ord => ord,
        }
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct Reverse<T>(T);

impl<T: Ord> Ord for Reverse<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.0.cmp(&self.0)
    }
}

impl<T: Ord> PartialOrd for Reverse<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Eq> Eq for Reverse<T> {}

impl<T: PartialEq> PartialEq for Reverse<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

struct MemtableSource {
    entries: Vec<(InternalKey, Bytes)>,
    position: usize,
    end_key: Bytes,
    max_sequence: u64,
}

impl MemtableSource {
    fn peek(&self) -> Option<&(InternalKey, Bytes)> {
        if self.position >= self.entries.len() {
            return None;
        }
        let (key, _) = &self.entries[self.position];
        if key.user_key.as_ref() >= self.end_key.as_ref() {
            return None;
        }
        if key.sequence <= self.max_sequence {
            return Some(&self.entries[self.position]);
        }
        None
    }

    fn advance(&mut self) {
        self.position += 1;
        while self.position < self.entries.len() {
            let (key, _) = &self.entries[self.position];
            if key.user_key.as_ref() >= self.end_key.as_ref() {
                break;
            }
            if key.sequence <= self.max_sequence {
                break;
            }
            self.position += 1;
        }
    }
}

pub(crate) struct MergeIterator {
    heap: BinaryHeap<Reverse<HeapEntry>>,
    memtable_sources: Vec<MemtableSource>,
    sstable_sources: Vec<SstableIterator>,
    sstable_current: Vec<Option<(Bytes, Bytes)>>,
    end_key: Bytes,
    max_sequence: u64,
    last_user_key: Option<Bytes>,
}

impl MergeIterator {
    pub(crate) fn new(end_key: Bytes, max_sequence: u64) -> Self {
        Self {
            heap: BinaryHeap::new(),
            memtable_sources: Vec::new(),
            sstable_sources: Vec::new(),
            sstable_current: Vec::new(),
            end_key,
            max_sequence,
            last_user_key: None,
        }
    }

    pub(crate) fn add_memtable_entries(
        &mut self,
        entries: Vec<(InternalKey, Bytes)>,
        start_key: &[u8],
        source_idx: usize,
    ) {
        let start_pos = entries
            .iter()
            .position(|(k, _)| k.user_key.as_ref() >= start_key)
            .unwrap_or(entries.len());

        let source = MemtableSource {
            entries,
            position: start_pos,
            end_key: self.end_key.clone(),
            max_sequence: self.max_sequence,
        };

        if let Some((key, value)) = source.peek().cloned() {
            self.heap.push(Reverse(HeapEntry {
                user_key: key.user_key,
                sequence: key.sequence,
                value_type: key.value_type as u8,
                value,
                source_idx,
            }));
        }

        self.memtable_sources.push(source);
    }

    pub(crate) async fn add_sstable(
        &mut self,
        mut iter: SstableIterator,
        source_idx: usize,
    ) -> Result<()> {
        if let Some((key, value)) = iter.next().await? {
            if key.len() >= 9 {
                let user_key = Bytes::copy_from_slice(&key[..key.len() - 9]);
                if user_key.as_ref() < self.end_key.as_ref() {
                    let seq_bytes: [u8; 8] = key[key.len() - 9..key.len() - 1]
                        .try_into()
                        .unwrap_or([0; 8]);
                    let key_seq = !u64::from_be_bytes(seq_bytes);

                    if key_seq <= self.max_sequence {
                        let value_type = key[key.len() - 1];
                        self.heap.push(Reverse(HeapEntry {
                            user_key,
                            sequence: key_seq,
                            value_type,
                            value,
                            source_idx,
                        }));
                    }

                    self.sstable_current.push(None);
                    self.sstable_sources.push(iter);
                    return Ok(());
                }
            }
        }

        self.sstable_current.push(None);
        self.sstable_sources.push(iter);
        Ok(())
    }

    pub(crate) async fn next(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        loop {
            let entry = match self.heap.pop() {
                Some(Reverse(e)) => e,
                None => return Ok(None),
            };

            let source_idx = entry.source_idx;

            if source_idx < self.memtable_sources.len() {
                let source = &mut self.memtable_sources[source_idx];
                source.advance();
                if let Some((key, value)) = source.peek().cloned() {
                    self.heap.push(Reverse(HeapEntry {
                        user_key: key.user_key,
                        sequence: key.sequence,
                        value_type: key.value_type as u8,
                        value,
                        source_idx,
                    }));
                }
            } else {
                let sst_idx = source_idx - self.memtable_sources.len();
                if sst_idx < self.sstable_sources.len() {
                    let iter = &mut self.sstable_sources[sst_idx];
                    while let Some((key, value)) = iter.next().await? {
                        if key.len() < 9 {
                            continue;
                        }
                        let user_key = Bytes::copy_from_slice(&key[..key.len() - 9]);
                        if user_key.as_ref() >= self.end_key.as_ref() {
                            break;
                        }
                        let seq_bytes: [u8; 8] = key[key.len() - 9..key.len() - 1]
                            .try_into()
                            .unwrap_or([0; 8]);
                        let key_seq = !u64::from_be_bytes(seq_bytes);
                        if key_seq > self.max_sequence {
                            continue;
                        }
                        let value_type = key[key.len() - 1];
                        self.heap.push(Reverse(HeapEntry {
                            user_key,
                            sequence: key_seq,
                            value_type,
                            value,
                            source_idx,
                        }));
                        break;
                    }
                }
            }

            if let Some(ref last) = self.last_user_key {
                if last == &entry.user_key {
                    continue;
                }
            }

            self.last_user_key = Some(entry.user_key.clone());

            if entry.value_type == ValueType::Value as u8 {
                return Ok(Some((entry.user_key, entry.value)));
            }
        }
    }
}
