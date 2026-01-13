use crate::error::{Error, Result};
use crate::types::{decode_varint, encode_varint};
use bytes::{Bytes, BytesMut};

pub(crate) struct BlockBuilder {
    buffer: BytesMut,
    restarts: Vec<u32>,
    last_key: Vec<u8>,
    entry_count: usize,
    restart_interval: usize,
}

impl BlockBuilder {
    pub(crate) fn new(restart_interval: usize) -> Self {
        Self {
            buffer: BytesMut::new(),
            restarts: vec![0],
            last_key: Vec::new(),
            entry_count: 0,
            restart_interval,
        }
    }

    pub(crate) fn add(&mut self, key: &[u8], value: &[u8]) {
        let shared = if self.entry_count % self.restart_interval == 0 {
            if self.entry_count > 0 {
                self.restarts.push(self.buffer.len() as u32);
            }
            0
        } else {
            self.shared_prefix_len(key)
        };

        let non_shared = key.len() - shared;

        self.buffer.extend_from_slice(&encode_varint(shared as u64));
        self.buffer
            .extend_from_slice(&encode_varint(non_shared as u64));
        self.buffer
            .extend_from_slice(&encode_varint(value.len() as u64));
        self.buffer.extend_from_slice(&key[shared..]);
        self.buffer.extend_from_slice(value);

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.entry_count += 1;
    }

    fn shared_prefix_len(&self, key: &[u8]) -> usize {
        let limit = std::cmp::min(self.last_key.len(), key.len());
        let mut i = 0;
        while i < limit && self.last_key[i] == key[i] {
            i += 1;
        }
        i
    }

    pub(crate) fn finish(mut self) -> Bytes {
        for restart in &self.restarts[1..] {
            self.buffer.extend_from_slice(&restart.to_le_bytes());
        }
        self.buffer
            .extend_from_slice(&((self.restarts.len() - 1) as u32).to_le_bytes());

        let crc = crc32fast::hash(&self.buffer);
        self.buffer.extend_from_slice(&crc.to_le_bytes());

        self.buffer.freeze()
    }

    pub(crate) fn estimated_size(&self) -> usize {
        self.buffer.len() + self.restarts.len() * 4 + 8
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    #[allow(dead_code)]
    pub(crate) fn reset(&mut self) {
        self.buffer.clear();
        self.restarts.clear();
        self.restarts.push(0);
        self.last_key.clear();
        self.entry_count = 0;
    }

    pub(crate) fn last_key(&self) -> &[u8] {
        &self.last_key
    }
}

pub(crate) struct Block {
    data: Bytes,
    restart_offset: usize,
    restart_count: usize,
}

impl Block {
    pub(crate) fn new(data: Bytes) -> Result<Self> {
        if data.len() < 8 {
            return Err(Error::Corruption("Block too small".into()));
        }

        let crc_offset = data.len() - 4;
        let expected_crc = u32::from_le_bytes(data[crc_offset..].try_into().unwrap());
        let actual_crc = crc32fast::hash(&data[..crc_offset]);
        if expected_crc != actual_crc {
            return Err(Error::InvalidCrc {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        Ok(Self::new_unchecked(data))
    }

    pub(crate) fn new_unchecked(data: Bytes) -> Self {
        let crc_offset = data.len() - 4;
        let count_offset = crc_offset - 4;
        let restart_count =
            u32::from_le_bytes(data[count_offset..crc_offset].try_into().unwrap()) as usize;
        let restart_offset = count_offset - restart_count * 4;

        Self {
            data,
            restart_offset,
            restart_count,
        }
    }

    pub(crate) fn seek(&self, target: &[u8]) -> BlockIterator {
        let restart_idx = self.find_restart_point(target);
        let start_pos = self.get_restart(restart_idx);

        let mut iter = BlockIterator {
            data: self.data.clone(),
            restart_offset: self.restart_offset,
            pos: start_pos,
            current_key: Vec::with_capacity(64),
        };

        while let Some((key, _)) = iter.peek() {
            if key.as_ref() >= target {
                break;
            }
            iter.next();
        }

        iter
    }

    fn find_restart_point(&self, target: &[u8]) -> usize {
        if self.restart_count == 0 {
            return 0;
        }

        let mut left = 0;
        let mut right = self.restart_count;

        while left < right {
            let mid = left + (right - left) / 2;
            let restart_pos = self.get_restart(mid);

            if let Some(key) = self.decode_key_at(restart_pos) {
                if key.as_ref() < target {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            } else {
                break;
            }
        }

        if left > 0 {
            left - 1
        } else {
            0
        }
    }

    fn decode_key_at(&self, pos: usize) -> Option<Bytes> {
        if pos >= self.restart_offset {
            return None;
        }

        let (shared, bytes_read) = decode_varint(&self.data[pos..])?;
        let mut offset = pos + bytes_read;

        let (non_shared, bytes_read) = decode_varint(&self.data[offset..])?;
        offset += bytes_read;

        let (_value_len, bytes_read) = decode_varint(&self.data[offset..])?;
        offset += bytes_read;

        if shared != 0 {
            return None;
        }

        let key_end = offset + non_shared as usize;
        if key_end > self.data.len() {
            return None;
        }

        Some(self.data.slice(offset..key_end))
    }

    fn get_restart(&self, index: usize) -> usize {
        if index == 0 {
            return 0;
        }
        let offset = self.restart_offset + (index - 1) * 4;
        u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap()) as usize
    }

    #[allow(dead_code)]
    pub(crate) fn iter(&self) -> BlockIterator {
        BlockIterator {
            data: self.data.clone(),
            restart_offset: self.restart_offset,
            pos: 0,
            current_key: Vec::with_capacity(64),
        }
    }
}

pub(crate) struct BlockIterator {
    data: Bytes,
    restart_offset: usize,
    pos: usize,
    current_key: Vec<u8>,
}

impl BlockIterator {
    fn peek(&self) -> Option<(Bytes, Bytes)> {
        if self.pos >= self.restart_offset {
            return None;
        }

        let mut pos = self.pos;
        let mut current_key = self.current_key.clone();

        let (shared, bytes_read) = decode_varint(&self.data[pos..])?;
        pos += bytes_read;

        let (non_shared, bytes_read) = decode_varint(&self.data[pos..])?;
        pos += bytes_read;

        let (value_len, bytes_read) = decode_varint(&self.data[pos..])?;
        pos += bytes_read;

        current_key.truncate(shared as usize);
        let key_end = pos + non_shared as usize;
        current_key.extend_from_slice(&self.data[pos..key_end]);
        pos = key_end;

        let value_end = pos + value_len as usize;
        let value = self.data.slice(pos..value_end);

        Some((Bytes::from(current_key), value))
    }
}

impl Iterator for BlockIterator {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.restart_offset {
            return None;
        }

        let (shared, bytes_read) = decode_varint(&self.data[self.pos..])?;
        self.pos += bytes_read;

        let (non_shared, bytes_read) = decode_varint(&self.data[self.pos..])?;
        self.pos += bytes_read;

        let (value_len, bytes_read) = decode_varint(&self.data[self.pos..])?;
        self.pos += bytes_read;

        self.current_key.truncate(shared as usize);
        let key_end = self.pos + non_shared as usize;
        self.current_key
            .extend_from_slice(&self.data[self.pos..key_end]);
        self.pos = key_end;

        let value_end = self.pos + value_len as usize;
        let value = self.data.slice(self.pos..value_end);
        self.pos = value_end;

        Some((Bytes::from(self.current_key.clone()), value))
    }
}

impl BlockIterator {
    pub(crate) fn advance(&mut self) -> Option<(usize, usize)> {
        if self.pos >= self.restart_offset {
            return None;
        }

        let (shared, bytes_read) = decode_varint(&self.data[self.pos..])?;
        self.pos += bytes_read;

        let (non_shared, bytes_read) = decode_varint(&self.data[self.pos..])?;
        self.pos += bytes_read;

        let (value_len, bytes_read) = decode_varint(&self.data[self.pos..])?;
        self.pos += bytes_read;

        self.current_key.truncate(shared as usize);
        let key_end = self.pos + non_shared as usize;
        self.current_key
            .extend_from_slice(&self.data[self.pos..key_end]);
        self.pos = key_end;

        let value_start = self.pos;
        let value_end = self.pos + value_len as usize;
        self.pos = value_end;

        Some((value_start, value_end))
    }

    pub(crate) fn current_key(&self) -> &[u8] {
        &self.current_key
    }

    pub(crate) fn value_slice(&self, start: usize, end: usize) -> Bytes {
        self.data.slice(start..end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_builder_single_entry() {
        let mut builder = BlockBuilder::new(16);
        builder.add(b"key", b"value");
        let data = builder.finish();

        let block = Block::new(data).unwrap();
        let mut iter = block.iter();
        let (k, v) = iter.next().unwrap();
        assert_eq!(k.as_ref(), b"key");
        assert_eq!(v.as_ref(), b"value");
    }

    #[test]
    fn test_block_builder_multiple_entries() {
        let mut builder = BlockBuilder::new(16);
        builder.add(b"key1", b"value1");
        builder.add(b"key2", b"value2");
        builder.add(b"key3", b"value3");
        let data = builder.finish();

        let block = Block::new(data).unwrap();
        let entries: Vec<_> = block.iter().collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0.as_ref(), b"key1");
        assert_eq!(entries[1].0.as_ref(), b"key2");
        assert_eq!(entries[2].0.as_ref(), b"key3");
    }

    #[test]
    fn test_block_seek() {
        let mut builder = BlockBuilder::new(4);
        for i in 0..20 {
            builder.add(format!("key{:02}", i).as_bytes(), b"value");
        }
        let data = builder.finish();

        let block = Block::new(data).unwrap();

        let mut iter = block.seek(b"key10");
        let (k, _) = iter.next().unwrap();
        assert_eq!(k.as_ref(), b"key10");

        let mut iter = block.seek(b"key05");
        let (k, _) = iter.next().unwrap();
        assert_eq!(k.as_ref(), b"key05");

        let mut iter = block.seek(b"key00");
        let (k, _) = iter.next().unwrap();
        assert_eq!(k.as_ref(), b"key00");
    }

    #[test]
    fn test_block_seek_between_keys() {
        let mut builder = BlockBuilder::new(4);
        builder.add(b"key10", b"v1");
        builder.add(b"key20", b"v2");
        builder.add(b"key30", b"v3");
        let data = builder.finish();

        let block = Block::new(data).unwrap();

        let mut iter = block.seek(b"key15");
        let (k, _) = iter.next().unwrap();
        assert_eq!(k.as_ref(), b"key20");
    }

    #[test]
    fn test_block_prefix_compression() {
        let mut builder = BlockBuilder::new(16);
        builder.add(b"prefix_aaa", b"v1");
        builder.add(b"prefix_aab", b"v2");
        builder.add(b"prefix_aac", b"v3");
        let data = builder.finish();

        let block = Block::new(data).unwrap();
        let entries: Vec<_> = block.iter().collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0.as_ref(), b"prefix_aaa");
        assert_eq!(entries[1].0.as_ref(), b"prefix_aab");
        assert_eq!(entries[2].0.as_ref(), b"prefix_aac");
    }

    #[test]
    fn test_block_restart_points() {
        let mut builder = BlockBuilder::new(2);
        for i in 0..10 {
            builder.add(format!("key{:02}", i).as_bytes(), b"value");
        }
        let data = builder.finish();

        let block = Block::new(data).unwrap();
        let entries: Vec<_> = block.iter().collect();
        assert_eq!(entries.len(), 10);
    }

    #[test]
    fn test_block_invalid_crc() {
        let mut builder = BlockBuilder::new(16);
        builder.add(b"key", b"value");
        let mut data = builder.finish().to_vec();
        let last_idx = data.len() - 1;
        data[last_idx] ^= 0xff;

        assert!(Block::new(Bytes::from(data)).is_err());
    }

    #[test]
    fn test_block_builder_estimated_size() {
        let mut builder = BlockBuilder::new(16);
        assert!(builder.is_empty());
        builder.add(b"key", b"value");
        assert!(!builder.is_empty());
        assert!(builder.estimated_size() > 0);
    }

    #[test]
    fn test_block_builder_reset() {
        let mut builder = BlockBuilder::new(16);
        builder.add(b"key", b"value");
        builder.reset();
        assert!(builder.is_empty());
    }

    #[test]
    fn test_block_builder_last_key() {
        let mut builder = BlockBuilder::new(16);
        builder.add(b"key1", b"value1");
        assert_eq!(builder.last_key(), b"key1");
        builder.add(b"key2", b"value2");
        assert_eq!(builder.last_key(), b"key2");
    }

    #[test]
    fn test_block_too_small() {
        let data = Bytes::from(vec![0u8; 4]);
        assert!(Block::new(data).is_err());
    }
}
