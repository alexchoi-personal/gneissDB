use crate::error::{Error, Result};
use bytes::Bytes;

#[derive(Clone, Debug)]
pub(crate) struct IndexEntry {
    pub(crate) last_key: Bytes,
    pub(crate) offset: u64,
    pub(crate) size: u32,
}

pub(crate) struct IndexBlock {
    entries: Vec<IndexEntry>,
}

impl IndexBlock {
    pub(crate) fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub(crate) fn add(&mut self, last_key: Bytes, offset: u64, size: u32) {
        self.entries.push(IndexEntry {
            last_key,
            offset,
            size,
        });
    }

    pub(crate) fn find_block(&self, key: &[u8]) -> Option<&IndexEntry> {
        if self.entries.is_empty() {
            return None;
        }

        let idx = self
            .entries
            .partition_point(|entry| entry.last_key.as_ref() < key);

        if idx < self.entries.len() {
            Some(&self.entries[idx])
        } else {
            self.entries.last()
        }
    }

    pub(crate) fn encode(&self) -> Bytes {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());

        for entry in &self.entries {
            buf.extend_from_slice(&(entry.last_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&entry.last_key);
            buf.extend_from_slice(&entry.offset.to_le_bytes());
            buf.extend_from_slice(&entry.size.to_le_bytes());
        }

        Bytes::from(buf)
    }

    pub(crate) fn decode(data: Bytes) -> Result<Self> {
        if data.len() < 4 {
            return Err(Error::Corruption("Index block too small".into()));
        }

        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut pos = 4;
        let mut entries = Vec::with_capacity(count);

        for _ in 0..count {
            if pos + 4 > data.len() {
                return Err(Error::Corruption("Index entry truncated".into()));
            }
            let key_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + key_len + 12 > data.len() {
                return Err(Error::Corruption("Index entry truncated".into()));
            }
            let last_key = data.slice(pos..pos + key_len);
            pos += key_len;

            let offset = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;

            let size = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;

            entries.push(IndexEntry {
                last_key,
                offset,
                size,
            });
        }

        Ok(Self { entries })
    }

    #[allow(dead_code)]
    pub(crate) fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_block_add_find() {
        let mut index = IndexBlock::new();
        index.add(Bytes::from("key1"), 0, 100);
        index.add(Bytes::from("key5"), 100, 100);
        index.add(Bytes::from("key9"), 200, 100);

        let entry = index.find_block(b"key1").unwrap();
        assert_eq!(entry.offset, 0);

        let entry = index.find_block(b"key3").unwrap();
        assert_eq!(entry.offset, 100);

        let entry = index.find_block(b"key7").unwrap();
        assert_eq!(entry.offset, 200);
    }

    #[test]
    fn test_index_block_encode_decode() {
        let mut index = IndexBlock::new();
        index.add(Bytes::from("key1"), 0, 100);
        index.add(Bytes::from("key5"), 100, 200);
        index.add(Bytes::from("key9"), 300, 300);

        let encoded = index.encode();
        let decoded = IndexBlock::decode(encoded).unwrap();

        assert_eq!(decoded.entries().len(), 3);
        assert_eq!(decoded.entries()[0].last_key.as_ref(), b"key1");
        assert_eq!(decoded.entries()[1].offset, 100);
        assert_eq!(decoded.entries()[2].size, 300);
    }

    #[test]
    fn test_index_block_empty() {
        let index = IndexBlock::new();
        assert!(index.is_empty());
        assert!(index.find_block(b"key").is_none());
    }

    #[test]
    fn test_index_block_decode_too_small() {
        let data = Bytes::from(vec![0u8, 1, 2]);
        assert!(IndexBlock::decode(data).is_err());
    }

    #[test]
    fn test_index_block_entries() {
        let mut index = IndexBlock::new();
        index.add(Bytes::from("key1"), 0, 100);
        assert_eq!(index.entries().len(), 1);
    }

    #[test]
    fn test_index_block_binary_search() {
        let mut index = IndexBlock::new();
        for i in 0..100 {
            index.add(
                Bytes::from(format!("key{:03}", i * 10)),
                i as u64 * 1000,
                100,
            );
        }

        let entry = index.find_block(b"key000").unwrap();
        assert_eq!(entry.offset, 0);

        let entry = index.find_block(b"key005").unwrap();
        assert_eq!(entry.offset, 1000);

        let entry = index.find_block(b"key500").unwrap();
        assert_eq!(entry.offset, 50000);

        let entry = index.find_block(b"key999").unwrap();
        assert_eq!(entry.offset, 99000);
    }
}
