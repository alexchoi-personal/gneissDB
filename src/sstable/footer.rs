use crate::error::{Error, Result};
use bytes::Bytes;

pub(crate) const FOOTER_SIZE: usize = 52;
pub(crate) const MAGIC: u64 = 0x524F434B5344425F;

pub(crate) struct Footer {
    pub(crate) index_offset: u64,
    pub(crate) index_size: u32,
    pub(crate) bloom_offset: u64,
    pub(crate) bloom_size: u32,
    pub(crate) min_key: Bytes,
    pub(crate) max_key: Bytes,
}

impl Footer {
    pub(crate) fn encode(&self) -> Bytes {
        let mut buf = Vec::with_capacity(FOOTER_SIZE);

        buf.extend_from_slice(&self.index_offset.to_le_bytes());
        buf.extend_from_slice(&self.index_size.to_le_bytes());
        buf.extend_from_slice(&self.bloom_offset.to_le_bytes());
        buf.extend_from_slice(&self.bloom_size.to_le_bytes());

        buf.extend_from_slice(&(self.min_key.len() as u16).to_le_bytes());
        let min_key_padded = &self.min_key[..std::cmp::min(self.min_key.len(), 8)];
        buf.extend_from_slice(min_key_padded);
        buf.resize(buf.len() + 8 - min_key_padded.len(), 0);

        buf.extend_from_slice(&(self.max_key.len() as u16).to_le_bytes());
        let max_key_padded = &self.max_key[..std::cmp::min(self.max_key.len(), 8)];
        buf.extend_from_slice(max_key_padded);
        buf.resize(buf.len() + 8 - max_key_padded.len(), 0);

        buf.extend_from_slice(&MAGIC.to_le_bytes());

        Bytes::from(buf)
    }

    pub(crate) fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < FOOTER_SIZE {
            return Err(Error::Corruption("Footer too small".into()));
        }

        let offset = data.len() - FOOTER_SIZE;
        let data = &data[offset..];

        let magic = u64::from_le_bytes(data[FOOTER_SIZE - 8..].try_into().unwrap());
        if magic != MAGIC {
            return Err(Error::InvalidMagic);
        }

        let index_offset = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let index_size = u32::from_le_bytes(data[8..12].try_into().unwrap());
        let bloom_offset = u64::from_le_bytes(data[12..20].try_into().unwrap());
        let bloom_size = u32::from_le_bytes(data[20..24].try_into().unwrap());

        let min_key_len = u16::from_le_bytes(data[24..26].try_into().unwrap()) as usize;
        let min_key = Bytes::copy_from_slice(&data[26..26 + std::cmp::min(min_key_len, 8)]);

        let max_key_len = u16::from_le_bytes(data[34..36].try_into().unwrap()) as usize;
        let max_key = Bytes::copy_from_slice(&data[36..36 + std::cmp::min(max_key_len, 8)]);

        Ok(Self {
            index_offset,
            index_size,
            bloom_offset,
            bloom_size,
            min_key,
            max_key,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn may_contain_key(&self, key: &[u8]) -> bool {
        let key_prefix = &key[..std::cmp::min(key.len(), 8)];
        let min_prefix = &self.min_key[..std::cmp::min(self.min_key.len(), 8)];
        let max_prefix = &self.max_key[..std::cmp::min(self.max_key.len(), 8)];

        key_prefix >= min_prefix && key_prefix <= max_prefix
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_footer_encode_decode() {
        let footer = Footer {
            index_offset: 1000,
            index_size: 200,
            bloom_offset: 500,
            bloom_size: 100,
            min_key: Bytes::from("aaa"),
            max_key: Bytes::from("zzz"),
        };

        let encoded = footer.encode();
        assert_eq!(encoded.len(), FOOTER_SIZE);

        let decoded = Footer::decode(&encoded).unwrap();
        assert_eq!(decoded.index_offset, 1000);
        assert_eq!(decoded.index_size, 200);
        assert_eq!(decoded.bloom_offset, 500);
        assert_eq!(decoded.bloom_size, 100);
    }

    #[test]
    fn test_footer_invalid_magic() {
        let mut data = vec![0u8; FOOTER_SIZE];
        data[FOOTER_SIZE - 8..].copy_from_slice(&[0u8; 8]);
        assert!(Footer::decode(&data).is_err());
    }

    #[test]
    fn test_footer_too_small() {
        let data = vec![0u8; FOOTER_SIZE - 1];
        assert!(Footer::decode(&data).is_err());
    }

    #[test]
    fn test_footer_may_contain_key() {
        let footer = Footer {
            index_offset: 0,
            index_size: 0,
            bloom_offset: 0,
            bloom_size: 0,
            min_key: Bytes::from("bbb"),
            max_key: Bytes::from("yyy"),
        };

        assert!(!footer.may_contain_key(b"aaa"));
        assert!(footer.may_contain_key(b"bbb"));
        assert!(footer.may_contain_key(b"mmm"));
        assert!(footer.may_contain_key(b"yyy"));
        assert!(!footer.may_contain_key(b"zzz"));
    }

    #[test]
    fn test_footer_long_keys() {
        let footer = Footer {
            index_offset: 0,
            index_size: 0,
            bloom_offset: 0,
            bloom_size: 0,
            min_key: Bytes::from("verylongkey1"),
            max_key: Bytes::from("verylongkey9"),
        };

        let encoded = footer.encode();
        let decoded = Footer::decode(&encoded).unwrap();
        assert_eq!(decoded.min_key.len(), 8);
        assert_eq!(decoded.max_key.len(), 8);
    }
}
