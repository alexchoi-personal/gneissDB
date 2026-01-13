use bytes::Bytes;
use std::cmp::Ordering;

pub(crate) type SequenceNumber = u64;
#[allow(dead_code)]
pub(crate) type UserKey = Bytes;
#[allow(dead_code)]
pub(crate) type UserValue = Bytes;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[repr(u8)]
pub(crate) enum ValueType {
    Deletion = 0,
    Value = 1,
}

impl ValueType {
    pub(crate) fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(ValueType::Deletion),
            1 => Some(ValueType::Value),
            _ => None,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct InternalKey {
    pub(crate) user_key: Bytes,
    pub(crate) sequence: SequenceNumber,
    pub(crate) value_type: ValueType,
}

impl InternalKey {
    #[allow(dead_code)]
    pub(crate) fn new(user_key: Bytes, sequence: SequenceNumber, value_type: ValueType) -> Self {
        Self {
            user_key,
            sequence,
            value_type,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn new_for_seek(user_key: Bytes, sequence: SequenceNumber) -> Self {
        Self::new(user_key, sequence, ValueType::Value)
    }

    pub(crate) fn encode(&self) -> Bytes {
        let mut buf = Vec::with_capacity(self.user_key.len() + 9);
        buf.extend_from_slice(&self.user_key);
        let inverted_seq = !self.sequence;
        buf.extend_from_slice(&inverted_seq.to_be_bytes());
        buf.push(self.value_type as u8);
        Bytes::from(buf)
    }

    #[allow(dead_code)]
    pub(crate) fn encode_to(&self, buf: &mut [u8]) -> usize {
        let len = self.user_key.len();
        buf[..len].copy_from_slice(&self.user_key);
        let inverted_seq = !self.sequence;
        buf[len..len + 8].copy_from_slice(&inverted_seq.to_be_bytes());
        buf[len + 8] = self.value_type as u8;
        len + 9
    }

    pub(crate) fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 9 {
            return None;
        }
        let user_key = Bytes::copy_from_slice(&data[..data.len() - 9]);
        let inverted_seq =
            u64::from_be_bytes(data[data.len() - 9..data.len() - 1].try_into().ok()?);
        let sequence = !inverted_seq;
        let value_type = ValueType::from_u8(data[data.len() - 1])?;
        Some(Self {
            user_key,
            sequence,
            value_type,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn encoded_len(&self) -> usize {
        self.user_key.len() + 9
    }
}

pub(crate) struct ParsedInternalKey<'a> {
    pub(crate) user_key: &'a [u8],
    pub(crate) sequence: SequenceNumber,
    pub(crate) value_type: ValueType,
}

impl<'a> ParsedInternalKey<'a> {
    pub(crate) fn parse(data: &'a [u8]) -> Option<Self> {
        if data.len() < 9 {
            return None;
        }
        let user_key = &data[..data.len() - 9];
        let inverted_seq =
            u64::from_be_bytes(data[data.len() - 9..data.len() - 1].try_into().ok()?);
        let sequence = !inverted_seq;
        let value_type = ValueType::from_u8(data[data.len() - 1])?;
        Some(Self {
            user_key,
            sequence,
            value_type,
        })
    }
}

pub(crate) fn encode_seek_key(user_key: &[u8], sequence: SequenceNumber, buf: &mut [u8]) -> usize {
    let len = user_key.len();
    buf[..len].copy_from_slice(user_key);
    let inverted_seq = !sequence;
    buf[len..len + 8].copy_from_slice(&inverted_seq.to_be_bytes());
    buf[len + 8] = ValueType::Value as u8;
    len + 9
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.user_key.cmp(&other.user_key) {
            Ordering::Equal => other.sequence.cmp(&self.sequence),
            other_ord => other_ord,
        }
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) fn encode_varint_to_vec(mut value: u64, buf: &mut Vec<u8>) {
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

pub(crate) fn encode_varint(value: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(10);
    encode_varint_to_vec(value, &mut buf);
    buf
}

pub(crate) fn decode_varint(data: &[u8]) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;
    for (i, &byte) in data.iter().enumerate() {
        if shift > 63 {
            return None;
        }
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((result, i + 1));
        }
        shift += 7;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_type_from_u8() {
        assert_eq!(ValueType::from_u8(0), Some(ValueType::Deletion));
        assert_eq!(ValueType::from_u8(1), Some(ValueType::Value));
        assert_eq!(ValueType::from_u8(2), None);
    }

    #[test]
    fn test_internal_key_encode_decode() {
        let key = InternalKey::new(Bytes::from("hello"), 12345, ValueType::Value);
        let encoded = key.encode();
        let decoded = InternalKey::decode(&encoded).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn test_internal_key_ordering() {
        let k1 = InternalKey::new(Bytes::from("a"), 100, ValueType::Value);
        let k2 = InternalKey::new(Bytes::from("a"), 50, ValueType::Value);
        let k3 = InternalKey::new(Bytes::from("b"), 100, ValueType::Value);

        assert!(k1 < k2);
        assert!(k2 < k3);
        assert!(k1 < k3);
    }

    #[test]
    fn test_varint_encode_decode() {
        for value in [0u64, 1, 127, 128, 16383, 16384, u64::MAX] {
            let buf = encode_varint(value);
            let (decoded, len) = decode_varint(&buf).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(len, buf.len());
        }
    }

    #[test]
    fn test_varint_encode_to_vec() {
        let mut buf = Vec::new();
        encode_varint_to_vec(128, &mut buf);
        assert_eq!(buf, vec![0x80, 0x01]);
    }

    #[test]
    fn test_internal_key_decode_too_short() {
        assert!(InternalKey::decode(&[0, 1, 2, 3, 4, 5, 6, 7]).is_none());
    }

    #[test]
    fn test_varint_decode_overflow() {
        let bad_data = [0xff; 11];
        assert!(decode_varint(&bad_data).is_none());
    }

    #[test]
    fn test_internal_key_encoded_len() {
        let key = InternalKey::new(Bytes::from("test"), 1, ValueType::Value);
        assert_eq!(key.encoded_len(), 13);
    }

    #[test]
    fn test_internal_key_new_for_seek() {
        let key = InternalKey::new_for_seek(Bytes::from("test"), 100);
        assert_eq!(key.value_type, ValueType::Value);
        assert_eq!(key.sequence, 100);
    }
}
