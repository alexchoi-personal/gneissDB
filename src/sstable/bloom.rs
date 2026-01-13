use bytes::Bytes;

pub(crate) struct BloomFilter {
    bits: Vec<u8>,
    k: usize,
}

impl BloomFilter {
    pub(crate) fn new(bits_per_key: usize, num_keys: usize) -> Self {
        let bits = std::cmp::max(64, num_keys * bits_per_key);
        let k = ((bits_per_key as f64 * 0.69) as usize).clamp(1, 30);
        Self {
            bits: vec![0u8; bits.div_ceil(8)],
            k,
        }
    }

    pub(crate) fn from_bytes(data: Bytes) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        let k = data[data.len() - 1] as usize;
        if k > 30 {
            return None;
        }
        let bits = data[..data.len() - 1].to_vec();
        Some(Self { bits, k })
    }

    pub(crate) fn add(&mut self, key: &[u8]) {
        let mut h = Self::bloom_hash(key);
        let delta = h.rotate_left(15);
        let bits_len = self.bits.len() * 8;
        for _ in 0..self.k {
            let bit_pos = (h as usize) % bits_len;
            self.bits[bit_pos / 8] |= 1 << (bit_pos % 8);
            h = h.wrapping_add(delta);
        }
    }

    pub(crate) fn may_contain(&self, key: &[u8]) -> bool {
        let mut h = Self::bloom_hash(key);
        let delta = h.rotate_left(15);
        let bits_len = self.bits.len() * 8;
        for _ in 0..self.k {
            let bit_pos = (h as usize) % bits_len;
            if self.bits[bit_pos / 8] & (1 << (bit_pos % 8)) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }

    pub(crate) fn encode(&self) -> Bytes {
        let mut result = self.bits.clone();
        result.push(self.k as u8);
        Bytes::from(result)
    }

    fn bloom_hash(key: &[u8]) -> u32 {
        let mut h: u32 = 0;
        for &b in key {
            h = h.wrapping_mul(0x5bd1e995).wrapping_add(b as u32);
            h ^= h >> 15;
        }
        h
    }

    #[allow(dead_code)]
    pub(crate) fn size(&self) -> usize {
        self.bits.len() + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut filter = BloomFilter::new(10, 100);
        filter.add(b"hello");
        filter.add(b"world");

        assert!(filter.may_contain(b"hello"));
        assert!(filter.may_contain(b"world"));
    }

    #[test]
    fn test_bloom_filter_false_positives() {
        let mut filter = BloomFilter::new(10, 1000);
        for i in 0..1000 {
            filter.add(format!("key{}", i).as_bytes());
        }

        for i in 0..1000 {
            assert!(filter.may_contain(format!("key{}", i).as_bytes()));
        }

        let mut false_positives = 0;
        for i in 1000..2000 {
            if filter.may_contain(format!("key{}", i).as_bytes()) {
                false_positives += 1;
            }
        }
        assert!(false_positives < 20);
    }

    #[test]
    fn test_bloom_filter_encode_decode() {
        let mut filter = BloomFilter::new(10, 100);
        filter.add(b"hello");
        filter.add(b"world");

        let encoded = filter.encode();
        let decoded = BloomFilter::from_bytes(encoded).unwrap();

        assert!(decoded.may_contain(b"hello"));
        assert!(decoded.may_contain(b"world"));
    }

    #[test]
    fn test_bloom_filter_empty() {
        let filter = BloomFilter::new(10, 0);
        assert!(!filter.may_contain(b"anything"));
    }

    #[test]
    fn test_bloom_filter_size() {
        let filter = BloomFilter::new(10, 100);
        assert!(filter.size() > 0);
    }

    #[test]
    fn test_bloom_filter_from_empty_bytes() {
        assert!(BloomFilter::from_bytes(Bytes::new()).is_none());
    }

    #[test]
    fn test_bloom_filter_from_invalid_k() {
        let data = Bytes::from(vec![0u8, 31]);
        assert!(BloomFilter::from_bytes(data).is_none());
    }
}
