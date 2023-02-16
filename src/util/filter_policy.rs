use super::hash;

pub trait FilterPolicy {
    fn name(&self) -> &str;
    fn create_filter(&self, keys: &[&[u8]], dst: &mut Vec<u8>);
    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool;
}

pub struct BloomFilterPolicy {
    bits_per_key: usize,
    k: usize,
}

impl BloomFilterPolicy {
    pub fn new(bits_per_key: usize) -> Self {
        let k = (bits_per_key as f32 * 0.69) as usize;
        let k = k.clamp(1, 30);
        Self { bits_per_key, k }
    }

    pub fn bloom_hash(key: &[u8]) -> u32 {
        hash(key, 0xbc9f1d34)
    }
}

impl FilterPolicy for BloomFilterPolicy {
    fn name(&self) -> &str {
        "leveldb.BuiltinBloomFilter2"
    }

    fn create_filter(&self, keys: &[&[u8]], dst: &mut Vec<u8>) {
        let bits = keys.len() * self.bits_per_key;
        let bits = bits.max(64);
        let bytes = (bits + 7) / 8;
        let bits = bytes * 8;
        let init_size = dst.len();
        dst.resize(init_size + bytes, 0);
        dst.push(self.k as u8);
        let array = &mut dst[init_size..];
        for key in keys {
            let mut h = BloomFilterPolicy::bloom_hash(key);
            let delta = (h >> 17) | (h << 15);
            for _ in 0..self.k {
                let bitpos = h as usize % bits;
                array[bitpos / 8] |= 1 << (bitpos % 8);
                h = h.wrapping_add(delta);
            }
        }
    }

    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        let len = filter.len();
        if len < 2 {
            return false;
        }
        let bits = (len - 1) * 8;
        let k = filter[len - 1];
        if k > 30 {
            return true;
        }

        let mut h = BloomFilterPolicy::bloom_hash(key);
        let delta = (h >> 17) | (h << 15);
        for _ in 0..k {
            let bitpos = h as usize % bits;
            if filter[bitpos / 8] & (1 << bitpos % 8) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use std::{iter, mem::size_of, slice};

    use super::{BloomFilterPolicy, FilterPolicy};
    use crate::util::encode_fixed32;

    struct BloomTest {
        policy: Box<dyn FilterPolicy>,
        filter: Vec<u8>,
        keys: Vec<Vec<u8>>,
    }

    impl BloomTest {
        pub fn new(policy: Box<dyn FilterPolicy>) -> Self {
            Self {
                policy,
                filter: vec![],
                keys: vec![],
            }
        }

        pub fn reset(&mut self) {
            self.keys.clear();
            self.filter.clear();
        }

        pub fn add(&mut self, s: &[u8]) {
            self.keys.push(s.to_owned());
        }

        pub fn build(&mut self) {
            let mut key_slice = vec![];
            for key in &self.keys {
                key_slice.push(key.as_slice());
            }
            self.filter.clear();
            self.policy.create_filter(&key_slice, &mut self.filter);
            self.keys.clear();
        }

        pub fn filter_size(&self) -> usize {
            self.filter.len()
        }

        pub fn matches(&mut self, s: &[u8]) -> bool {
            if !self.keys.is_empty() {
                self.build();
            }
            self.policy.key_may_match(s, &self.filter)
        }

        pub fn false_positive_rate(&mut self) -> f64 {
            let mut buf = [0; size_of::<u32>()];
            let mut result = 0;
            for i in 0..10000 {
                if self.matches(&key(i + 1000000000, &mut buf)) {
                    result += 1;
                }
            }
            result as f64 / 10000f64
        }
    }

    fn key(i: u32, dst: &mut [u8]) -> &[u8] {
        encode_fixed32(dst, i);
        unsafe { slice::from_raw_parts(dst.as_ptr(), size_of::<u32>()) }
    }

    #[test]
    fn test_bloomfilter_empty() {
        let mut bloom_test = BloomTest::new(Box::new(BloomFilterPolicy::new(10)));
        assert!(!bloom_test.matches(&"hello".as_bytes()));
        assert!(!bloom_test.matches(&"world".as_bytes()));
    }

    #[test]
    fn test_bloomfilter_small() {
        let mut bloom_test = BloomTest::new(Box::new(BloomFilterPolicy::new(10)));
        bloom_test.add(&"hello".as_bytes());
        bloom_test.add(&"world".as_bytes());
        assert!(bloom_test.matches(&"hello".as_bytes()));
        assert!(bloom_test.matches(&"world".as_bytes()));
        assert!(!bloom_test.matches(&"x".as_bytes()));
        assert!(!bloom_test.matches(&"foo".as_bytes()));
    }

    #[test]
    fn test_bloomfilter_varying_lengths() {
        let mut bloom_test = BloomTest::new(Box::new(BloomFilterPolicy::new(10)));
        let mut buf = [0; size_of::<u32>()];
        let mut mediocre_filters = 0;
        let mut good_filters = 0;
        let lengths = iter::successors(Some(1), |&l| {
            if l < 10 {
                Some(l + 1)
            } else if l < 100 {
                Some(l + 10)
            } else if l < 1000 {
                Some(l + 100)
            } else if l < 10000 {
                Some(l + 1000)
            } else {
                None
            }
        });
        for l in lengths {
            bloom_test.reset();
            for i in 0..l {
                bloom_test.add(&key(i, &mut buf));
            }
            bloom_test.build();

            assert!(bloom_test.filter_size() <= l as usize * 10 / 8 + 40);

            // All added keys must match
            for i in 0..l {
                assert!(bloom_test.matches(&key(i, &mut buf)));
            }

            // Check false positive rate
            let rate = bloom_test.false_positive_rate();

            assert!(rate <= 0.02); // Must not be over 2%
            if rate > 0.0125 {
                mediocre_filters += 1; // Allowed, but not too often
            } else {
                good_filters += 1;
            }
        }
        assert!(mediocre_filters <= good_filters / 5);
    }
}
