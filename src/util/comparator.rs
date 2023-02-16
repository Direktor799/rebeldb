use std::cmp::Ordering;

/// Used for slice compare with custom order
pub trait Comparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;
    fn name(&self) -> &str;
    fn find_shortest_separator(&self, start: &[u8], _limit: &[u8]) -> Vec<u8> {
        start.to_vec()
    }
    fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
        key.to_vec()
    }
}

pub struct BytewiseComparator {}

impl BytewiseComparator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Comparator for BytewiseComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    fn name(&self) -> &str {
        "leveldb.BytewiseComparator"
    }

    fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8> {
        let mut result = start.to_vec();
        let min_length = result.len().min(limit.len());
        let diff_index = (0..min_length).find(|&index| result[index] != limit[index]);
        if let Some(diff_index) = diff_index {
            let diff_byte = result[diff_index];
            if diff_byte < u8::MAX && diff_byte + 1 < limit[diff_index] {
                result[diff_index] = result[diff_index] + 1;
                result.resize(diff_index + 1, Default::default());
            }
        }
        result
    }

    fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
        let mut result = key.to_vec();
        let index = (0..result.len()).find(|&index| result[index] != 0xff);
        if let Some(index) = index {
            result[index] = result[index] + 1;
            result.resize(index + 1, Default::default());
        }
        result
    }
}
