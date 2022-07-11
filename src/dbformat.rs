use std::{cmp::Ordering, intrinsics::copy_nonoverlapping, io::BufRead, ptr::null_mut, slice};

use crate::util::{
    decode_fixed64, encode_fixed64, encode_varint32, put_fixed64, Comparator, FilterPolicy,
};

pub type SequenceNumber = u64;

pub const MAX_SEQUENCE_NUMBER: u64 = (1 << 56) - 1;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum ValueType {
    Deletion = 0x0,
    Value = 0x1,
}

impl From<u8> for ValueType {
    fn from(value: u8) -> Self {
        if value == 0x0 {
            Self::Deletion
        } else if value == 0x1 {
            Self::Value
        } else {
            panic!()
        }
    }
}

pub const VALUE_TYPE_FOR_SEEK: ValueType = ValueType::Value;

struct ParsedInternalKey<'a> {
    user_key: &'a [u8],
    sequence: SequenceNumber,
    type_: ValueType,
}

impl<'a> ParsedInternalKey<'a> {
    pub fn new(user_key: &'a [u8], sequence: SequenceNumber, type_: ValueType) -> Self {
        Self {
            user_key,
            sequence,
            type_,
        }
    }
}

/// Append the serialization of "key" to dst.
fn append_internal_key(dst: &mut Vec<u8>, key: &ParsedInternalKey) {
    dst.extend_from_slice(key.user_key);
    put_fixed64(dst, key.sequence << 8 | key.type_ as u64);
}

/// Attempt to parse an internal key from "internal_key".
fn parse_internal_key(internal_key: &[u8]) -> Option<ParsedInternalKey> {
    let n = internal_key.len();
    if n < 8 {
        return None;
    }
    let num = decode_fixed64(&internal_key[n - 8..]);
    Some(ParsedInternalKey::new(
        unsafe { slice::from_raw_parts(internal_key.as_ptr(), n - 8) },
        num >> 8,
        (num as u8).into(),
    ))
}

fn extract_user_key(internal_key: &[u8]) -> &[u8] {
    assert!(internal_key.len() >= 8);
    &internal_key[..internal_key.len() - 8]
}

pub struct InternalKeyComparator {
    user_comparator: Box<dyn Comparator>,
}

impl InternalKeyComparator {
    pub fn new(user_comparator: Box<dyn Comparator>) -> Self {
        Self { user_comparator }
    }

    pub fn user_comparator(&self) -> &dyn Comparator {
        self.user_comparator.as_ref()
    }
}

impl Comparator for InternalKeyComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        // Order by:
        //    increasing user key (according to user-supplied comparator)
        //    decreasing sequence number
        //    decreasing type (though sequence# should be enough to disambiguate)
        let r = self
            .user_comparator
            .compare(extract_user_key(a), extract_user_key(b));
        if r == Ordering::Equal {
            let anum = decode_fixed64(&a[a.len() - 8..]);
            let bnum = decode_fixed64(&b[b.len() - 8..]);
            anum.cmp(&bnum).reverse()
        } else {
            r
        }
    }

    fn name(&self) -> &str {
        "leveldb.InternalKeyComparator"
    }

    fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8> {
        let user_start = extract_user_key(start);
        let user_limit = extract_user_key(limit);
        let mut result = self
            .user_comparator
            .find_shortest_separator(user_start, user_limit);
        if result.len() < user_start.len()
            && self.user_comparator.compare(user_start, &result) == Ordering::Less
        {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            put_fixed64(
                &mut result,
                MAX_SEQUENCE_NUMBER << 8 | VALUE_TYPE_FOR_SEEK as u64,
            );
            assert_eq!(self.compare(start, &result), Ordering::Less);
            assert_eq!(self.compare(&result, limit), Ordering::Less);
            result
        } else {
            start.to_vec()
        }
    }

    fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
        let user_key = extract_user_key(key);
        let mut result = self.user_comparator.find_short_successor(user_key);
        if result.len() < user_key.len()
            && self.user_comparator.compare(user_key, &result) == Ordering::Less
        {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            put_fixed64(
                &mut result,
                MAX_SEQUENCE_NUMBER << 8 | VALUE_TYPE_FOR_SEEK as u64,
            );
            assert_eq!(self.compare(key, &result), Ordering::Less);
            result
        } else {
            key.to_vec()
        }
    }
}

struct InternalFilterPolicy {
    user_policy: Box<dyn FilterPolicy>,
}

impl InternalFilterPolicy {
    fn new(user_policy: Box<dyn FilterPolicy>) -> Self {
        Self { user_policy }
    }
}

impl FilterPolicy for InternalFilterPolicy {
    fn name(&self) -> &str {
        self.user_policy.name()
    }

    fn create_filter(&self, keys: &[&[u8]], dst: &mut Vec<u8>) {
        let mut user_keys = Vec::with_capacity(keys.len());
        for key in keys {
            user_keys.push(extract_user_key(key));
        }
        self.user_policy.create_filter(&user_keys, dst)
    }

    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        self.user_policy
            .key_may_match(extract_user_key(key), filter)
    }
}

struct InternalKey {
    rep: Vec<u8>,
}

impl InternalKey {
    pub fn new_empty() -> Self {
        Self { rep: vec![] }
    }

    pub fn new(user_key: &[u8], seq: SequenceNumber, type_: ValueType) -> Self {
        let mut rep = vec![];
        append_internal_key(&mut rep, &ParsedInternalKey::new(user_key, seq, type_));
        Self { rep }
    }

    pub fn decode_from(&mut self, s: &[u8]) -> bool {
        self.rep = s.to_vec();
        !self.rep.is_empty()
    }

    pub fn encode(&self) -> &[u8] {
        assert!(!self.rep.is_empty());
        &self.rep
    }

    pub fn user_key(&self) -> &[u8] {
        extract_user_key(&self.rep)
    }
}

const LOOKUP_KEY_STACK_SPACE: usize = 200;

enum LookupKeyInner {
    OnStack([u8; LOOKUP_KEY_STACK_SPACE]),
    OnHeap(Vec<u8>),
}

pub struct LookupKey {
    start: *mut u8,
    kstart: *mut u8,
    end: *mut u8,
    space: LookupKeyInner,
}

impl LookupKey {
    pub fn new(user_key: &[u8], sequence: SequenceNumber) -> Self {
        let ksize = user_key.len();
        let needed = ksize + 13;
        let mut result = if needed <= LOOKUP_KEY_STACK_SPACE {
            Self {
                start: null_mut(),
                kstart: null_mut(),
                end: null_mut(),
                space: LookupKeyInner::OnStack([0; LOOKUP_KEY_STACK_SPACE]),
            }
        } else {
            Self {
                start: null_mut(),
                kstart: null_mut(),
                end: null_mut(),
                space: LookupKeyInner::OnHeap(vec![0; needed]),
            }
        };
        result.start = match result.space {
            LookupKeyInner::OnStack(mut data) => data.as_mut_ptr(),
            LookupKeyInner::OnHeap(ref mut data) => data.as_mut_ptr(),
        };
        let mut target = unsafe { slice::from_raw_parts_mut(result.start, needed) };
        let koffset = encode_varint32(&mut target, (ksize + 8) as u32);
        unsafe {
            result.kstart = result.start.add(koffset);
            copy_nonoverlapping(user_key.as_ptr(), result.kstart, ksize);
            encode_fixed64(
                &mut target[koffset + ksize..],
                sequence << 8 | VALUE_TYPE_FOR_SEEK as u64,
            );
            result.end = result.start.add(koffset + ksize + 8);
        }
        result
    }

    pub fn memtable_key(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.start, self.end.offset_from(self.start) as usize) }
    }

    pub fn internal_key(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.kstart, self.end.offset_from(self.kstart) as usize) }
    }

    pub fn user_key(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(self.kstart, self.end.offset_from(self.kstart) as usize - 8)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        dbformat::{MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK},
        util::{BytewiseComparator, Comparator},
    };

    use super::{
        append_internal_key, parse_internal_key, InternalKey, InternalKeyComparator,
        ParsedInternalKey, SequenceNumber, ValueType,
    };

    fn ikey(user_key: &[u8], seq: SequenceNumber, type_: ValueType) -> Vec<u8> {
        let mut encoded = vec![];
        append_internal_key(
            &mut encoded,
            &super::ParsedInternalKey::new(user_key, seq, type_),
        );
        encoded
    }

    fn shorten(short: &[u8], long: &[u8]) -> Vec<u8> {
        InternalKeyComparator::new(Box::new(BytewiseComparator::new()))
            .find_shortest_separator(short, long)
    }

    fn short_successor(short: &[u8]) -> Vec<u8> {
        InternalKeyComparator::new(Box::new(BytewiseComparator::new())).find_short_successor(short)
    }

    fn test_key(user_key: &[u8], seq: SequenceNumber, type_: ValueType) {
        let encoded = ikey(user_key, seq, type_);
        let decoded = parse_internal_key(&encoded).unwrap();
        assert_eq!(user_key, decoded.user_key);
        assert_eq!(seq, decoded.sequence);
        assert_eq!(type_, decoded.type_);
    }

    #[test]
    fn test_format_internal_key_encode_decode() {
        let keys = vec!["", "k", "hello", "longggggggggggggggggggggg"];
        let seqs = vec![
            1,
            2,
            3,
            (1 << 8) - 1,
            1 << 8,
            (1 << 8) + 1,
            (1 << 16) - 1,
            1 << 16,
            (1 << 16) + 1,
            (1 << 32) - 1,
            1 << 32,
            (1 << 32) + 1,
        ];
        for key in keys {
            for &seq in &seqs {
                test_key(key.as_bytes(), seq, ValueType::Value);
                test_key("hello".as_bytes(), 1, ValueType::Deletion);
            }
        }
    }

    #[test]
    fn test_format_internal_key_decode_from_empty() {
        let mut internal_key = InternalKey::new_empty();
        assert!(!internal_key.decode_from(&[]));
    }

    #[test]
    fn test_format_internal_key_shortest_separator() {
        // When user keys are same
        assert_eq!(
            ikey("foo".as_bytes(), 100, ValueType::Value),
            shorten(
                &ikey("foo".as_bytes(), 100, ValueType::Value),
                &ikey("foo".as_bytes(), 99, ValueType::Value)
            ),
        );

        assert_eq!(
            ikey("foo".as_bytes(), 100, ValueType::Value),
            shorten(
                &ikey("foo".as_bytes(), 100, ValueType::Value),
                &ikey("foo".as_bytes(), 101, ValueType::Value)
            ),
        );

        assert_eq!(
            ikey("foo".as_bytes(), 100, ValueType::Value),
            shorten(
                &ikey("foo".as_bytes(), 100, ValueType::Value),
                &ikey("foo".as_bytes(), 100, ValueType::Value)
            ),
        );

        assert_eq!(
            ikey("foo".as_bytes(), 100, ValueType::Value),
            shorten(
                &ikey("foo".as_bytes(), 100, ValueType::Value),
                &ikey("foo".as_bytes(), 100, ValueType::Deletion),
            ),
        );

        // When user keys are misordered
        assert_eq!(
            ikey("foo".as_bytes(), 100, ValueType::Value),
            shorten(
                &ikey("foo".as_bytes(), 100, ValueType::Value),
                &ikey("bar".as_bytes(), 99, ValueType::Value)
            ),
        );

        // When user keys are different, but correctly ordered
        assert_eq!(
            ikey("g".as_bytes(), MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK),
            shorten(
                &ikey("foo".as_bytes(), 100, ValueType::Value),
                &ikey("hello".as_bytes(), 200, ValueType::Value)
            ),
        );

        // When start user key is prefix of limit user key
        assert_eq!(
            ikey("foo".as_bytes(), 100, ValueType::Value),
            shorten(
                &ikey("foo".as_bytes(), 100, ValueType::Value),
                &ikey("foobar".as_bytes(), 200, ValueType::Value),
            ),
        );

        // When limit user key is prefix of start user key
        assert_eq!(
            ikey("foobar".as_bytes(), 100, ValueType::Value),
            shorten(
                &ikey("foobar".as_bytes(), 100, ValueType::Value),
                &ikey("foo".as_bytes(), 200, ValueType::Value),
            ),
        );
    }

    #[test]
    fn test_format_internal_key_short_successor() {
        assert_eq!(
            ikey("g".as_bytes(), MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK),
            short_successor(&ikey("foo".as_bytes(), 100, ValueType::Value))
        );
        assert_eq!(
            ikey(&[0xff, 0xff], 100, ValueType::Value),
            short_successor(&ikey(&[0xff, 0xff], 100, ValueType::Value))
        );
    }
}
