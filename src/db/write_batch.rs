use std::mem;

use crate::{
    dbformat::ValueType,
    memtable::MemTable,
    util::{
        decode_fixed32, decode_fixed64, decode_size_prefixed_slice, encode_fixed32, encode_fixed64,
        extend_size_prefixed_slice,
    },
    Error, Result,
};

/// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
const HEADER_SIZE: usize = 12;
const SEQ_SIZE: usize = mem::size_of::<u64>();

pub trait WriteBatchHandler {
    fn put(&mut self, key: &[u8], value: &[u8]);
    fn delete(&mut self, key: &[u8]);
}

struct MemTableInserter<'a> {
    sequence: u64,
    mem: &'a mut MemTable,
}

impl<'a> MemTableInserter<'a> {
    fn new(sequence: u64, mem: &'a mut MemTable) -> Self {
        Self { sequence, mem }
    }
}

impl<'a> WriteBatchHandler for MemTableInserter<'a> {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.mem.add(self.sequence, ValueType::Value, key, value);
        self.sequence += 1;
    }

    fn delete(&mut self, key: &[u8]) {
        self.mem.add(self.sequence, ValueType::Deletion, key, &[]);
        self.sequence += 1;
    }
}

pub struct WriteBatch {
    rep: Vec<u8>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self {
            rep: vec![0; HEADER_SIZE],
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.set_count(self.count() + 1);
        self.rep.push(ValueType::Value as u8);
        extend_size_prefixed_slice(&mut self.rep, key);
        extend_size_prefixed_slice(&mut self.rep, value);
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.set_count(self.count() + 1);
        self.rep.push(ValueType::Deletion as u8);
        extend_size_prefixed_slice(&mut self.rep, key);
    }

    pub fn clear(&mut self) {
        self.rep.clear();
        self.rep.resize(HEADER_SIZE, 0);
    }

    pub fn approximate_size(&self) -> usize {
        self.rep.len()
    }

    pub fn append(&mut self, source: &WriteBatch) {
        self.set_count(self.count() + source.count());
        self.rep.extend_from_slice(&source.rep[HEADER_SIZE..]);
    }

    pub fn iterate(&self, mut handler: Box<dyn WriteBatchHandler + '_>) -> Result<()> {
        if self.rep.len() < HEADER_SIZE {
            return Err(Error::corruption("malformed WriteBatch (too small)"));
        }

        let mut index = HEADER_SIZE;
        let mut found = 0;
        while index != self.rep.len() {
            found += 1;
            let tag = self.rep[index];
            index += 1;
            match tag.into() {
                ValueType::Value => {
                    let key = match decode_size_prefixed_slice(&self.rep[index..]) {
                        Some((key, offset)) => {
                            index += offset;
                            key
                        }
                        None => return Err(Error::corruption("bad WriteBatch Put")),
                    };
                    let value = match decode_size_prefixed_slice(&self.rep[index..]) {
                        Some((value, offset)) => {
                            index += offset;
                            value
                        }
                        None => return Err(Error::corruption("bad WriteBatch Put")),
                    };
                    handler.put(key, value);
                }
                ValueType::Deletion => {
                    let key = match decode_size_prefixed_slice(&self.rep[index..]) {
                        Some((key, offset)) => {
                            index += offset;
                            key
                        }
                        None => return Err(Error::corruption("bad WriteBatch Delete")),
                    };
                    handler.delete(key);
                }
            }
        }

        if found != self.count() {
            Err(Error::corruption("WriteBatch has wrong count"))
        } else {
            Ok(())
        }
    }

    pub(crate) fn count(&self) -> u32 {
        decode_fixed32(&self.rep[SEQ_SIZE..HEADER_SIZE])
    }

    pub(crate) fn set_count(&mut self, n: u32) {
        encode_fixed32(&mut self.rep[SEQ_SIZE..HEADER_SIZE], n)
    }

    pub(crate) fn sequence(&self) -> u64 {
        decode_fixed64(&self.rep[..SEQ_SIZE])
    }

    pub(crate) fn set_sequence(&mut self, seq: u64) {
        encode_fixed64(&mut self.rep[..SEQ_SIZE], seq)
    }

    pub(crate) fn contents(&self) -> &[u8] {
        &self.rep
    }

    pub(crate) fn byte_size(&self) -> usize {
        self.rep.len()
    }

    pub(crate) fn set_contents(&mut self, contents: &[u8]) {
        self.rep = contents.to_vec()
    }

    pub(crate) fn insert_into(&self, memtable: &mut MemTable) -> Result<()> {
        let inserter = Box::new(MemTableInserter::new(self.sequence(), memtable));
        self.iterate(inserter)
    }
}

#[cfg(test)]
mod tests {
    use std::str::from_utf8;

    use super::WriteBatch;
    use crate::{
        dbformat::{InternalKeyComparator, ParsedInternalKey, ValueType},
        memtable::MemTable,
        util::BytewiseComparator,
    };

    fn print_contents(b: &WriteBatch) -> String {
        let cmp = InternalKeyComparator::new(Box::new(BytewiseComparator::new()));
        let mem = MemTable::new(cmp);
        let status = b.insert_into(&mut mem.borrow_mut());
        let mem_inner = mem.borrow();
        let mut iter = mem_inner.new_iterator();
        iter.seek_to_first();
        let mut result = String::new();
        let mut count = 0;
        while iter.valid() {
            let ikey = ParsedInternalKey::parse(iter.key()).unwrap();
            match ikey.type_() {
                ValueType::Value => {
                    result.push_str(&format!(
                        "Put({}, {})",
                        from_utf8(ikey.user_key()).unwrap(),
                        from_utf8(iter.value()).unwrap()
                    ));
                    count += 1;
                }
                ValueType::Deletion => {
                    result.push_str(&format!("Delete({})", from_utf8(ikey.user_key()).unwrap(),));
                    count += 1;
                }
            }
            result.push('@');
            result.push_str(&ikey.sequence().to_string());
            iter.next();
        }

        if status.is_err() {
            result.push_str("ParseError()");
        } else if count != b.count() {
            result.push_str("CountMismatch()");
        }
        result
    }

    #[test]
    fn test_write_batch_empty() {
        let batch = WriteBatch::new();
        assert_eq!("", print_contents(&batch));
        assert_eq!(0, batch.count());
    }

    #[test]
    fn test_write_batch_multiple() {
        let mut batch = WriteBatch::new();
        batch.put("foo".as_bytes(), "bar".as_bytes());
        batch.delete("box".as_bytes());
        batch.put("baz".as_bytes(), "boo".as_bytes());
        batch.set_sequence(100);
        assert_eq!(100, batch.sequence());
        assert_eq!(3, batch.count());
        assert_eq!(
            "Put(baz, boo)@102Delete(box)@101Put(foo, bar)@100",
            print_contents(&batch)
        );
    }

    #[test]
    fn test_write_batch_corruption() {
        let mut batch = WriteBatch::new();
        batch.put("foo".as_bytes(), "bar".as_bytes());
        batch.delete("box".as_bytes());
        batch.set_sequence(200);
        let content = batch.contents().to_owned();
        batch.set_contents(&content[..content.len() - 1]);
        assert_eq!("Put(foo, bar)@200ParseError()", print_contents(&batch));
    }

    #[test]
    fn test_write_batch_append() {
        let mut b1 = WriteBatch::new();
        b1.set_sequence(200);
        let mut b2 = WriteBatch::new();
        b2.set_sequence(300);
        b1.append(&b2);
        assert_eq!("", print_contents(&b1));
        b2.put("a".as_bytes(), "va".as_bytes());
        b1.append(&b2);
        assert_eq!("Put(a, va)@200", print_contents(&b1));
        b2.clear();
        b2.put("b".as_bytes(), "vb".as_bytes());
        b1.append(&b2);
        assert_eq!("Put(a, va)@200Put(b, vb)@201", print_contents(&b1));
        b2.delete("foo".as_bytes());
        b1.append(&b2);
        assert_eq!(
            "Put(a, va)@200Put(b, vb)@202Put(b, vb)@201Delete(foo)@203",
            print_contents(&b1)
        );
    }

    #[test]
    fn test_write_batch_approximate_size() {
        let mut batch = WriteBatch::new();
        let empty_size = batch.approximate_size();

        batch.put("foo".as_bytes(), "bar".as_bytes());
        let one_key_size = batch.approximate_size();
        assert!(empty_size < one_key_size);

        batch.put("baz".as_bytes(), "boo".as_bytes());
        let two_key_size = batch.approximate_size();
        assert!(one_key_size < two_key_size);

        batch.delete("box".as_bytes());
        let post_delete_size = batch.approximate_size();
        assert!(two_key_size < post_delete_size);
    }
}
