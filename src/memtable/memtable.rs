use super::skiplist::{KeyComparator, SkipList, SkipListIterator};
use crate::dbformat::{InternalKeyComparator, LookupKey, SequenceNumber, ValueType};
use crate::iterator::Iterator;
use crate::util::{
    decode_fixed64, decode_varint32, encode_fixed64, encode_varint32, put_varint32, varint_length,
    DBError, Result,
};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::intrinsics::copy_nonoverlapping;
use std::ptr::null;
use std::rc::Rc;
use std::slice;

fn decode_length_prefixed_slice_ptr(ptr: *const u8) -> (&'static [u8], usize) {
    let buf = unsafe { slice::from_raw_parts(ptr, 5) };
    let (len, offset) = decode_varint32(&buf).unwrap();
    (
        unsafe { &slice::from_raw_parts(ptr.add(offset), len as usize) },
        offset + len as usize,
    )
}

fn encode_key(scratch: &mut Vec<u8>, target: &[u8]) -> *const u8 {
    scratch.clear();
    put_varint32(scratch, target.len() as u32);
    scratch.extend_from_slice(target);
    scratch.as_ptr()
}

pub struct MemTableKeyComparator {
    comparator: InternalKeyComparator,
}

impl MemTableKeyComparator {
    pub fn new(comparator: InternalKeyComparator) -> Self {
        Self { comparator }
    }
}

/// compare the inner data which key refers to
impl KeyComparator<*const u8> for MemTableKeyComparator {
    fn compare(&self, a: &*const u8, b: &*const u8) -> Ordering {
        let (a, _) = decode_length_prefixed_slice_ptr(*a);
        let (b, _) = decode_length_prefixed_slice_ptr(*b);
        crate::util::Comparator::compare(&self.comparator, &*a, &*b)
    }
}

struct MemTableIterator<'a> {
    iter: SkipListIterator<'a, *const u8, MemTableKeyComparator>,
    tmp: Vec<u8>,
}

impl<'a> MemTableIterator<'a> {
    pub fn new(table: &'a SkipList<*const u8, MemTableKeyComparator>) -> Self {
        Self {
            iter: SkipListIterator::new(table),
            tmp: vec![],
        }
    }
}

impl<'a> Iterator for MemTableIterator<'a> {
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first()
    }

    fn seek_to_last(&mut self) {
        self.iter.seek_to_last()
    }

    fn seek(&mut self, target: &[u8]) {
        self.iter.seek(&encode_key(&mut self.tmp, target))
    }

    fn next(&mut self) {
        self.iter.next()
    }

    fn prev(&mut self) {
        self.iter.prev()
    }

    fn key(&self) -> &[u8] {
        decode_length_prefixed_slice_ptr(*self.iter.key()).0
    }

    fn value(&self) -> &[u8] {
        let (_, offset) = decode_length_prefixed_slice_ptr(*self.iter.key());
        decode_length_prefixed_slice_ptr(unsafe { self.iter.key().add(offset) }).0
    }

    fn status(&self) -> Result<()> {
        Ok(())
    }
}

pub struct MemTable {
    table: SkipList<*const u8, MemTableKeyComparator>,
}

impl MemTable {
    pub fn new(comparator: InternalKeyComparator) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self {
            table: SkipList::new(MemTableKeyComparator { comparator }, null()),
        }))
    }
    pub fn approximate_memory_usage(&self) -> usize {
        self.table.arena.memory_usage()
    }
    pub fn new_iterator(&self) -> Box<dyn Iterator + '_> {
        Box::new(MemTableIterator::new(&self.table))
    }

    /// Format of an entry is concatenation of:
    ///  key_size     : varint32 of internal_key.size()
    ///  key bytes    : char[internal_key.size()]
    ///  tag          : uint64((sequence << 8) | type)
    ///  value_size   : varint32 of value.size()
    ///  value bytes  : char[value.size()]
    pub fn add(&mut self, seq: SequenceNumber, type_: ValueType, key: &[u8], value: &[u8]) {
        let key_size = key.len();
        let val_size = value.len();
        let internal_key_size = key_size + 8;
        let encoded_len = varint_length(internal_key_size as u64)
            + internal_key_size
            + varint_length(val_size as u64)
            + val_size;
        let ptr = self.table.arena.allocate(encoded_len);

        let memkey = unsafe { slice::from_raw_parts_mut(ptr, encoded_len) };
        let varint_len = encode_varint32(memkey, internal_key_size as u32);
        unsafe {
            copy_nonoverlapping(key.as_ptr(), memkey[varint_len..].as_mut_ptr(), key_size);
        }
        let offset_to_tag = varint_len + key_size;
        encode_fixed64(&mut memkey[offset_to_tag..], (seq << 8) | type_ as u64);
        let offset_to_value = offset_to_tag + 8;
        let varint_len = encode_varint32(&mut memkey[offset_to_value..], val_size as u32);
        unsafe {
            copy_nonoverlapping(
                value.as_ptr(),
                memkey[offset_to_value + varint_len..].as_mut_ptr(),
                val_size,
            );
        }
        assert_eq!(offset_to_value + varint_len + val_size, encoded_len);
        self.table.insert(memkey.as_ptr());
    }

    pub fn get(&self, key: &LookupKey) -> Option<Result<Vec<u8>>> {
        let memkey = key.memtable_key();
        let mut iter = SkipListIterator::new(&self.table);
        iter.seek(&memkey.as_ptr());
        if iter.valid() {
            // entry format is:
            //    klength  varint32
            //    userkey  char[klength]
            //    tag      uint64
            //    vlength  varint32
            //    value    char[vlength]
            // Check that it belongs to same user key.  We do not check the
            // sequence number since the Seek() call above should have skipped
            // all entries with overly large sequence numbers.
            let entry = *iter.key();
            let (ukey_len, ukey_offset) =
                decode_varint32(unsafe { slice::from_raw_parts(entry, 5) }).unwrap();
            if self.table.comparator.comparator.user_comparator().compare(
                unsafe { slice::from_raw_parts(entry.add(ukey_offset), ukey_len as usize - 8) },
                key.user_key(),
            ) == Ordering::Equal
            {
                // Correct user key
                let tag = decode_fixed64(unsafe {
                    slice::from_raw_parts(entry.add(ukey_offset + ukey_len as usize - 8), 8)
                });
                match ValueType::from(tag as u8) {
                    ValueType::Value => {
                        let (value, _) = decode_length_prefixed_slice_ptr(unsafe {
                            entry.add(ukey_offset + ukey_len as usize)
                        });
                        return Some(Ok(value.to_vec()));
                    }
                    ValueType::Deletion => {
                        return Some(Err(DBError::not_found("")));
                    }
                }
            }
        }
        None
    }
}
