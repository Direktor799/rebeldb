mod arena;
mod skiplist;

use std::{
    cell::RefCell, cmp::Ordering, intrinsics::copy_nonoverlapping, ptr::null, rc::Rc, slice,
};

use skiplist::{KeyComparator, SkipList, SkipListIterator};

use crate::{
    dbformat::{InternalKeyComparator, LookupKey, ValueType},
    iterator::Iterator,
    util::{
        decode_fixed64, decode_varint32, encode_fixed64, encode_varint32, extend_varint32,
        varint_size, Comparator, Error, Result,
    },
};

/// Used to get internal key
fn decode_length_prefixed_slice_ptr(ptr: *const u8) -> (&'static [u8], usize) {
    let buf = unsafe { slice::from_raw_parts(ptr, 5) };
    let (len, offset) = decode_varint32(&buf).unwrap();
    (
        unsafe { &slice::from_raw_parts(ptr.add(offset), len as usize) },
        offset + len as usize,
    )
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
        self.comparator.compare(a, b)
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
        self.tmp.clear();
        extend_varint32(&mut self.tmp, target.len() as u32);
        self.tmp.extend_from_slice(target);
        self.iter.seek(&self.tmp.as_ptr())
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
    pub fn add(&mut self, seq: u64, type_: ValueType, key: &[u8], value: &[u8]) {
        let internal_key_size = key.len() + 8;
        let encoded_len = varint_size(internal_key_size as u64)
            + internal_key_size
            + varint_size(value.len() as u64)
            + value.len();

        let ptr = self.table.arena.allocate(encoded_len);
        let mem_kv = unsafe { slice::from_raw_parts_mut(ptr, encoded_len) };

        let varint_len = encode_varint32(mem_kv, internal_key_size as u32);
        unsafe {
            copy_nonoverlapping(key.as_ptr(), mem_kv[varint_len..].as_mut_ptr(), key.len());
        }
        let offset_to_tag = varint_len + key.len();
        encode_fixed64(&mut mem_kv[offset_to_tag..], (seq << 8) | type_ as u64);
        let offset_to_value = offset_to_tag + 8;
        let varint_len = encode_varint32(&mut mem_kv[offset_to_value..], value.len() as u32);
        unsafe {
            copy_nonoverlapping(
                value.as_ptr(),
                mem_kv[offset_to_value + varint_len..].as_mut_ptr(),
                value.len(),
            );
        }

        self.table.insert(mem_kv.as_ptr());
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
                        return Some(Err(Error::not_found("")));
                    }
                }
            }
        }
        None
    }
}
