use std::{
    cmp, mem, ptr,
    sync::atomic::{self, AtomicPtr, AtomicUsize},
};

use super::arena::Arena;
use crate::util::Random;

const MAX_HEIGHT: usize = 12;
const BRANCHING: u32 = 4;

/// Used for skiplist::Node<Key>.key, Key is random type
pub trait KeyComparator<Key> {
    fn compare(&self, a: &Key, b: &Key) -> cmp::Ordering;
}

struct Node<Key> {
    key: Key,
    next: [AtomicPtr<Node<Key>>; 1],
}

impl<Key> Node<Key> {
    pub unsafe fn next(&self, height: usize) -> *mut Node<Key> {
        self.next
            .get_unchecked(height)
            .load(atomic::Ordering::Acquire)
    }

    pub unsafe fn set_next(&mut self, height: usize, next: *mut Node<Key>) {
        self.next
            .get_unchecked(height)
            .store(next, atomic::Ordering::Release)
    }

    pub unsafe fn no_barrier_next(&self, height: usize) -> *mut Node<Key> {
        self.next
            .get_unchecked(height)
            .load(atomic::Ordering::Relaxed)
    }

    pub unsafe fn no_barrier_set_next(&mut self, height: usize, next: *mut Node<Key>) {
        self.next
            .get_unchecked(height)
            .store(next, atomic::Ordering::Relaxed)
    }

    pub fn key(&self) -> &Key {
        &self.key
    }
}
pub struct SkipList<Key, C: KeyComparator<Key>> {
    pub(super) comparator: C,
    pub(super) arena: Arena,
    head: *const Node<Key>,
    max_height: AtomicUsize,
    rnd: Random,
}

impl<Key, C: KeyComparator<Key>> SkipList<Key, C> {
    /// requires a dummy value since there is no `Default` for raw pointers
    pub fn new(comparator: C, dummy: Key) -> Self {
        let mut result = Self {
            comparator,
            arena: Arena::new(),
            head: ptr::null(),
            max_height: AtomicUsize::new(1),
            rnd: Random::new(0xdeadbeef),
        };
        result.head = result.new_node(dummy, MAX_HEIGHT);
        result
    }

    pub fn insert(&mut self, key: Key) {
        let mut prev = [ptr::null::<Node<Key>>(); MAX_HEIGHT];
        let _ = self.find_greater_or_equal(&key, Some(&mut prev));
        let height = self.random_height();

        if height > self.get_max_height() {
            for i in self.get_max_height()..height {
                prev[i] = self.head;
            }
            self.max_height.store(height, atomic::Ordering::Release);
        }

        let new_node = self.new_node(key, height);
        for i in 0..height {
            unsafe {
                let prev_i = &mut *(prev[i] as *mut Node<Key>);
                // no need to use barrier now, it happens later
                new_node.no_barrier_set_next(i, prev_i.no_barrier_next(i));
                prev_i.set_next(i, new_node);
            }
        }
    }

    fn get_max_height(&self) -> usize {
        self.max_height.load(atomic::Ordering::Acquire)
    }

    fn new_node(&mut self, key: Key, height: usize) -> &mut Node<Key> {
        let node_memory = self.arena.allocate_aligned(
            mem::size_of::<Node<Key>>() + mem::size_of::<AtomicPtr<Node<Key>>>() * (height - 1),
        ) as *mut Node<Key>;
        let node_ref = unsafe { &mut *node_memory };
        node_ref.key = key;
        node_ref
    }

    fn random_height(&mut self) -> usize {
        let mut height = 1;
        while height < MAX_HEIGHT && self.rnd.one_in(BRANCHING) {
            height += 1;
        }
        height
    }

    /// Return the earliest node that comes at or after the key.
    fn find_greater_or_equal(
        &self,
        key: &Key,
        mut prev: Option<&mut [*const Node<Key>]>,
    ) -> *const Node<Key> {
        let mut current = self.head;
        let mut level = self.get_max_height() - 1;
        loop {
            let next = unsafe { current.as_ref().unwrap().next(level) };
            if !next.is_null()
                && self
                    .comparator
                    .compare(unsafe { next.as_ref().unwrap() }.key(), key)
                    == cmp::Ordering::Less
            {
                current = next;
            } else {
                if let Some(ref mut prev) = prev {
                    prev[level] = current;
                }
                if level == 0 {
                    break unsafe { current.as_ref().unwrap().next(0) };
                } else {
                    level -= 1;
                }
            }
        }
    }

    // Return the latest node with a key < key.
    fn find_less(&self, key: &Key) -> *const Node<Key> {
        let mut current = self.head;
        let mut level = self.get_max_height() - 1;
        loop {
            let next = unsafe { current.as_ref().unwrap().next(level) };
            if !next.is_null()
                && self
                    .comparator
                    .compare(unsafe { next.as_ref().unwrap() }.key(), key)
                    == cmp::Ordering::Less
            {
                current = next;
            } else {
                if level == 0 {
                    break current;
                } else {
                    level -= 1;
                }
            }
        }
    }

    // Return head_ if list is empty.
    fn find_last(&self) -> *const Node<Key> {
        let mut current = self.head;
        let mut level = self.get_max_height() - 1;
        loop {
            let next = unsafe { current.as_ref().unwrap().next(level) };
            if !next.is_null() {
                current = next;
            } else {
                if level == 0 {
                    break current;
                } else {
                    level -= 1;
                }
            }
        }
    }

    #[cfg(test)]
    pub fn contains(&self, key: &Key) -> bool {
        let result = self.find_greater_or_equal(key, None);
        !result.is_null()
            && self
                .comparator
                .compare(unsafe { result.as_ref().unwrap() }.key(), key)
                == cmp::Ordering::Equal
    }
}

pub struct SkipListIterator<'a, Key, C: KeyComparator<Key>> {
    list: &'a SkipList<Key, C>,
    node: *const Node<Key>,
}

impl<'a, Key, C: KeyComparator<Key>> SkipListIterator<'a, Key, C> {
    pub fn new(list: &'a SkipList<Key, C>) -> Self {
        Self {
            list,
            node: ptr::null(),
        }
    }

    pub fn valid(&self) -> bool {
        !self.node.is_null()
    }

    pub fn key(&self) -> &'a Key {
        unsafe { self.node.as_ref().unwrap() }.key()
    }

    pub fn next(&mut self) {
        self.node = unsafe { self.node.as_ref().unwrap().next(0) }
    }

    pub fn prev(&mut self) {
        self.node = self
            .list
            .find_less(unsafe { self.node.as_ref().unwrap() }.key());
        if self.node == self.list.head {
            self.node = ptr::null();
        }
    }

    pub fn seek(&mut self, target: &Key) {
        self.node = self.list.find_greater_or_equal(target, None);
    }

    pub fn seek_to_first(&mut self) {
        self.node = unsafe { self.list.head.as_ref().unwrap().next(0) }
    }

    pub fn seek_to_last(&mut self) {
        self.node = self.list.find_last();
        if self.node == self.list.head {
            self.node = ptr::null();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{KeyComparator, SkipList};
    use crate::{memtable::skiplist::SkipListIterator, util::Random};

    struct U8Comparator {}

    impl KeyComparator<u64> for U8Comparator {
        fn compare(&self, a: &u64, b: &u64) -> std::cmp::Ordering {
            a.cmp(b)
        }
    }

    #[test]
    fn test_skiplist_empty() {
        let list = SkipList::new(U8Comparator {}, 0);
        assert!(!list.contains(&10));
        let mut iter = SkipListIterator::new(&list);
        assert!(!iter.valid());
        iter.seek_to_first();
        assert!(!iter.valid());
        iter.seek(&100);
        assert!(!iter.valid());
        iter.seek_to_last();
        assert!(!iter.valid());
    }

    #[test]
    fn test_skiplist_insert_and_lookup() {
        const N: usize = 2000;
        const R: u64 = 5000;
        let mut rnd = Random::new(1000);
        let mut keys = BTreeSet::new();
        let mut list = SkipList::new(U8Comparator {}, 0);
        for _ in 0..N {
            let key = rnd.next() as u64 % R;
            if keys.insert(key) {
                list.insert(key);
            }
        }
        for i in 0..R {
            assert_eq!(list.contains(&i), keys.contains(&i));
        }
        // Simple iterator tests
        {
            let mut iter = SkipListIterator::new(&list);
            assert!(!iter.valid());

            iter.seek(&0);
            assert!(iter.valid());
            assert_eq!(keys.iter().next().unwrap(), iter.key());

            iter.seek_to_first();
            assert!(iter.valid());
            assert_eq!(keys.iter().next().unwrap(), iter.key());

            iter.seek_to_last();
            assert!(iter.valid());
            assert_eq!(keys.iter().last().unwrap(), iter.key());
        }

        // Forward iteration test
        for i in 0..R {
            let mut list_iter = SkipListIterator::new(&list);
            list_iter.seek(&i);

            // Compare against model iterator
            let mut set_iter = keys.iter().skip_while(|&&v| v < i);
            for _ in 0..3 {
                if let Some(value) = set_iter.next() {
                    assert!(list_iter.valid());
                    assert_eq!(value, list_iter.key());
                    list_iter.next();
                } else {
                    assert!(!list_iter.valid());
                    break;
                }
            }
        }

        // Backward iteration test
        {
            let mut list_iter = SkipListIterator::new(&list);
            list_iter.seek_to_last();

            // Compare against model iterator
            for value in keys.iter().rev() {
                assert!(list_iter.valid());
                assert_eq!(value, list_iter.key());
                list_iter.prev();
            }
            assert!(!list_iter.valid());
        }
    }
}
