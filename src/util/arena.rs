/// Used for memory allocation
use std::{mem, ptr};

const BLOCK_SIZE: usize = 4096;

pub struct Arena {
    alloc_ptr: *mut u8,
    alloc_bytes_remaining: usize,
    blocks: Vec<Vec<u8>>,
    memory_usage: usize,
}

impl Arena {
    pub fn new() -> Self {
        Self {
            alloc_ptr: ptr::null_mut(),
            alloc_bytes_remaining: 0,
            blocks: vec![],
            memory_usage: 0,
        }
    }

    pub fn allocate(&mut self, bytes: usize) -> *mut u8 {
        // no 0-byte allocations
        assert!(bytes > 0);
        if bytes <= self.alloc_bytes_remaining {
            let result = self.alloc_ptr;
            self.alloc_ptr = unsafe { self.alloc_ptr.add(bytes) };
            self.alloc_bytes_remaining -= bytes;
            return result;
        }
        self.allocate_fallback(bytes)
    }

    pub fn allocate_aligned(&mut self, bytes: usize) -> *mut u8 {
        let align = mem::size_of::<usize>().max(8);
        let current_mod = self.alloc_ptr as usize & (align - 1);
        let slop = if current_mod == 0 {
            0
        } else {
            align - current_mod
        };
        if slop + bytes <= self.alloc_bytes_remaining {
            let result = unsafe { self.alloc_ptr.add(slop) };
            self.alloc_ptr = unsafe { self.alloc_ptr.add(slop + bytes) };
            self.alloc_bytes_remaining -= slop + bytes;
            return result;
        }
        let result = self.allocate_fallback(bytes);
        // alignment should be OK, but no guarantee
        assert!(result as usize & (align - 1) == 0);
        result
    }

    pub fn memory_usage(&self) -> usize {
        self.memory_usage
    }

    fn allocate_fallback(&mut self, bytes: usize) -> *mut u8 {
        // allocate huge block separately
        if bytes > BLOCK_SIZE / 4 {
            return self.allocate_new_block(bytes);
        }

        self.alloc_ptr = self.allocate_new_block(BLOCK_SIZE);
        self.alloc_bytes_remaining = BLOCK_SIZE;
        let result = self.alloc_ptr;
        self.alloc_ptr = unsafe { self.alloc_ptr.add(bytes) };
        self.alloc_bytes_remaining -= bytes;
        result
    }

    fn allocate_new_block(&mut self, block_bytes: usize) -> *mut u8 {
        self.blocks.push(vec![0u8; block_bytes]);
        self.memory_usage += block_bytes;
        self.blocks.last_mut().unwrap().as_mut_ptr()
    }
}

#[cfg(test)]
mod tests {
    use super::Arena;
    use crate::util::Random;

    #[test]
    fn test_arena_empty() {
        let _arena = Arena::new();
    }

    #[test]
    fn test_arena_simple() {
        let mut allocated: Vec<(usize, *const u8)> = Vec::new();
        let mut arena = Arena::new();
        const N: usize = 100000;
        let mut bytes = 0usize;
        let mut rnd = Random::new(301);
        for i in 0..N {
            let mut s = if i % (N / 10) == 0 {
                i
            } else {
                if rnd.one_in(4000) {
                    rnd.uniform(6000) as usize
                } else {
                    if rnd.one_in(10) {
                        rnd.uniform(100) as usize
                    } else {
                        rnd.uniform(20) as usize
                    }
                }
            };
            if s == 0 {
                // Our arena disallows size 0 allocations.
                s = 1;
            }
            let r = if rnd.one_in(10) {
                arena.allocate_aligned(s)
            } else {
                arena.allocate(s)
            };

            for b in 0..s {
                // Fill the "i"th allocation with a known bit pattern
                unsafe { *r.add(b) = i as u8 };
            }
            bytes += s;
            allocated.push((s, r));
            assert!(arena.memory_usage() >= bytes);
            if i > N / 10 {
                assert!(arena.memory_usage() <= (bytes as f64 * 1.10) as usize);
            }
        }
        for i in 0..allocated.len() {
            let num_bytes = allocated[i].0;
            let p = allocated[i].1;
            for b in 0..num_bytes {
                // Check the "i"th allocation for the known bit pattern
                assert_eq!(unsafe { *p.add(b) }, i as u8);
            }
        }
    }
}
