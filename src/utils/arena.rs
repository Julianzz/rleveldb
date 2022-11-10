use std::{
    mem::size_of,
    sync::atomic::{AtomicUsize, Ordering},
};

const BLOCK_SIZE: usize = 4096;
const POINTER_LENGTH: usize = size_of::<*mut u8>();
const EXTRA_VEC_LEN: usize = POINTER_LENGTH + 2 * size_of::<usize>();

pub struct Arena {
    p: *mut u8,
    remain: usize,
    usage: AtomicUsize,
    blocks: Vec<Vec<u8>>,
}

impl Arena {
    pub fn allocate(&mut self, n: usize) -> *mut u8 {
        if n < self.remain {
            let result = self.p;
            unsafe {
                self.p = self.p.add(n);
            }
            self.remain -= n;
            result
        } else {
            self.allocate_fallback(n)
        }
    }

    pub fn allocated_aligned(&mut self, n: usize) -> *mut u8 {
        let align = if POINTER_LENGTH > 8 {
            POINTER_LENGTH
        } else {
            8
        };
        let current_mod = self.p as usize & (align - 1);
        let slop = if current_mod == 0 {
            0
        } else {
            align - current_mod
        };
        let needed = n + slop;
        if needed <= self.remain {
            let result = unsafe {
                let result = self.p.add(slop);
                self.p = self.p.add(needed);
                result
            };
            self.remain -= needed;
            result
        } else {
            self.allocate_fallback(n)
        }
    }

    pub fn memory_usage(&self) -> usize {
        self.usage.load(Ordering::SeqCst)
    }

    pub fn allocate_fallback(&mut self, n: usize) -> *mut u8 {
        if n > BLOCK_SIZE / 4 {
            self.allocate_new_block(n)
        } else {
            self.p = self.allocate_new_block(BLOCK_SIZE);
            self.remain = BLOCK_SIZE;
            let result = self.p;
            unsafe {
                self.p = self.p.add(n);
            }
            self.remain -= n;
            result
        }
    }

    fn allocate_new_block(&mut self, block_bytes: usize) -> *mut u8 {
        let mut v: Vec<u8> = Vec::with_capacity(block_bytes);
        // unsafe {
        //     v.set_len(block_bytes);
        // }
        let r = v.as_mut_ptr();
        self.blocks.push(v);
        self.usage
            .fetch_add(block_bytes + size_of::<Vec<u8>>(), Ordering::SeqCst);
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vec_len() {
        assert_eq!(size_of::<Vec<u8>>(), EXTRA_VEC_LEN);
    }
}
