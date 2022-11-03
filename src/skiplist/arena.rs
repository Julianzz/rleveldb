use std::{
    cell::Cell,
    mem,
    ops::Add,
    ptr,
    sync::atomic::{AtomicUsize, Ordering},
};

const ADDR_ALIGN_MASK: usize = 7;

pub struct Arena {
    len: AtomicUsize,
    cap: Cell<usize>,
    ptr: Cell<*mut u8>,
}

impl Arena {
    pub fn with_capacity(cap: usize) -> Self {
        let mut buf: Vec<u64> = Vec::with_capacity(cap / 8);
        let ptr = buf.as_mut_ptr() as *mut u8;
        let cap = buf.capacity() * 8;
        mem::forget(buf);

        Arena {
            len: AtomicUsize::new(8),
            cap: Cell::new(cap),
            ptr: Cell::new(ptr),
        }
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::SeqCst)
    }

    pub fn alloc(&self, size: usize) -> usize {
        let size = (size + ADDR_ALIGN_MASK) & !ADDR_ALIGN_MASK;
        let offset = self.len.fetch_add(size, Ordering::SeqCst);

        if offset + size > self.cap.get() {
            let mut grow_by = self.cap.get();
            if grow_by > 1 << 30 {
                grow_by = 1 << 30
            }
            if grow_by < size {
                grow_by = size
            }

            let new_size = (self.cap.get() + grow_by) / 8;
            let mut new_buf: Vec<u64> = Vec::with_capacity(new_size);
            let new_ptr = new_buf.as_mut_ptr() as *mut u8;
            unsafe { ptr::copy_nonoverlapping(self.ptr.get(), new_ptr, self.cap.get()) }

            //release old
            let old_ptr = self.ptr.get() as *mut u64;
            unsafe {
                Vec::from_raw_parts(old_ptr, 0, self.cap.get() / 8);
            }

            self.ptr.set(new_ptr);
            self.cap.set(new_buf.capacity() * 8);

            mem::forget(new_buf);
        }

        offset
    }

    pub unsafe fn get_mut<T>(&self, offset: usize) -> *mut T {
        if offset == 0 {
            return ptr::null_mut();
        }
        self.ptr.get().add(offset) as _
    }

    pub fn offset<T>(&self, ptr: *const T) -> usize {
        let ptr_addr = ptr as usize;
        let self_addr = self.ptr.get() as usize;
        if ptr_addr > self_addr && ptr_addr < self_addr + self.cap.get() {
            ptr_addr - self_addr
        } else {
            0
        }
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        let ptr = self.ptr.get() as *mut u64;
        let cap = self.cap.get() / 8;
        unsafe {
            Vec::from_raw_parts(ptr, 0, cap);
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena() {
        // There is enough space
        let arena = Arena::with_capacity(128);
        let offset = arena.alloc(8);
        assert_eq!(offset, 8);
        assert_eq!(arena.len(), 16);
        unsafe {
            let ptr = arena.get_mut::<u64>(offset);
            let offset = arena.offset::<u64>(ptr);
            assert_eq!(offset, 8);
        }

        // There is not enough space, grow buf and then return the offset
        let offset = arena.alloc(256);
        assert_eq!(offset, 16);
    }
}
