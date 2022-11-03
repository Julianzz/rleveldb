use std::{ptr, slice::from_raw_parts};

#[derive(Clone, Copy)]
pub struct UnsafeSlice {
    data: *const u8,
    size: usize,
}

impl UnsafeSlice {
    pub fn new(data: *const u8, size: usize) -> Self {
        UnsafeSlice { data, size }
    }

    #[inline]
    pub fn data(&self) -> *const u8 {
        self.data
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    pub fn at(&self, index: usize) -> u8 {
        assert!(index < self.size);
        unsafe { *self.data.add(index) }
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_null() || self.size == 0
    }

    #[inline]
    pub unsafe fn as_ref(&self) -> &[u8] {
        assert!(!self.data.is_null());
        from_raw_parts(self.data, self.size)
    }
}

impl Default for UnsafeSlice {
    fn default() -> Self {
        Self {
            data: ptr::null(),
            size: 0,
        }
    }
}

impl From<&[u8]> for UnsafeSlice {
    fn from(v: &[u8]) -> Self {
        UnsafeSlice::new(v.as_ptr(), v.len())
    }
}
impl From<&str> for UnsafeSlice {
    fn from(v: &str) -> Self {
        UnsafeSlice::new(v.as_ptr(), v.len())
    }
}

// impl AsRef<[u8]> for Slice {
//     fn as_ref(&self) -> &[u8] {
//         if self.data.is_null() {
//             panic!("try convert empty slice to &[u8]");
//         }
//         unsafe { from_raw_parts(self.data, self.size) }
//     }
// }
