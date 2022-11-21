use crate::error::Result;
use std::intrinsics::unlikely;
use std::io;

pub trait BufferWriter {
    unsafe fn bytes_mut(&mut self, size: usize) -> &mut [u8];
    unsafe fn advance_mut(&mut self, count: usize);
    fn write_bytes(&mut self, values: &[u8]) -> Result<()>;
}

pub trait BufferReader {
    fn bytes(&self) -> &[u8];
    fn advance(&mut self, count: usize);
    fn read_bytes(&mut self, count: usize) -> Result<&[u8]>;
}

impl<'a> BufferReader for &'a [u8] {
    #[inline]
    fn bytes(&self) -> &[u8] {
        self
    }

    #[inline]
    fn advance(&mut self, count: usize) {
        *self = &self[count..]
    }

    fn read_bytes(&mut self, count: usize) -> Result<&[u8]> {
        if unlikely(self.len() < count) {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Unexpected EOF").into());
        }
        let (left, right) = self.split_at(count);
        *self = right;
        Ok(left)
    }
}

impl<'a, T: BufferReader + ?Sized> BufferReader for &'a mut T {
    #[inline]
    fn bytes(&self) -> &[u8] {
        (**self).bytes()
    }

    #[inline]
    fn advance(&mut self, count: usize) {
        (**self).advance(count)
    }

    #[inline]
    fn read_bytes(&mut self, count: usize) -> Result<&[u8]> {
        (**self).read_bytes(count)
    }
}

impl<'a> BufferWriter for &'a mut [u8] {
    #[inline]
    unsafe fn bytes_mut(&mut self, _size: usize) -> &mut [u8] {
        self
    }

    #[inline]
    unsafe fn advance_mut(&mut self, count: usize) {
        let original_self = std::mem::take(self);
        *self = &mut original_self[count..];
    }

    fn write_bytes(&mut self, values: &[u8]) -> Result<()> {
        let write_len = values.len();
        if unlikely(self.len() < write_len) {
            return Err(
                io::Error::new(io::ErrorKind::UnexpectedEof, "buffer not long enough").into(),
            );
        }
        let original_self = std::mem::take(self);
        original_self[..write_len].copy_from_slice(values);
        *self = &mut original_self[write_len..];
        Ok(())
    }
}
