pub mod mem;
pub mod posix;

use std::{io::Write, path::Path};

use crate::error::Result;

pub trait RandomAccessFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()>;
}

pub trait WritableFile {
    fn append(&mut self, data: &[u8]) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
}

pub trait SequencialFile {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()>;
    fn skip(&mut self, n: usize) -> Result<()>;
}

pub trait Env: Send + Sync + Clone + 'static {
    type RandomAccessFile: RandomAccessFile + 'static;
    type WritableFile: WritableFile + 'static;
    type SequencialFile: SequencialFile + 'static;

    fn new_random_access_file(&self, name: &Path) -> Result<Self::RandomAccessFile>;
    fn new_writable_file(&self, name: &Path) -> Result<Self::WritableFile>;
    fn new_sequential_file(&self, name: &Path) -> Result<Self::SequencialFile>;
    fn file_size(&self, path: &Path) -> Result<usize>;
}

// pub fn size_of(path: &Path) -> Result<usize> {
//     let meta = fs::metadata(path)?;
//     Ok(meta.len() as usize)
// }

// pub fn open_random_access_file(path: &Path) -> Result<File> {
//     fs::OpenOptions::new()
//         .read(true)
//         .open(path)
//         .map_err(From::from)
// }
