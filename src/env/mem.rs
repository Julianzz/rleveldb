use std::{
    io::Write,
    path::{self, Path},
};

use crate::error::Result;

use super::{Env, RandomAccessFile, SequencialFile, WritableFile};

pub struct MemFs {
    data: Vec<u8>,
}

impl MemFs {
    pub fn new(data: Vec<u8>) -> Self {
        MemFs { data: data }
    }

    pub fn empty() -> Self {
        MemFs { data: Vec::new() }
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

impl RandomAccessFile for MemFs {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let data = &self.data[offset as usize..offset as usize + buf.len()];
        buf.copy_from_slice(data);
        Ok(buf.len())
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        self.read_at(buf, offset)?;
        Ok(())
    }
}

impl WritableFile for MemFs {
    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.data.extend_from_slice(data);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        Ok(())
    }
}

impl SequencialFile for MemFs {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        todo!()
    }

    fn skip(&mut self, n: usize) -> Result<()> {
        todo!()
    }

    fn read_all(&mut self, buf: &mut String) -> Result<()> {
        todo!()
    }
}

#[derive(Clone, Copy)]
pub struct MemEnv {}

impl Env for MemEnv {
    type RandomAccessFile = MemFs;

    type WritableFile = MemFs;

    type SequencialFile = MemFs;

    fn new_random_access_file(&self, name: &Path) -> Result<Self::RandomAccessFile> {
        Ok(MemFs { data: Vec::new() })
    }

    fn new_writable_file(&self, name: &Path) -> Result<Self::WritableFile> {
        Ok(MemFs { data: Vec::new() })
    }

    fn file_size(&self, path: &Path) -> Result<usize> {
        todo!()
    }

    fn new_sequential_file(&self, name: &Path) -> Result<Self::SequencialFile> {
        todo!()
    }

    fn delete_file(&self, path: &Path) -> Result<()> {
        todo!()
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        todo!()
    }

    fn file_exists(&self, path: &Path) -> bool {
        todo!()
    }

    fn rename_file(&self,from:&Path, to:&Path)-> Result<()> {
        todo!()
    }

    fn get_children(&self, path: &Path, files: &mut Vec<String>)-> Result<()> {
        todo!()
    }
}
