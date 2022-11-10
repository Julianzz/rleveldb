use super::{Env, RandomAccessFile, SequencialFile, WritableFile};
use crate::error::Result;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::{
    fs::{self, File},
    io::{Read, Write},
    path::Path,
};

pub struct PosixFile(File);

#[cfg(unix)]
impl RandomAccessFile for PosixFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.0.read_at(buf, offset).map_err(From::from)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        self.0.read_exact_at(buf, offset).map_err(From::from)
    }
}

#[cfg(unix)]
impl WritableFile for PosixFile {
    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.0.write_all(data).map_err(From::from)
    }

    fn flush(&mut self) -> Result<()> {
        self.0.flush().map_err(From::from)
    }

    fn sync(&mut self) -> Result<()> {
        self.0.sync_all().map_err(From::from)
    }
}

#[cfg(unix)]
impl SequencialFile for PosixFile {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.0.read_exact(buf).map_err(From::from)
    }
    fn read_all(&mut self, buf: &mut String) -> Result<()> {
        self.0.read_to_string(buf).map(|_| ()).map_err(From::from)
    }

    fn skip(&mut self, n: usize) -> Result<()> {
        todo!()
    }
}

#[derive(Clone, Copy)]
pub struct PosixEnv {}

impl Env for PosixEnv {
    type RandomAccessFile = PosixFile;
    type WritableFile = PosixFile;
    type SequencialFile = PosixFile;

    fn new_random_access_file(&self, name: &Path) -> Result<Self::RandomAccessFile> {
        Ok(PosixFile(fs::OpenOptions::new().read(true).open(name)?))
    }

    fn file_size(&self, path: &Path) -> Result<usize> {
        let meta = fs::metadata(path)?;
        Ok(meta.len() as usize)
    }

    fn new_writable_file(&self, path: &Path) -> Result<Self::WritableFile> {
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(false)
            .open(path)?;
        Ok(PosixFile(file))
    }

    fn new_sequential_file(&self, path: &Path) -> Result<Self::SequencialFile> {
        let file = fs::OpenOptions::new().read(true).write(false).open(path)?;
        Ok(PosixFile(file))
    }

    fn delete_file(&self, path: &Path) -> Result<()> {
        fs::remove_file(path)?;
        Ok(())
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        fs::create_dir(path).map_err(From::from)
    }

    fn file_exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn rename_file(&self, from: &Path, to: &Path) -> Result<()> {
        fs::rename(from, to).map_err(From::from)
    }

    fn get_children(&self, path: &Path, files: &mut Vec<String>) -> Result<()> {
        for file in fs::read_dir(path)? {
            if file.is_ok() {
                files.push(file.unwrap().file_name().into_string().unwrap())
            }
        }
        Ok(())
    }
}
