pub mod mem;
pub mod posix;

use std::fmt::Display;
use std::io;
use std::ops::Deref;
use std::path::Path;

use thiserror::Error;

#[derive(Error, Debug)]
pub struct IoError(io::Error);
impl From<io::Error> for IoError {
    fn from(err: io::Error) -> Self {
        IoError(err)
    }
}

impl Display for IoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Deref for IoError {
    type Target = io::Error;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub type IoResult<T> = Result<T, IoError>;

pub trait RandomAccessFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> IoResult<usize>;
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> IoResult<()>;
}

pub trait WritableFile {
    fn append(&mut self, data: &[u8]) -> IoResult<()>;
    fn flush(&mut self) -> IoResult<()>;
    fn sync(&mut self) -> IoResult<()>;
}

pub trait SequencialFile {
    fn read_exact(&mut self, buf: &mut [u8]) -> IoResult<()>;
    fn read_to_string(&mut self, buf: &mut String) -> IoResult<()>;
}

pub trait Env: Send + Sync + Clone + 'static {
    type RandomAccessFile: RandomAccessFile + 'static;
    type WritableFile: WritableFile + 'static;
    type SequencialFile: SequencialFile + 'static;

    fn new_random_access_file(&self, name: &Path) -> IoResult<Self::RandomAccessFile>;
    fn new_writable_file(&self, name: &Path) -> IoResult<Self::WritableFile>;
    fn new_sequential_file(&self, name: &Path) -> IoResult<Self::SequencialFile>;

    fn file_size(&self, path: &Path) -> IoResult<usize>;
    fn file_exists(&self, path: &Path) -> bool;

    fn delete_file(&self, path: &Path) -> IoResult<()>;
    fn rename_file(&self, from: &Path, to: &Path) -> IoResult<()>;

    fn create_dir(&self, path: &Path) -> IoResult<()>;
    fn get_children(&self, path: &Path, files: &mut Vec<String>) -> IoResult<()>;
}

pub fn do_write_string_to_file<E: Env>(
    env: E,
    data: &[u8],
    file_name: impl AsRef<Path>,
    sync: bool,
) -> IoResult<()> {
    let file_name = file_name.as_ref();
    let mut file = env.new_writable_file(file_name)?;
    file.append(data)?;
    if sync {
        file.sync()?;
    }
    Ok(())
}

pub fn write_string_to_file<E: Env>(
    env: E,
    data: &[u8],
    file_name: impl AsRef<Path>,
) -> IoResult<()> {
    let file_name = file_name.as_ref();
    let ret = do_write_string_to_file(env.clone(), data, file_name, false);
    if ret.is_err() {
        let _ = env.delete_file(file_name);
    }
    ret
}

pub fn write_string_to_file_sync<E: Env>(
    env: E,
    data: &[u8],
    file_name: impl AsRef<Path>,
) -> IoResult<()> {
    let file_name = file_name.as_ref();
    let ret = do_write_string_to_file(env.clone(), data, file_name, true);
    if ret.is_err() {
        let _ = env.delete_file(file_name);
    }
    ret
}

pub fn read_file_to_vec<E: Env>(
    env: E,
    fname: impl AsRef<Path>,
    data: &mut String,
) -> IoResult<()> {
    data.clear();
    let mut f = env.new_sequential_file(fname.as_ref())?;
    f.read_to_string(data)?;

    Ok(())
}
