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
    fn read_all(&mut self, buf: &mut String) -> Result<()>;
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
    fn delete_file(&self, path: &Path) -> Result<()>;
    fn file_exists(&self, path: &Path) -> bool;
    fn create_dir(&self, path: &Path) -> Result<()>;
    fn rename_file(&self, from: &Path, to: &Path) -> Result<()>;
    fn get_children(&self, path: &Path, files: &mut Vec<String>) -> Result<()>;
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

pub fn do_write_string_to_file<E: Env>(
    env: E,
    data: &[u8],
    file_name: impl AsRef<Path>,
    sync: bool,
) -> Result<()> {
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
) -> Result<()> {
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
) -> Result<()> {
    let file_name = file_name.as_ref();
    let ret = do_write_string_to_file(env.clone(), data, file_name, true);
    if ret.is_err() {
        let _ = env.delete_file(file_name);
    }
    ret
}

pub fn read_file_to_vec<E: Env>(env: E, fname: impl AsRef<Path>, data: &mut String) -> Result<()> {
    data.clear();
    let mut f = env.new_sequential_file(fname.as_ref())?;
    f.read_all(data)?;

    Ok(())
}
