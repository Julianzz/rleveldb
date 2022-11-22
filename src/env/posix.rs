use super::{Env, IoResult, RandomAccessFile, SequencialFile, WritableFile};
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
    fn read_at(&self, buf: &mut [u8], offset: u64) -> IoResult<usize> {
        Ok(self.0.read_at(buf, offset)?)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> IoResult<()> {
        Ok(self.0.read_exact_at(buf, offset)?)
    }
}

#[cfg(unix)]
impl WritableFile for PosixFile {
    fn append(&mut self, data: &[u8]) -> IoResult<()> {
        Ok(self.0.write_all(data)?)
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(self.0.flush()?)
    }

    fn sync(&mut self) -> IoResult<()> {
        Ok(self.0.sync_all()?)
    }
}

#[cfg(unix)]
impl SequencialFile for PosixFile {
    fn read_exact(&mut self, buf: &mut [u8]) -> IoResult<()> {
        Ok(self.0.read_exact(buf)?)
    }
    fn read_to_string(&mut self, buf: &mut String) -> IoResult<()> {
        Ok(self.0.read_to_string(buf).map(|_| ())?)
    }
}

#[cfg(unix)]
#[derive(Clone, Copy)]
pub struct PosixEnv {}

#[cfg(unix)]
impl Env for PosixEnv {
    type RandomAccessFile = PosixFile;
    type WritableFile = PosixFile;
    type SequencialFile = PosixFile;

    fn new_random_access_file(&self, name: &Path) -> IoResult<Self::RandomAccessFile> {
        Ok(PosixFile(fs::OpenOptions::new().read(true).open(name)?))
    }

    fn file_size(&self, path: &Path) -> IoResult<usize> {
        let meta = fs::metadata(path)?;
        Ok(meta.len() as usize)
    }

    fn new_writable_file(&self, path: &Path) -> IoResult<Self::WritableFile> {
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(false)
            .open(path)?;
        Ok(PosixFile(file))
    }

    fn new_sequential_file(&self, path: &Path) -> IoResult<Self::SequencialFile> {
        let file = fs::OpenOptions::new().read(true).write(false).open(path)?;
        Ok(PosixFile(file))
    }

    fn delete_file(&self, path: &Path) -> IoResult<()> {
        Ok(fs::remove_file(path)?)
    }

    fn create_dir(&self, path: &Path) -> IoResult<()> {
        Ok(fs::create_dir(path)?)
    }

    fn file_exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn rename_file(&self, from: &Path, to: &Path) -> IoResult<()> {
        Ok(fs::rename(from, to)?)
    }

    fn get_children(&self, path: &Path, files: &mut Vec<String>) -> IoResult<()> {
        for file in fs::read_dir(path)? {
            if let Ok(f) = file {
                files.push(f.file_name().into_string().unwrap())
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;

    #[test]
    fn test_create_get_children() {
        let env = PosixEnv {};
        let file_names = &mut ["demo", "demo2", "demo4"];
        file_names.sort();

        let tmp_dir = TempDir::new("example").unwrap();
        for file_name in file_names.iter() {
            let file_path = tmp_dir.path().join(*file_name);
            let mut file = env.new_writable_file(&file_path).unwrap();
            file.append(file_name.as_bytes()).unwrap()
        }
        let mut files = Vec::new();
        env.get_children(tmp_dir.path(), &mut files).unwrap();
        files.sort();

        assert_eq!(files.len(), file_names.len());
        assert!(files.iter().zip(file_names.iter()).map(|(f, s)| f == *s).all(|t|t));

        for file_name in file_names.iter() {
            let file_path = tmp_dir.path().join(*file_name);
            assert!(env.file_exists(&file_path));

            let mut file = env.new_sequential_file(&file_path).unwrap();
            let mut content = String::new();
            file.read_to_string(&mut content).unwrap();

            assert_eq!(content,*file_name );
        }

        



    }
}
