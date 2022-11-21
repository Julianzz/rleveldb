use std::path::{Path, PathBuf};

use crate::{
    env::{write_string_to_file_sync, Env},
    error::{Error, Result},
};

pub type FileNum = u64;

// pub struct FileMeta {
//     pub num: FileNum,
//     pub size: usize,
// }

const CURRENT: &str = "CURRENT";
const LOCK: &str = "LOCK";

#[derive(PartialEq, Copy, Clone)]
pub enum FileType {
    Log,
    DBLock,
    Table,
    Descriptor,
    Current,
    Temp,
    InfoLog,
}

pub fn parse_file_name<P: AsRef<Path>>(f: P) -> Result<(FileNum, FileType)> {
    let f = f.as_ref().to_str().unwrap();
    if f == CURRENT {
        Ok((0, FileType::Current))
    } else if f == LOCK {
        Ok((0, FileType::DBLock))
    } else if f == "LOG" || f == "LOG.old" {
        Ok((0, FileType::InfoLog))
    } else if f.starts_with("MANIFEST-") {
        if let Some(ix) = f.find('-') {
            if let Ok(num) = FileNum::from_str_radix(&f[ix + 1..], 10) {
                Ok((num, FileType::Descriptor))
            } else {
                Err(Error::InvalidArgument(
                    "manifest file number is invalid".into(),
                ))
            }
        } else {
            Err(Error::InvalidArgument("manifest file format wrong".into()))
        }
    } else if let Some(ix) = f.find('.') {
        if let Ok(num) = FileNum::from_str_radix(&f[..ix], 10) {
            match &f[ix + 1..] {
                "log" => Ok((num, FileType::Log)),
                "sst" | "ldb" => Ok((num, FileType::Table)),
                "dbtmp" => Ok((num, FileType::Temp)),
                _ => Err(Error::InvalidArgument("unknow file extension".into())),
            }
        } else {
            Err(Error::InvalidArgument("invalid file num for table".into()))
        }
    } else {
        Err(Error::InvalidArgument("unknown file type".into()))
    }
}

pub fn table_file_name<P: AsRef<Path>>(name: P, num: FileNum) -> PathBuf {
    assert!(num > 0);
    name.as_ref().join(format!("{:06}.ldb", num))
}

pub fn log_file_name<P: AsRef<Path>>(name: P, num: FileNum) -> PathBuf {
    name.as_ref().join(format!("{:0>6}.log", num))
}

pub fn sst_table_file_name<P: AsRef<Path>>(name: P, num: FileNum) -> PathBuf {
    name.as_ref().join(format!("{:0>6}.sst", num))
}

pub fn descriptor_file_name<P: AsRef<Path>>(name: P, num: FileNum) -> PathBuf {
    name.as_ref().join(format!("MANIFEST-{:0>6}", num))
}
pub fn temp_file_name<P: AsRef<Path>>(name: P, num: FileNum) -> PathBuf {
    name.as_ref().join(format!("{:0>6}.dbtmp", num))
}

pub fn current_file_name<P: AsRef<Path>>(name: P) -> PathBuf {
    name.as_ref().join("CURRENT")
}

pub fn lock_file_name<P: AsRef<Path>>(name: P) -> PathBuf {
    name.as_ref().join("LOCK")
}

pub fn info_log_file_name<P: AsRef<Path>>(name: P) -> PathBuf {
    name.as_ref().join("LOG")
}

pub fn old_info_log_file_name<P: AsRef<Path>>(name: P) -> PathBuf {
    name.as_ref().join("LOG.old")
}

pub fn set_current_file<E: Env>(env: E, db_name: &String, descriptor_num: u64) -> Result<()> {
    let manifest = descriptor_file_name(db_name, descriptor_num);
    let mut content = manifest.to_str().unwrap()[db_name.len() + 1..].to_owned();
    content.push('\n');
    let tmp = temp_file_name(db_name, descriptor_num);

    let res = write_string_to_file_sync(env.clone(), content.as_bytes(), &tmp);
    if res.is_ok() {
        Ok(env.rename_file(&tmp, &current_file_name(db_name))?)
    } else {
        Ok(env.delete_file(&tmp)?)
    }
}
