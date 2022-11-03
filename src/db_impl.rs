use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::Result;
use crate::options::{ReadOption, WriteOption};
use crate::version::Version;
use crate::version_edit::VersionEdit;
use crate::MemTable;
use crate::{env::Env, options::Options, write_batch::WriteBatch};

pub struct DB {
    name: Path,
}

pub struct DBImplInner<E> {
    env: E,
}

impl<E: Env> DBImplInner<E> {
    pub fn new(options: Options, db_name: impl AsRef<str>, env: E) -> Self {
        DBImplInner { env }
    }
    pub fn write(&self, options: &WriteOption, updates: Option<WriteBatch>) -> Result<()> {
        todo!()
    }

    pub fn get(&self, option: &ReadOption, key: &[u8], value: &mut Vec<u8>) -> Result<()> {
        todo!()
    }

    fn write_level0_table(
        &self,
        mem: Arc<MemTable>,
        edit: &mut VersionEdit,
        base: Option<Arc<Version>>,
    ) {
    }

    fn new_db(&self) {}

    
}
