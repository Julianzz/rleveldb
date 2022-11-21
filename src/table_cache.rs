use std::sync::Arc;

use byteorder::{LittleEndian, WriteBytesExt};

use crate::{
    env::Env,
    error::Result,
    filenames::{sst_table_file_name, table_file_name},
    options::Options,
    sstable::Table,
};

#[derive(Clone)]
pub struct TableCache<E> {
    env: E,
    dbname: String,
    options: Arc<Options>,
    size: u64,
}

impl<E: Env> TableCache<E> {
    /// Creates a new [`TableCache<E>`].
    pub fn new(dbname: String, options: Arc<Options>, env: E, size: u64) -> Self {
        TableCache {
            dbname,
            env,
            options,
            size,
        }
    }

    pub fn find_table(
        &self,
        file_number: u64,
        file_size: u64,
    ) -> Result<Arc<Table<E::RandomAccessFile>>> {
        let mut key = Vec::with_capacity(8);
        key.write_u64::<LittleEndian>(file_number).unwrap();

        let file = self.open_table_file(file_number)?;
        let table = Table::open(self.options.clone(), file, file_size)?;
        Ok(Arc::new(table))
    }

    pub fn open_table_file(&self, file_number: u64) -> Result<E::RandomAccessFile> {
        let file_name = table_file_name(&self.dbname, file_number);
        if let Ok(file) = self.env.new_random_access_file(&file_name) {
            Ok(file)
        } else {
            let old_file_name = sst_table_file_name(&self.dbname, file_number);
            Ok(self.env.new_random_access_file(&old_file_name)?)
        }
    }
}
