// use std::{
//     fs::File,
//     path::{Path, PathBuf},
// };

// use crate::{
//     env,
//     error::{Error, Result},
//     filenames::{table_file_name, FileNum},
// };

// use super::{options::Options, table_reader::Table};

// pub struct TableCache {
//     dbname: PathBuf,
//     opts: Options,
// }

// impl TableCache {
//     pub fn new<P: AsRef<Path>>(db: P, opt: Options, entries: usize) -> TableCache {
//         TableCache {
//             dbname: db.as_ref().to_owned(),
//             opts: opt,
//         }
//     }
//     pub fn get_table(&mut self, file_num: FileNum) -> Result<Table<File>> {
//         self.open_table(file_num)
//     }

//     // pub fn get(&mut self, file_num: FileNum)
//     pub fn open_table(&mut self, file_num: FileNum) -> Result<Table<File>> {
//         let name = table_file_name(&self.dbname, file_num);
//         let path = name.as_path();
//         let file_size = env::size_of(path)?;
//         if file_size == 0 {
//             return Err(Error::InvalidData(format!("file {} is empty", file_num)));
//         }

//         let file = env::open_random_access_file(path)?;
//         Table::new(file, file_size, self.opts.clone())
//     }
// }
