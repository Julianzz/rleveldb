#![feature(core_intrinsics)]

#![allow(dead_code)]

mod db;
mod log;
mod memtable;
// mod skiplist;
mod builder;
mod cmp;
mod codec;
mod consts;
mod db_impl;
mod env;
mod error;
mod filenames;
mod filter;
mod format;
mod iterator;
mod options;
mod skiplist;
mod slice;
mod sstable;
mod table_cache;
mod types;
mod utils;
mod version;
mod version_edit;
mod version_set;
mod write_batch;
mod merge;

pub use db_impl::LevelDB;
pub use log::{LogReader, LogWriter};
pub use memtable::{LookupKey, MemTable};
pub use options::{Options, ReadOption, WriteOption};
pub use types::ValueType;
pub use env::posix::PosixEnv;


#[derive(PartialEq)]
pub enum Forward {
    FORWARD = 0,
    BACKWARD = 1,
}
