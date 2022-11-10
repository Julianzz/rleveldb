use std::{rc::Rc, sync::Arc};

use crate::{
    cmp::{BitWiseComparator, Comparator},
    filter::FilterPolicy,
    table::block::Block,
    utils::cache::Cache,
};

#[derive(Clone, Copy)]
pub enum Compress {
    NO = 0x0,
    Snappy = 0x1,
}

impl Compress {
    pub fn as_byte(&self) -> u8 {
        *self as u8
    }
}

impl From<u8> for Compress {
    fn from(v: u8) -> Self {
        match v {
            0x0 => Self::NO,
            0x1 => Self::Snappy,
            _ => panic!("unknow compress type"),
        }
    }
}

#[derive(Clone)]
pub struct Options {
    pub comparator: Arc<dyn Comparator>,
    pub filter_policy: Option<Arc<dyn FilterPolicy>>,

    pub block_restart_interval: u32,
    pub block_size: usize,
    pub max_open_files: u64,
    pub max_file_size: usize,
    pub write_buffer_size: u64,

    pub compression_type: Compress,
    // pub env: Rc<Box<dyn Env>>,
    pub paranoid_checks: bool,
    pub reuse_log: bool,
    pub error_if_exists: bool,
    pub create_if_missing: bool,

    pub block_cache: Option<Arc<dyn Cache<Vec<u8>, Block>>>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            comparator: Arc::new(BitWiseComparator {}),
            block_size: 4 * 1024,
            block_restart_interval: 16,
            max_file_size: 2 * 1024 * 1024,
            max_open_files: 1000,
            compression_type: Compress::NO,
            paranoid_checks: false,
            block_cache: None,
            filter_policy: None,
            write_buffer_size: 4 * 1024 * 1024,
            reuse_log: false,
            error_if_exists: false,
            create_if_missing: false,
        }
    }
}

#[derive(Clone,Default)]
pub struct ReadOption {
    pub verify_checksum: bool,
    pub fill_cache: bool,
}

#[derive(Clone, Default)]
pub struct WriteOption {
    pub sync: bool,
}
