use std::{rc::Rc, sync::Arc};

use crate::{cmp::Comparator, filter::FilterPolicy};

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

    pub compress_type: Compress,
    // pub env: Rc<Box<dyn Env>>,
}
#[derive(Clone)]
pub struct ReadOption {
    pub verify_checksum: bool,
    pub fill_cache: bool,
}

#[derive(Clone)]
pub struct WriteOption {}
