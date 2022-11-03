use std::sync::Arc;

use crate::{
    codec::{NumberDecoder, NumberEncoder},
    filter::FilterPolicy,
    slice::UnsafeSlice,
};

const FILTER_BASE_LG: usize = 11;
const FILTER_BASE: usize = 1 << FILTER_BASE_LG;

pub struct FilterBlockBuilder {
    policy: Arc<dyn FilterPolicy>,
    keys: Vec<u8>,
    start: Vec<usize>,
    result: Vec<u8>,
    tmp_keys: Vec<UnsafeSlice>,
    filter_offset: Vec<usize>,
}

impl FilterBlockBuilder {
    pub fn new(policy: Arc<dyn FilterPolicy>) -> Self {
        FilterBlockBuilder {
            policy: policy,
            keys: Vec::new(),
            start: Vec::new(),
            result: Vec::new(),
            tmp_keys: Vec::new(),
            filter_offset: Vec::new(),
        }
    }

    pub fn add_key(&mut self, key: &[u8]) {
        self.start.push(self.keys.len());
        self.keys.extend_from_slice(key);
    }

    pub fn start_block(&mut self, block_offset: usize) {
        let filter_index = block_offset / FILTER_BASE;
        assert!(filter_index >= self.filter_offset.len());

        while filter_index > self.filter_offset.len() {
            self.generate_filter();
        }
    }

    pub fn finish(mut self) -> Vec<u8> {
        if self.start.is_empty() {
            self.generate_filter();
        }

        let array_size = self.result.len();
        for offset in self.filter_offset.iter() {
            self.result.encode_u32_le(*offset as u32).unwrap();
        }

        self.result.encode_u32_le(array_size as u32).unwrap();
        self.result.push(FILTER_BASE_LG as u8);

        self.result
    }
    fn generate_filter(&mut self) {
        let num_keys = self.start.len();
        if num_keys == 0 {
            self.filter_offset.push(self.result.len());
            return;
        }

        self.start.push(self.keys.len());
        self.tmp_keys.resize(num_keys, Default::default());
        for i in 0..num_keys {
            let (begin, end) = (self.start[i], self.start[i + 1]);
            self.tmp_keys[i] = UnsafeSlice::new(self.keys[begin..end].as_ptr(), end - begin);
        }

        self.filter_offset.push(self.result.len());
        self.policy.create_filter(&self.tmp_keys, &mut self.result);

        self.keys.clear();
        self.start.clear();
        self.tmp_keys.clear();
    }
}

pub struct FilterBlockReader<'a> {
    policy: Arc<dyn FilterPolicy>,
    data: &'a [u8],
    offset: usize,
    num: usize,
    base_lg: usize,
}

impl<'a> FilterBlockReader<'a> {
    pub fn new(policy: Arc<dyn FilterPolicy>, data: &'a [u8]) -> Self {
        let mut reader = FilterBlockReader {
            policy,
            data: Default::default(),
            offset: 0,
            num: 0,
            base_lg: 0,
        };

        let n = data.len();
        if n < 5 {
            return reader;
        }

        reader.base_lg = data[n - 1] as usize;
        let last_word = data[n - 5..].as_ref().decode_u32_le().unwrap() as usize;
        if last_word > n - 5 {
            return reader;
        }
        reader.offset = last_word;
        reader.data = data;
        reader.num = (n - 5 - last_word) / 4;
        reader
    }
    pub fn key_may_match(&self, block_offset: usize, key: &[u8]) -> bool {
        let index = block_offset >> self.base_lg;
        if index < self.num {
            let start = self.data[self.offset + index * 4..]
                .as_ref()
                .decode_u32_le()
                .unwrap();
            let limit = self.data[self.offset + index * 4 + 4..]
                .as_ref()
                .decode_u32_le()
                .unwrap();

            if start <= limit && limit <= self.offset as u32 {
                let n = (limit - start) as usize;
                let p = &self.data[start as usize..start as usize + n];
                return self.policy.key_match(key, p);
            } else if start == limit {
                // empty filter
                return false;
            }
        }
        true
    }
}
