use std::{
    cmp::{self, Ordering},
    sync::Arc,
};

use integer_encoding::{FixedIntWriter, VarIntWriter};

use crate::cmp::Comparator;

pub struct BlockBuilder {
    comparator: Arc<dyn Comparator>,
    block_restart_interval: u32,
    buffer: Vec<u8>,    // destination buffer
    restarts: Vec<u32>, // restart points
    counter: u32,       // number of entries emitted since restart
    last_key: Vec<u8>,
    restart_counter: u32,
}

impl BlockBuilder {
    pub fn new(comparator: Arc<dyn Comparator>, block_restart_interval: u32) -> Self {
        assert!(block_restart_interval >= 1);
        let mut restarts = vec![0];
        restarts.reserve(1024);

        BlockBuilder {
            comparator,
            block_restart_interval,
            buffer: Vec::new(),
            restarts,
            counter: 0,
            last_key: Vec::new(),
            restart_counter: 0,
        }
    }
    pub fn entries(&self) -> usize {
        self.counter as usize
    }
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
    pub fn current_size_estimate(&self) -> usize {
        self.buffer.len() + 4 * self.restarts.len() + 4
    }

    pub fn last_key(&self) -> &[u8] {
        &self.last_key
    }

    pub fn reset(&mut self) {
        self.buffer.clear();
        self.restarts.clear();
        self.last_key.clear();
        self.restart_counter = 0;
        self.counter = 0;
    }

    pub fn add<T: AsRef<[u8]>>(&mut self, key: T, val: T) {
        let key = key.as_ref();
        let val = val.as_ref();

        assert!(self.restart_counter <= self.block_restart_interval);
        assert!(
            self.buffer.is_empty()
                || self.comparator.compare(key, &self.last_key) == Ordering::Greater
        );

        let mut shared = 0;
        if self.restart_counter < self.block_restart_interval {
            let smallest = cmp::min(key.len(), self.last_key.len());
            while shared < smallest && self.last_key[shared] == key[shared] {
                shared += 1;
            }
        } else {
            self.restarts.push(self.buffer.len() as u32);
            // self.last_key.clear();
            self.restart_counter = 0;
        }

        let no_shared = key.len() - shared;

        self.buffer.write_varint(shared).unwrap();
        self.buffer.write_varint(no_shared).unwrap();
        self.buffer.write_varint(val.len()).unwrap();

        self.buffer.extend_from_slice(&key[shared..]);
        self.buffer.extend_from_slice(val);

        self.last_key.truncate(shared);
        self.last_key.extend_from_slice(&key[shared..]);

        self.restart_counter += 1;
        self.counter += 1;
    }

    pub fn finish(mut self) -> Vec<u8> {
        self.buffer.reserve(self.restarts.len() * 4 + 4);
        for r in self.restarts.iter() {
            self.buffer.write_fixedint(*r).unwrap();
        }
        self.buffer
            .write_fixedint(self.restarts.len() as u32)
            .unwrap();

        self.buffer
    }
}

#[cfg(test)]
mod tests {
    use crate::cmp::BitWiseComparator;

    use super::*;

    fn get_data() {}

    #[test]
    fn test_name() {
        let datas = &[("key1", "value1"), ("prefix1", "value2")];

        let comparator = Arc::new(BitWiseComparator {});
        let mut builder = BlockBuilder::new(comparator, 3);

        for &(k, v) in datas.iter() {
            builder.add(k, v);
        }
        let block = builder.finish();
        assert_eq!(block.len(), 37);
    }
}
