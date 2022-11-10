use std::{cmp, sync::Arc};

use byteorder::{LittleEndian, ReadBytesExt};
use bytes::BufMut;

use crate::{slice::UnsafeSlice, utils::hash::bloom_hash};

pub trait FilterPolicy {
    fn name(&self) -> &'static str;

    fn create_filter(&self, keys: &Vec<UnsafeSlice>, dst: &mut Vec<u8>);

    fn key_match(&self, key: &[u8], filter: &[u8]) -> bool;
}


pub struct BloomFilterPolicy {
    bits_per_key: usize,
    hash_num: usize,
}

impl BloomFilterPolicy {
    pub fn new(bits_per_key: usize) -> Self {
        let mut hash_num = (bits_per_key as f64 * 0.69) as usize;
        hash_num = cmp::max(1, hash_num);
        hash_num = cmp::min(30, hash_num);

        BloomFilterPolicy {
            bits_per_key,
            hash_num,
        }
    }
}

impl FilterPolicy for BloomFilterPolicy {
    fn name(&self) -> &'static str {
        "leveldb.BuiltinBloomFilter2"
    }

    fn create_filter(&self, keys: &Vec<UnsafeSlice>, dst: &mut Vec<u8>) {
        let mut bits = keys.len() * self.bits_per_key;
        bits = cmp::min(bits, 64);

        let bytes = (bits + 7) / 8;
        bits = bytes * 8;
        let init_size = dst.len();
        dst.resize(init_size + bytes + 1, 0);
        *dst.last_mut().unwrap() = self.hash_num as u8;

        let (_, data) = dst.split_at_mut(init_size);
        for &key in keys {
            let mut h = bloom_hash(unsafe { key.as_ref() }) as usize;
            let delta = (h >> 7) | (h << 15);
            for _ in 0..self.hash_num {
                let bitpos = h % bits;
                data[bitpos / 8] |= 1 << (bitpos % 8) as u8;
                h += delta;
            }
        }
    }

    fn key_match(&self, key: &[u8], filter: &[u8]) -> bool {
        let len = filter.len();
        if len < 2 {
            return false;
        }
        let bits = (len - 1) * 8;
        let hash_num = filter[filter.len() - 1] as usize;
        if hash_num > 30 {
            return true;
        }
        let mut h = bloom_hash(key) as usize;
        let delta = (h >> 7) | (h << 15);
        for _ in 0..hash_num {
            let bitpos = h % bits;
            if filter[bitpos / 8] & (1 << (bitpos % 8) as u8) == 0 {
                return false;
            }
            h += delta;
        }

        true
    }
}

struct BloomFilterFactory {
    keys: Vec<UnsafeSlice>,
    policy: Arc<dyn FilterPolicy>,
}

impl BloomFilterFactory {
    pub fn new(bit_per_key: usize) -> Self {
        BloomFilterFactory {
            keys: Vec::new(),
            policy: Arc::new(BloomFilterPolicy::new(bit_per_key)),
        }
    }

    pub fn add(&mut self, k: UnsafeSlice) {
        self.keys.push(k);
    }

    pub fn build(self, filter: &mut Vec<u8>) {
        self.policy.create_filter(&self.keys, filter);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct BloomTest {}

    #[test]
    fn test_small() {
        let mut factory = BloomFilterFactory::new(10);
        factory.add("hello".into());
        factory.add("time".into());
        let mut filter = Vec::new();
        factory.build(&mut filter);

        let policy = BloomFilterPolicy::new(10);
        assert!(policy.key_match("hello".as_bytes(), &filter));
        assert!(policy.key_match("time".as_bytes(), &filter));
    }
}
