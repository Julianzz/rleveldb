use std::{cmp, sync::Arc};

use byteorder::{LittleEndian, ReadBytesExt};
use bytes::BufMut;

use crate::slice::UnsafeSlice;

pub trait FilterPolicy {
    fn name(&self) -> &'static str;

    fn create_filter(&self, keys: &Vec<UnsafeSlice>, dst: &mut Vec<u8>);

    fn key_match(&self, key: &[u8], filter: &[u8]) -> bool;
}

pub fn hash(data: &[u8], seed: u32) -> u32 {
    // Similar to murmur hash
    let n = data.len();
    let m: u32 = 0xc6a4a793;
    let r: u32 = 24;
    let mut h = seed ^ (m.wrapping_mul(n as u32));
    let mut buf = data;
    while buf.len() >= 4 {
        let w = buf.read_u32::<LittleEndian>().unwrap();
        h = h.wrapping_add(w);
        h = h.wrapping_mul(m);
        h ^= h >> 16;
    }

    for i in (0..buf.len()).rev() {
        h += u32::from(buf[i]) << (i * 8) as u32;
        if i == 0 {
            h = h.wrapping_mul(m);
            h ^= h >> r;
        }
    }
    h
}
fn bloom_hash(key: &[u8]) -> u32 {
    hash(key, 0xbc9f1d34)
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
