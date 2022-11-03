use super::format::BlockContent;
use crate::{
    cmp::Comparator,
    codec::{self, NumberDecoder, VarDecoder},
    error::{Error, Result},
    iterator::DBItertor,
    slice::UnsafeSlice,
};
use std::{cmp::Ordering, sync::Arc};

const RESTART_SIZE: usize = 4;
pub struct Block {
    content: Arc<BlockContent>,
    restart_offset: u32,
    num_restarts: u32,
}

impl Block {
    pub fn from_raw(content: BlockContent) -> Result<Self> {
        let n = content.len();
        if n < RESTART_SIZE {
            return Err(Error::Corruption(
                "bad block contents, size too small".into(),
            ));
        }
        let num_restarts = codec::decode_u32_le(&content[n - 4..]);
        let max_restart_allowed = (n - RESTART_SIZE) / RESTART_SIZE;
        if num_restarts as usize > max_restart_allowed {
            Err(Error::Corruption("bac block contents".into()))
        } else {
            Ok(Block {
                content: Arc::new(content),
                restart_offset: n as u32 - num_restarts * RESTART_SIZE as u32 - RESTART_SIZE as u32,
                num_restarts,
            })
        }
    }

    pub fn iter(&self, comparator: Arc<dyn Comparator>) -> BlockIter {
        BlockIter::new(self, comparator)
    }
}

pub struct BlockIter {
    block_content: Arc<BlockContent>,
    comparator: Arc<dyn Comparator>,

    restarts: u32,
    num_restarts: u32,

    current: u32,
    restart_index: u32,
    key: Vec<u8>,
    value: UnsafeSlice,
    err: Option<Error>,
}

impl BlockIter {
    pub fn new(block: &Block, comparator: Arc<dyn Comparator>) -> Self {
        BlockIter {
            block_content: block.content.clone(),
            comparator,

            restarts: block.restart_offset,
            num_restarts: block.num_restarts,

            current: block.restart_offset,
            restart_index: block.num_restarts,

            key: Vec::new(),
            value: Default::default(),
            err: None,
        }
    }

    pub fn next_entry_offset(&self) -> u32 {
        let offset = unsafe {
            self.value
                .data()
                .add(self.value.size())
                // .offset(self.value.size() as isize)
                .offset_from(self.block_content.data.as_ptr())
        };
        offset as u32
    }

    pub fn seek_to_restart_point(&mut self, index: u32) {
        self.key.clear();
        self.restart_index = index;
        self.current = self.get_restart_point(index);
        let ptr = unsafe { self.block_content.as_ptr().offset(self.current as isize) };
        self.value = UnsafeSlice::new(ptr, 0)
    }

    pub fn decode_entry(&self, offset: u32) -> Result<(u32, u32, u32, u32)> {
        if self.restarts - offset < 3 {
            return Err(Error::Corruption("bad entry in block".into()));
        }
        let mut data = &self.block_content[offset as usize..];
        let mut step = data.len();

        let (mut shared, mut non_shared, mut value_len) =
            (data[0] as u32, data[1] as u32, data[2] as u32);
        if shared | non_shared | value_len < 128 {
            // Fast path: all three values are encoded in one byte each
            step = 3
        } else {
            shared = data.decode_var_u32()?;
            non_shared = data.decode_var_u32()?;
            value_len = data.decode_var_u32()?;
            step -= data.len();
        }

        //check
        let remain = self.restarts - offset - step as u32;
        if remain < non_shared + value_len {
            return Err(Error::Corruption("bad entry in block".into()));
        }

        Ok((shared, non_shared, value_len, step as u32))
    }

    fn get_restart_point(&self, index: u32) -> u32 {
        assert!(index < self.num_restarts);
        let offset = self.restarts as usize + RESTART_SIZE * index as usize;
        let mut buf = &self.block_content.data[offset..];
        buf.decode_u32_le().unwrap()
    }

    fn parse_next_entry(&mut self) -> bool {
        self.current = self.next_entry_offset();
        if self.current >= self.restarts {
            self.current = self.restarts;
            self.restart_index = self.num_restarts;
            return false;
        }

        if let Ok((shared, non_shared, value_len, step)) = self.decode_entry(self.current) {
            let offset = (self.current + step) as usize;
            let mut buf = self.block_content[offset..].as_ref();

            let non_shared_key = &buf[..non_shared as usize];
            self.key.truncate(shared as usize);
            self.key.extend_from_slice(non_shared_key);

            buf = &buf[non_shared as usize..];
            self.value = buf[..value_len as usize].into();
            while self.restart_index + 1 < self.num_restarts
                && self.get_restart_point(self.restart_index + 1) < self.current
            {
                self.restart_index += 1;
            }
            true
        } else {
            self.corruption_err();
            false
        }
    }

    fn corruption_err(&mut self) {
        self.err
            .get_or_insert(Error::Corruption("bad entry in block".into()));
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        self.comparator.compare(a, b)
    }
}

impl DBItertor for BlockIter {
    fn valid(&self) -> bool {
        self.current < self.restarts
    }

    fn seek_to_first(&mut self) {
        self.seek_to_restart_point(0);
        self.parse_next_entry();
    }

    fn seek_to_last(&mut self) {
        self.seek_to_restart_point(self.num_restarts - 1);
        loop {
            if !self.parse_next_entry() || self.next_entry_offset() >= self.restarts {
                break;
            }
        }
    }

    fn seek(&mut self, target: &[u8]) {
        let (mut left, mut right) = (0, self.num_restarts - 1);
        while left < right {
            let mid = (left + right) / 2;
            let region_offset = self.get_restart_point(mid);
            if let Ok((shared, non_shared, _, step)) = self.decode_entry(region_offset) {
                if shared != 0 {
                    self.corruption_err();
                    return;
                }
                let offset = region_offset + step;
                let buf = self.block_content[offset as usize..].as_ref();
                let key = buf[..non_shared as usize].as_ref();
                if self.compare(key, target) == Ordering::Less {
                    left = mid;
                } else {
                    right = mid - 1;
                }
            } else {
                self.corruption_err();
                return;
            }
        }
        self.seek_to_restart_point(left);
        loop {
            if !self.parse_next_entry() {
                return;
            }
            let key = self.key.as_slice();
            if self.compare(key, target) != Ordering::Less {
                return;
            }
        }
    }

    fn next(&mut self) {
        assert!(self.valid());
        self.parse_next_entry();
    }
    fn prev(&mut self) {
        assert!(self.valid());

        let origin = self.current;
        while self.get_restart_point(self.restart_index) >= origin {
            if self.restart_index == 0 {
                self.current = self.restarts;
                self.restart_index = self.num_restarts;
            }
            self.restart_index -= 1;
        }
        self.seek_to_restart_point(self.restart_index);
        loop {
            if !self.parse_next_entry() || self.next_entry_offset() >= origin {
                break;
            }
        }
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid());
        self.key.as_slice()
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid());
        unsafe { self.value.as_ref() }
    }

    fn status(&mut self) -> Result<()> {
        if self.err.is_some() {
            return Err(self.err.take().unwrap());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{cmp::BitWiseComparator, table::block_builder::BlockBuilder};

    use super::*;

    fn get_data() -> Vec<(&'static [u8], &'static [u8])> {
        vec![
            ("key1".as_bytes(), "value1".as_bytes()),
            (
                "loooooooooooooooooooooooooooooooooongerkey1".as_bytes(),
                "shrtvl1".as_bytes(),
            ),
            ("medium length key 1".as_bytes(), "some value 2".as_bytes()),
            ("prefix_key1".as_bytes(), "value".as_bytes()),
            ("prefix_key2".as_bytes(), "value".as_bytes()),
            ("prefix_key3".as_bytes(), "value".as_bytes()),
        ]
    }

    #[test]
    fn test_block_iterator() {
        let comparator = Arc::new(BitWiseComparator {});

        let mut builder = BlockBuilder::new(comparator.clone(), 3);
        let datas: Vec<(&[u8], &[u8])> = get_data();

        for &(k, v) in datas.iter() {
            builder.add(k, v);
        }
        let contents = builder.finish();
        let block_content = BlockContent::new(contents);
        let block = Block::from_raw(block_content).unwrap();
        let mut iter = block.iter(comparator.clone());

        iter.seek_to_first();
        for (_idx, &(key, val)) in datas.iter().enumerate() {
            assert!(iter.valid());
            assert_eq!(key, iter.key());
            assert_eq!(val, iter.value());
            iter.next();
        }
    }
}
