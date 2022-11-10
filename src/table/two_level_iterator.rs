use std::{cmp::Ordering, collections::btree_map::Iter};

use rand::seq::index;

use crate::{
    cmp::{BitWiseComparator, Comparator},
    error::{Error, Result},
    iterator::DBIterator,
    options::ReadOption,
};

pub trait BlockIterBuilder {
    type Iter: DBIterator;
    fn build(&self, option: &ReadOption, index_val: &[u8]) -> Result<Self::Iter>;
}

pub struct TwoLevelIterator<I: DBIterator, B: BlockIterBuilder> {
    block_builder: B,
    option: ReadOption,
    index_iter: I,

    data_iter: Option<B::Iter>,
    data_block_handle: Vec<u8>,
    status: Option<Error>,
}

impl<I: DBIterator, B: BlockIterBuilder> TwoLevelIterator<I, B> {
    pub fn new(index_iter: I, block_builder: B, option: ReadOption) -> Self {
        TwoLevelIterator {
            block_builder,
            option,
            index_iter,

            data_iter: None,
            data_block_handle: Vec::new(),
            status: None,
        }
    }

    fn init_data_block(&mut self) {
        if !self.index_iter.valid() {
            self.set_data_iterator(None);
        } else {
            let handle = self.index_iter.value();
            let comparator = BitWiseComparator {};
            if !(self.data_iter.is_some()
                && comparator.compare(handle, &self.data_block_handle) == Ordering::Equal)
            {
                match self.block_builder.build(&self.option, handle) {
                    Ok(data_iter) => {
                        self.data_block_handle.clear();
                        self.data_block_handle.extend_from_slice(handle);
                        self.set_data_iterator(Some(data_iter));
                    }
                    Err(err) => {
                        self.data_iter = None;
                        self.save_err(err);
                    }
                }
            }
        }
    }

    fn set_data_iterator(&mut self, data_iter: Option<B::Iter>) {
        if let Some(ref mut iter) = self.data_iter {
            if let Err(err) = iter.status() {
                self.save_err(err);
            }
        }
        self.data_iter = data_iter;
    }

    fn save_err(&mut self, err: Error) {
        if self.status.is_none() {
            self.status = Some(err)
        }
    }

    fn skip_empty_data_blocks_forward(&mut self) {
        loop {
            if let Some(ref data_iter) = self.data_iter {
                if data_iter.valid() {
                    break;
                }
            }
            if !self.index_iter.valid() {
                self.set_data_iterator(None);
                break;
            }

            self.index_iter.next();
            self.init_data_block();

            if let Some(ref mut iter) = self.data_iter {
                iter.seek_to_first();
            }
        }
    }

    fn skip_empty_data_blocks_backward(&mut self) {
        loop {
            if let Some(ref data_iter) = self.data_iter {
                if data_iter.valid() {
                    break;
                }
            }
            if !self.index_iter.valid() {
                self.set_data_iterator(None);
                break;
            }

            self.index_iter.prev();
            self.init_data_block();
            if let Some(ref mut iter) = self.data_iter {
                iter.seek_to_last();
            }
        }
    }
}

impl<I: DBIterator, B: BlockIterBuilder> DBIterator for TwoLevelIterator<I, B> {
    fn valid(&self) -> bool {
        if let Some(ref iter) = self.data_iter {
            iter.valid()
        } else {
            false
        }
    }

    fn seek_to_first(&mut self) {
        self.index_iter.seek_to_first();
        self.init_data_block();
        if let Some(ref mut iter) = self.data_iter {
            iter.seek_to_first();
        }
        self.skip_empty_data_blocks_forward();
    }

    fn seek_to_last(&mut self) {
        self.index_iter.seek_to_last();
        self.init_data_block();
        if let Some(ref mut iter) = self.data_iter {
            iter.seek_to_last();
        }
        self.skip_empty_data_blocks_backward();
    }

    fn seek(&mut self, target: &[u8]) {
        self.index_iter.seek(target);
        self.init_data_block();

        if let Some(ref mut iter) = self.data_iter {
            iter.seek(target);
        }
        self.skip_empty_data_blocks_forward();
    }

    fn next(&mut self) {
        assert!(self.valid());
        if let Some(ref mut iter) = self.data_iter {
            iter.next();
            self.skip_empty_data_blocks_forward();
        }
    }

    fn prev(&mut self) {
        assert!(self.valid());
        if let Some(ref mut iter) = self.data_iter {
            iter.prev();
            self.skip_empty_data_blocks_backward();
        }
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid());
        let iter = self.data_iter.as_ref().unwrap();
        iter.key()
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid());
        let iter = self.data_iter.as_ref().unwrap();
        iter.value()
    }

    fn status(&mut self) -> Result<()> {
        let _ = self.index_iter.status()?;
        if let Some(ref mut data_iter) = self.data_iter {
            let _ = data_iter.status()?;
        };
        if self.status.is_some() {
            return Err(self.status.take().unwrap());
        }
        Ok(())
    }
}
