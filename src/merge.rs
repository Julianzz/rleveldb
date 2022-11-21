use std::cmp::Ordering;

use crate::{cmp::Comparator, error::Result, iterator::DBIterator, slice::UnsafeSlice, Forward};


pub struct MergingIterator<C: Comparator> {
    children: Vec<Box<dyn DBIterator>>,
    current: Option<usize>,
    comparator: C,
    direction: Forward,
}

impl<C: Comparator> MergingIterator<C> {
    pub fn new(comparator: C, children: Vec<Box<dyn DBIterator>>) -> Self {
        MergingIterator {
            children,
            current: None,
            comparator,
            direction: Forward::FORWARD,
        }
    }

    fn find_smallest(&mut self) {
        let mut smallest: Option<usize> = None;
        self.children.iter().enumerate().for_each(|(idx, child)| {
            if child.valid() {
                if let Some(small) = smallest {
                    if self
                        .comparator
                        .compare(child.key(), self.children[small].key())
                        == Ordering::Less
                    {
                        smallest = Some(idx);
                    }
                } else {
                    smallest = Some(idx)
                }
            }
        });
        self.current = smallest;
    }

    fn find_largest(&mut self) {
        let mut largest: Option<usize> = None;
        self.children.iter().enumerate().for_each(|(idx, child)| {
            if child.valid() {
                if let Some(large) = largest {
                    if self
                        .comparator
                        .compare(child.key(), self.children[large].key())
                        == Ordering::Greater
                    {
                        largest = Some(idx);
                    }
                } else {
                    largest = Some(idx)
                }
            }
        });
        self.current = largest;
    }
}

impl<C: Comparator> DBIterator for MergingIterator<C> {
    fn valid(&self) -> bool {
        self.current.is_some()
    }

    fn seek_to_first(&mut self) {
        self.children
            .iter_mut()
            .for_each(|child| child.seek_to_first());
        self.find_smallest();
        self.direction = Forward::FORWARD;
    }

    fn seek_to_last(&mut self) {
        self.children
            .iter_mut()
            .for_each(|child| child.seek_to_last());
        self.find_largest();
        self.direction = Forward::BACKWARD;
    }

    fn seek(&mut self, target: &[u8]) {
        self.children
            .iter_mut()
            .for_each(|child| child.seek(target));
        self.find_smallest();
    }

    fn next(&mut self) {
        assert!(self.valid());

        let current = self.current.unwrap();
        let current_key = UnsafeSlice::new(self.key().as_ptr(), self.key().len());
        if self.direction == Forward::BACKWARD {
            for (pos, child) in self.children.iter_mut().enumerate() {
                if pos != current {
                    child.seek(unsafe { current_key.as_ref() });
                    if child.valid()
                        && self
                            .comparator
                            .compare(unsafe { current_key.as_ref() }, child.key())
                            == Ordering::Equal
                    {
                        child.next();
                    }
                }
            }
            self.direction = Forward::FORWARD;
        }
        self.children[current].next();
        self.find_smallest()
    }

    fn prev(&mut self) {
        assert!(self.valid());

        let current = self.current.unwrap();
        let current_key = UnsafeSlice::new(self.key().as_ptr(), self.key().len());
        if self.direction == Forward::FORWARD {
            self.children
                .iter_mut()
                .enumerate()
                .for_each(|(pos, child)| {
                    if pos != current {
                        child.seek(unsafe { current_key.as_ref() });
                        if child.valid() {
                            child.prev();
                        } else {
                            child.seek_to_last();
                        }
                    }
                });
            self.direction = Forward::BACKWARD;
        }
        self.children[current].prev();
        self.find_largest();
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid());
        self.children[self.current.unwrap()].key()
    }

    fn value(&self) -> &[u8] {
        self.children[self.current.unwrap()].value()
    }

    fn status(&mut self) -> Result<()> {
        for i in 0..self.children.len() {
           self.children[i].status()?;
        }
        Ok(())
    }
}
