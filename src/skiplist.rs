use rand::{thread_rng, Rng};
use std::{cmp::Ordering, mem::replace, rc::Rc, sync::Arc, vec};

use crate::cmp::Comparator;

const MAX_HEIGHT: usize = 12;
const BRANCHING_FACTOR: usize = 4;

pub struct Node<T> {
    skips: Vec<Option<*mut Node<T>>>,
    next: Option<Box<Node<T>>>,
    key: T,
}
impl<T> Node<T> {
    pub fn next(&self) -> Option<*const Node<T>> {
        if self.next.is_some() {
            Some(self.next.as_ref().unwrap().as_ref() as *const Node<T>)
        } else {
            None
        }
    }
}

impl<T: Default + AsRef<[u8]>> Node<T> {
    fn empty() -> Node<T> {
        Node {
            skips: vec![None; MAX_HEIGHT],
            next: None,
            key: T::default(),
        }
    }
}

pub struct SkipList<T> {
    head: Box<Node<T>>,
    comparator: Rc<dyn Comparator>,
}

impl<T: Default + AsRef<[u8]>> SkipList<T> {
    pub fn new(comparator: Rc<dyn Comparator>) -> Self {
        SkipList {
            comparator: comparator.clone(),
            head: Box::new(Node::empty()),
        }
    }

    fn random_height(&self) -> usize {
        let mut height = 1;
        while height < MAX_HEIGHT && thread_rng().gen_range(0..BRANCHING_FACTOR) == 0 {
            height += 1;
        }
        height
    }

    // fn contains(&self, key: impl AsRef<[u8]>) -> bool {
    //     let key = key.as_ref();
    //     if let Some(n) = self.get_greater_or_equal(key) {
    //         n.key.starts_with(key.as_ref())
    //     } else {
    //         false
    //     }
    // }

    pub fn find_last(&self) -> Option<*const Node<T>> {
        let head = self.head.as_ref() as *const Node<T>;
        let mut level = MAX_HEIGHT - 1;
        let mut current = head;
        loop {
            let next = unsafe { (*current).next.as_ref() };
            if let Some(n) = next {
                current = n.as_ref() as *const Node<T>;
            } else {
                if level == 0 {
                    if current == head {
                        return None;
                    } else {
                        return Some(current);
                    }
                }
                level -= 1;
            }
        }
    }

    pub fn find_less_than(&self, key: impl AsRef<[u8]>) -> Option<*const Node<T>> {
        let key = key.as_ref();
        let mut current = self.head.as_ref() as *const Node<T>;
        let mut level = self.head.skips.len() - 1;
        loop {
            unsafe {
                if let Some(next) = (*current).skips[level] {
                    let ord = self.comparator.compare((*next).key.as_ref(), key);
                    match ord {
                        Ordering::Less => {
                            current = next;
                        }
                        Ordering::Equal | Ordering::Greater => return Some(current),
                    }
                } else {
                    if level == 0 {
                        return Some(current);
                    }
                    level -= 1
                }
            }
        }
    }
    pub fn get_greater_or_equal(&self, key: impl AsRef<[u8]>) -> Option<*const Node<T>> {
        let key = key.as_ref();

        let mut current = self.head.as_ref() as *const Node<T>;
        let mut level = self.head.skips.len() - 1;
        loop {
            unsafe {
                if let Some(next) = (*current).skips[level] {
                    let ord = self.comparator.compare((*next).key.as_ref(), key);
                    match ord {
                        Ordering::Less => {
                            current = next;
                            continue;
                        }
                        Ordering::Equal => return Some(&*next),
                        Ordering::Greater => {
                            if level == 0 {
                                return Some(&*next);
                            }
                        }
                    }
                }
            }
            if level == 0 {
                break;
            }
            level -= 1;
        }

        unsafe {
            if current.is_null()
                || current == self.head.as_ref() as *const Node<T>
                || self.comparator.compare((*current).key.as_ref(), key) == Ordering::Less
            {
                None
            } else {
                Some(current)
            }
        }
    }

    pub fn insert(&self, key: T) {
        let bytes = key.as_ref();
        let mut prevs: [Option<*mut Node<T>>; MAX_HEIGHT] = [None; MAX_HEIGHT];
        let new_height = self.random_height();

        let prevs = &mut prevs[0..new_height];
        let mut level = MAX_HEIGHT - 1;
        let mut current = self.head.as_ref() as *const Node<T> as *mut Node<T>;

        for item in prevs.iter_mut() {
            *item = Some(current);
        }

        loop {
            unsafe {
                if let Some(next) = (*current).skips[level] {
                    let next_key = (*next).key.as_ref();
                    let ord = self.comparator.compare(next_key, bytes);
                    assert!(ord != Ordering::Equal);

                    if ord == Ordering::Less {
                        current = next;
                        continue;
                    }
                }
            }

            if level < new_height {
                prevs[level] = Some(current);
            }

            if level == 0 {
                break;
            }
            level -= 1;
        }

        let new_skips = vec![None; new_height];

        let mut new_node = Box::new(Node {
            skips: new_skips,
            next: None,
            key,
        });

        let new_ptr = new_node.as_mut() as *mut Node<T>;
        for (i, item) in prevs.iter().enumerate().take(new_height) {
            if let Some(prev) = *item {
                unsafe {
                    new_node.skips[i] = (*prev).skips[i];
                    (*prev).skips[i] = Some(new_ptr);
                }
            }
        }
        unsafe {
            new_node.next = replace(&mut (*current).next, None);
            let _ = replace(&mut (*current).next, Some(new_node));
        }
    }
}

pub struct SkipListIter<T: Default + AsRef<[u8]>> {
    map: Arc<SkipList<T>>,
    current: Option<*const Node<T>>,
}
impl<T: Default + AsRef<[u8]>> SkipListIter<T> {
    pub fn new(map: Arc<SkipList<T>>) -> Self {
        SkipListIter { map, current: None }
    }
}

impl<T: Default + AsRef<[u8]>> SkipListIter<T> {
    pub fn valid(&self) -> bool {
        self.current.is_some()
    }

    pub fn seek_to_first(&mut self) {
        // self.current = Some(self.map.head.as_ref() as *const Node<T>);
        self.current = self.map.head.next()
    }

    pub fn seek_to_last(&mut self) {
        self.current = self.map.find_last();
    }

    pub fn seek(&mut self, target: &[u8]) {
        self.current = self.map.get_greater_or_equal(target);
    }

    pub fn next(&mut self) {
        assert!(self.valid());
        self.current = unsafe {
            (*self.current.unwrap())
                .next
                .as_ref()
                .map(|r| r.as_ref() as *const Node<T>)
        }
    }

    pub fn prev(&mut self) {
        assert!(self.valid());
        self.current = self.map.find_less_than(self.key());
    }

    pub fn key(&self) -> &[u8] {
        assert!(self.valid());
        let node = self.current.unwrap();
        unsafe { (*node).key.as_ref() }
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_skipmapinner_insert() {
//         let mut m = SkipMapInner::new();
//         let datas = &["liuzhenzhong", "xinghua"];

//         for i in datas {
//             m.insert(*i, *i);
//         }

//         for i in datas {
//             let v = m.get_greater_or_equal(*i);
//             assert!(matches!(v, Some(_)));

//             assert!((*i).as_bytes().cmp(v.unwrap().key.as_slice()) == Ordering::Equal);

//             // assert!(m.contains(*i));
//         }
//     }
//     #[test]
//     fn test_skipmap_iter() {
//         let m = SkipMap::new();
//         let datas = &["liuzhenzhong", "xinghua"];

//         for i in datas {
//             m.insert(*i, *i);
//         }

//         let mut i = 0;
//         let mut iter = m.iter();
//         while let Some(v) = iter.next() {
//             assert!(v.0.as_slice().cmp(datas[i].as_bytes()) == Ordering::Equal);
//             i += 1;
//         }

//         assert!(i == datas.len());
//     }
// }
