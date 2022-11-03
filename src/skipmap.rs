use std::{cell::RefCell, cmp::Ordering, mem::replace, rc::Rc, vec};

use rand::{rngs::StdRng, RngCore, SeedableRng};

const MAX_HEIGHT: usize = 12;
const BRANCHING_FACTOR: u32 = 4;

pub trait Cmp {
    fn cmp(&self, a: &[u8], b: &[u8]);
}
struct Node {
    skips: Vec<Option<*mut Node>>,
    next: Option<Box<Node>>,

    key: Vec<u8>,
    value: Vec<u8>,
}

impl Node {
    fn empty() -> Node {
        Node {
            skips: vec![None; MAX_HEIGHT],
            next: None,
            key: Vec::new(),
            value: Vec::new(),
        }
    }
}

pub struct SkipMap {
    map: Rc<RefCell<SkipMapInner>>,
}

impl SkipMap {
    pub fn new() -> Self {
        SkipMap {
            map: Rc::new(RefCell::new(SkipMapInner::new())),
        }
    }

    pub fn len(&self) -> usize {
        self.map.borrow().len
    }

    // pub fn contains(&self, key: impl AsRef<[u8]>) -> bool {
    //     self.map.borrow().contains(key)
    // }
    pub fn insert(&self, key: impl Into<Vec<u8>>, val: impl Into<Vec<u8>>) {
        self.map.borrow_mut().insert(key, val);
    }

    pub fn iter(&self) -> SkipMapIter {
        SkipMapIter {
            map: self.map.clone(),
            current: Some(self.map.borrow().head.as_ref() as *const Node),
        }
    }
}

pub struct SkipMapIter {
    map: Rc<RefCell<SkipMapInner>>,
    current: Option<*const Node>,
}

// impl DbItertor for SkipMapIter {
//     fn advance(&mut self) -> bool {
//         if let Some(c) = self.current {
//             unsafe {
//                 let next = (*c).next.as_ref();
//                 if let Some(v) = next {
//                     self.current = Some(v.as_ref() as *const Node);
//                     true
//                 } else {
//                     self.current = None;
//                     false
//                 }
//             }
//         } else {
//             false
//         }
//     }

//     fn current(&self, key: &mut Vec<u8>, value: &mut Vec<u8>) -> bool {
//         if let Some(v) = self.current {
//             unsafe {
//                 key.extend_from_slice(&(*v).key);
//                 value.extend_from_slice(&(*v).value);
//             }
//             true
//         } else {
//             false
//         }
//     }

//     fn seek(&mut self, key: &[u8]) {
//         if let Some(node) = self.map.borrow().get_greater_or_equal(key) {
//             self.current = Some(node as *const Node);
//         }
//     }
// }

struct SkipMapInner {
    head: Box<Node>,
    rand: StdRng,
    len: usize,
}

impl SkipMapInner {
    fn new() -> Self {
        SkipMapInner {
            head: Box::new(Node::empty()),
            rand: StdRng::seed_from_u64(0xdeadbeef),
            len: 0,
        }
    }

    fn random_height(&mut self) -> usize {
        let mut height = 1;
        while height < MAX_HEIGHT && self.rand.next_u32() % BRANCHING_FACTOR == 0 {
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

    fn get_greater_or_equal(&self, key: impl AsRef<[u8]>) -> Option<&Node> {
        let key = key.as_ref();

        let mut current = self.head.as_ref() as *const Node;
        let mut level = self.head.skips.len() - 1;
        loop {
            unsafe {
                if let Some(next) = (*current).skips[level] {
                    let ord = (*next).key.as_slice().cmp(key);
                    match ord {
                        std::cmp::Ordering::Less => {
                            current = next;
                            continue;
                        }
                        std::cmp::Ordering::Equal => return Some(&*next),
                        std::cmp::Ordering::Greater => {
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
                || current == self.head.as_ref()
                || (*current).key.as_slice().cmp(key) == Ordering::Less
            {
                None
            } else {
                Some(&(*current))
            }
        }
    }

    fn insert(&mut self, key: impl Into<Vec<u8>>, val: impl Into<Vec<u8>>) {
        let key = key.into();
        let val = val.into();

        let mut prevs: [Option<*mut Node>; MAX_HEIGHT] = [None; MAX_HEIGHT];
        let new_height = self.random_height();

        let prevs = &mut prevs[0..new_height];
        let mut level = MAX_HEIGHT - 1;
        let mut current = self.head.as_mut() as *mut Node;

        for item in prevs.iter_mut() {
            *item = Some(current);
        }

        loop {
            unsafe {
                if let Some(next) = (*current).skips[level] {
                    let ord = (*next).key.as_slice().cmp(&key);

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
            value: val,
        });

        let new_ptr = new_node.as_mut() as *mut Node;
        for (i, item) in prevs.iter().enumerate().take(new_height) {
            if let Some(prev) = *item {
                unsafe {
                    new_node.skips[i] = (*prev).skips[i];
                    (*prev).skips[i] = Some(new_ptr);
                }
            }
        }

        self.len += 1;
        unsafe {
            new_node.next = replace(&mut (*current).next, None);
            let _ = replace(&mut (*current).next, Some(new_node));
        }
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
