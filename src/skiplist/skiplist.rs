use std::{ptr::NonNull, sync::atomic::AtomicUsize, mem};

use bytes::Bytes;

use super::arena::Arena;

const HEIGHT_INCREASE: u32 = u32::MAX / 3;
pub const MAX_NODE_SIZE: usize = mem::size_of::<Node>();

const MAX_HEIGHT: usize = 20;

struct Node{
    key: Bytes,
    value: Bytes,
    height: usize,
    prev: AtomicUsize,
    tower: [AtomicUsize;MAX_HEIGHT]

}

impl Node {
    fn alloc(arena: Arena, key: Bytes, value: Bytes, height:usize) -> usize {
        let size = mem::size_of::<Node>();
        

        0
    }

}

pub struct SkipListInner {
    height: AtomicUsize,
    head: NonNull<Node>,
    area: Arena,
}   