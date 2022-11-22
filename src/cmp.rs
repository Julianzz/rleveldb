use crate::{
    codec::VarIntReader,
    format::{extract_sequence_key, extract_user_key},
};

use std::{
    cmp::{self, Ordering},
    sync::Arc,
};

pub trait Comparator {
    fn compare(&self, left: &[u8], right: &[u8]) -> Ordering;

    fn name(&self) -> &'static str;

    fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: &[u8]);

    fn find_shortest_successor(&self, key: &mut Vec<u8>);
}

#[derive(Clone, Copy)]
pub struct BitWiseComparator {}

impl Comparator for BitWiseComparator {
    fn compare(&self, left: &[u8], right: &[u8]) -> Ordering {
        left.cmp(right)
    }

    fn name(&self) -> &'static str {
        "Leveldb.BitwiseComaparator"
    }

    fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: &[u8]) {
        let min_length = cmp::min(start.len(), limit.len());
        let mut diff_index = 0;
        while diff_index < min_length && limit[diff_index] == start[diff_index] {
            diff_index += 1;
        }
        if diff_index < min_length {
            let diff_byte = start[diff_index];
            if diff_byte < 0xff && diff_byte + 1 < limit[diff_index] {
                start[diff_index] += 1;
                start.truncate(diff_index + 1);
            }
        }
    }

    fn find_shortest_successor(&self, key: &mut Vec<u8>) {
        let mut truncate_len = 0;
        for (_, byte) in key.iter_mut().enumerate() {
            if *byte != 0xff {
                *byte += 1;
                truncate_len += 1;
                break;
            }
        }
        if truncate_len != 0 {
            key.truncate(truncate_len)
        }
    }
}

#[derive(Clone)]
pub struct InternalKeyComparator {
    user_comparator: Arc<dyn Comparator>,
}

impl InternalKeyComparator {
    pub fn new(user_comparator: Arc<dyn Comparator>) -> Self {
        InternalKeyComparator { user_comparator }
    }
    pub fn user_comparator(&self) -> Arc<dyn Comparator> {
        self.user_comparator.clone()
    }
}

impl Comparator for InternalKeyComparator {
    // order by
    // increasing user key
    // decreasing sequence key
    // decreasing type key
    fn compare(&self, left: &[u8], right: &[u8]) -> Ordering {
        // let (left, right) = (left.as_ref(), right.as_ref());
        let left_key = extract_user_key(left);
        let right_key = extract_user_key(right);
        match self.user_comparator.compare(left_key, right_key) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => {
                let left_seq = extract_sequence_key(left);
                let right_seq = extract_sequence_key(right);
                right_seq.cmp(&left_seq)
            }
        }
    }

    fn name(&self) -> &'static str {
        "leveldb.InternalKeyComparator"
    }

    fn find_shortest_separator(&self, _start: &mut Vec<u8>, _limit: &[u8]) {
        todo!()
    }

    fn find_shortest_successor(&self, _key: &mut Vec<u8>) {
        todo!()
    }
}

pub struct KeyComparator {
    comparator: InternalKeyComparator,
}

impl KeyComparator {
    pub fn new(comparator: InternalKeyComparator) -> Self {
        KeyComparator { comparator }
    }
}

impl Comparator for KeyComparator {
    fn compare(&self, left: &[u8], right: &[u8]) -> Ordering {
        let left_key = get_length_prefixed_slice(left);
        let right_key = get_length_prefixed_slice(right);
        self.comparator.compare(left_key, right_key)
    }

    fn name(&self) -> &'static str {
        "leveldb.KeyComparator"
    }

    fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: &[u8]) {
        self.comparator.find_shortest_separator(start, limit)
    }

    fn find_shortest_successor(&self, key: &mut Vec<u8>) {
        self.comparator.find_shortest_successor(key)
    }
}

pub fn get_length_prefixed_slice(mut buf: &[u8]) -> &[u8] {
    let (len,_) = buf.read_var_u32().unwrap();
    // assert!(len as usize == buf.len());
    &buf[..len as usize]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_shortest_separator() {
        let tests: Vec<(&[u8], &[u8], &[u8])> = vec![
            (
                &[1u8, 2u8, 3u8, 4u8, 5u8],
                &[1u8, 2u8, 3u8, 4u8, 6u8],
                &[1u8, 2u8, 3u8, 4u8, 5u8],
            ),
            (
                &[1u8, 2u8, 3u8, 3u8, 5u8],
                &[1u8, 2u8, 3u8, 5u8, 5u8, 1u8],
                &[1u8, 2u8, 3u8, 4u8],
            ),
            (
                &[1u8, 2u8, 3u8, 4u8, 5u8],
                &[1u8, 2u8, 3u8, 4u8, 5u8],
                &[1u8, 2u8, 3u8, 4u8, 5u8],
            ),
            (
                &[1u8, 2u8, 4u8, 4u8, 5u8],
                &[1u8, 2u8, 3u8, 4u8, 5u8],
                &[1u8, 2u8, 4u8, 4u8, 5u8],
            ),
            (
                &[1u8, 2u8, 3u8, 4u8, 5u8, 1u8],
                &[1u8, 2u8, 3u8, 4u8, 5u8],
                &[1u8, 2u8, 3u8, 4u8, 5u8, 1u8],
            ),
            (
                &[1u8, 1u8, 3u8, 4u8, 5u8, 6u8],
                &[1u8, 5u8, 3u8, 4u8, 5u8],
                &[1u8, 2u8],
            ),
            (&[], &[], &[]),
            (&[0u8], &[], &[0]),
            (&[], &[0u8], &[]),
        ];

        let comparator = BitWiseComparator {};
        for (i, &(a, b, expect)) in tests.iter().enumerate() {
            let mut start = Vec::from(a);
            comparator.find_shortest_separator(&mut start, b);
            assert_eq!(&start, expect, "{}", i);
        }
    }

    #[test]
    fn test_bit_wise_comparator_cmp() {
        let _tests: Vec<(&[u8], &[u8], Ordering)> = vec![
            (
                &[1u8, 2u8, 3u8, 4u8, 5u8],
                &[1u8, 2u8, 3u8, 4u8, 6u8],
                Ordering::Less,
            ),
            (
                &[1u8, 2u8, 3u8, 4u8, 5u8],
                &[1u8, 2u8, 3u8, 4u8, 5u8, 1u8],
                Ordering::Less,
            ),
            (
                &[1u8, 2u8, 3u8, 4u8, 5u8],
                &[1u8, 2u8, 3u8, 4u8, 5u8],
                Ordering::Equal,
            ),
            (
                &[1u8, 2u8, 4u8, 4u8, 5u8],
                &[1u8, 2u8, 3u8, 4u8, 5u8],
                Ordering::Greater,
            ),
            (
                &[1u8, 2u8, 3u8, 4u8, 5u8, 1u8],
                &[1u8, 2u8, 3u8, 4u8, 5u8],
                Ordering::Greater,
            ),
            (
                &[1u8, 1u8, 3u8, 4u8, 5u8, 6u8],
                &[1u8, 2u8, 3u8, 4u8, 5u8],
                Ordering::Less,
            ),
            (
                &[1u8, 2u8, 3u8, 4u8, 5u8, 7u8],
                &[1u8, 2u8, 3u8, 4u8, 5u8],
                Ordering::Greater,
            ),
            (&[], &[], Ordering::Equal),
            (&[0u8], &[], Ordering::Greater),
        ];
    }
}
