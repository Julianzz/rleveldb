use std::cmp::{self, Ordering};

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
        for (i, byte) in key.iter_mut().enumerate() {
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
        let tests: Vec<(&[u8], &[u8], Ordering)> = vec![
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
