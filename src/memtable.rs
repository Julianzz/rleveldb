use std::{io::Write, sync::Arc};

use crate::{
    cmp::Comparator,
    skipmap::SkipMap,
    types::{SequenceNumber, ValueType},
};

use integer_encoding::{FixedIntWriter, VarInt, VarIntWriter};

pub struct MemTable {
    table: SkipMap,
}

impl MemTable {
    pub fn new(_: Arc<dyn Comparator>) -> MemTable {
        MemTable {
            table: SkipMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }
    pub fn is_empty(&self) -> bool {
        self.table.len() == 0
    }

    pub fn add<T: AsRef<[u8]>>(&mut self, seq: SequenceNumber, t: ValueType, key: T, value: T) {
        // Format of an entry is concatenation of:
        //  key_size     : varint32 of internal_key.size()
        //  key bytes    : char[internal_key.size()]
        //  tag          : uint64((sequence << 8) | type)
        //  value_size   : varint32 of value.size()
        //  value bytes  : char[value.size()]

        let key = key.as_ref();
        let value = value.as_ref();

        let key_size = key.len() + 8;
        let value_size = value.len();
        let size = key_size + value_size + key_size.required_space() + value_size.required_space();

        let mut buf = Vec::with_capacity(size);
        buf.write_varint(key_size).unwrap();
        buf.write_all(key).unwrap();
        buf.write_fixedint(t as u64 | (seq << 8)).unwrap();
        buf.write_varint(value_size).unwrap();
        buf.write_all(value).unwrap();

        assert_eq!(buf.len(), size);

        self.table.insert(buf, Vec::new());
    }

    // pub fn get(&self, search_key: LookupKey) -> (Option<Vec<u8>>, bool) {
    //     let mut iter = self.table.iter();
    //     iter.seek(search_key.memtable_key());

    //     let (mut key, mut value) = (vec![], vec![]);
    //     if iter.current(&mut key, &mut value) {
    //         let (key_len, mut i) = usize::decode_var(key.as_slice()).unwrap();
    //         let key_offset = i;
    //         i += key_len - 8;

    //         let (rkeylen, rkeyoff, tag, _, _) = if key.len() > i {
    //             let tag = usize::decode_fixed(&key[i..i + 8]);
    //             i += 8;
    //             let (val_len, j) = usize::decode_var(&key[i..]).unwrap();
    //             i += j;

    //             let val_offset = i;
    //             (key_len - 8, key_offset, tag, val_len, val_offset)
    //         } else {
    //             (key_len - 8, key_offset, 0, 0, 0)
    //         };
    //         let found_key = &key[rkeyoff..rkeyoff + rkeylen];
    //         if search_key.user_key().cmp(found_key) == Ordering::Equal {
    //             if tag & 0xff == ValueType::Value as usize {
    //                 return (Some(found_key.to_owned()), false);
    //             } else {
    //                 return (None, true);
    //             }
    //         }
    //     }
    //     (None, false)
    // }
}

pub struct LookupKey {
    key: Vec<u8>,
    key_offset: usize,
}

impl LookupKey {
    pub fn new(key: impl AsRef<[u8]>, seq: SequenceNumber, t: ValueType) -> Self {
        let key = key.as_ref();
        let key_size = key.len() + 8;
        let size = key_size + key_size.required_space();

        let mut buf = Vec::with_capacity(size);
        buf.write_varint(key_size).unwrap();
        buf.write_all(key).unwrap();
        buf.write_fixedint(seq << 8 | t as u64).unwrap();

        LookupKey {
            key: buf,
            key_offset: key_size.required_space(),
        }
    }

    pub fn memtable_key(&self) -> &[u8] {
        self.key.as_slice()
    }
    pub fn user_key(&self) -> &[u8] {
        &self.key[self.key_offset..self.key.len() - 8]
    }

    pub fn internal_key(&self) -> &[u8] {
        &self.key[self.key_offset..]
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_memtable() {
//         let mut table = MemTable::new();
//         let datas = &[("liuzhenzhong", 1u64, ValueType::Value)];

//         for (key, seq, typ) in datas {
//             table.add(*seq, *typ, *key, "")
//         }

//         for (key, seq, typ) in datas {
//             let lookup_key = LookupKey::new(*key, *seq, *typ);
//             let (val, found) = table.get(lookup_key);
//             assert!(!found, "delete key");
//             assert_eq!(val.unwrap().as_slice(), key.as_bytes());
//         }
//     }
// }
