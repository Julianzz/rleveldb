use std::{
    cmp::Ordering,
    io::Write,
    rc::Rc,
    sync::{
        atomic::{self, AtomicUsize},
        Arc,
    },
};

use crate::{
    cmp::{Comparator, InternalKeyComparator, KeyComparator},
    codec::{Decoder, NumberReader, VarLengthSliceWriter, VarintReader},
    error::{Error, Result},
    iterator::DBIterator,
    skiplist::{SkipList, SkipListIter},
    types::{SequenceNumber, ValueType},
    utils::buffer::BufferReader,
};

use integer_encoding::{FixedIntWriter, VarInt, VarIntWriter};

pub struct MemTable {
    table: Arc<SkipList<Vec<u8>>>,
    comparator: Arc<dyn Comparator>,
    memory_usage: AtomicUsize,
}

impl MemTable {
    pub fn new(internal_comparator: InternalKeyComparator) -> MemTable {
        let comparator = internal_comparator.user_comparator();
        let key_comparator = KeyComparator::new(internal_comparator);

        MemTable {
            table: Arc::new(SkipList::new(Rc::new(key_comparator))),
            comparator,
            memory_usage: AtomicUsize::new(0),
        }
    }

    pub fn add<T: AsRef<[u8]>>(&self, seq: SequenceNumber, t: ValueType, key: T, value: T) {
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

        self.memory_usage
            .fetch_add(buf.len(), atomic::Ordering::Relaxed);

        assert_eq!(buf.len(), size);

        self.table.insert(buf);
    }

    pub fn get(&self, search_key: LookupKey) -> Result<Option<Vec<u8>>> {
        let mut iter = SkipListIter::new(self.table.clone());
        iter.seek(search_key.memtable_key());

        if iter.valid() {
            let mut seek_key = iter.key();

            let internal_key_len = seek_key.read_var_u32().unwrap();
            let mut internal_key = seek_key.read_bytes(internal_key_len as usize).unwrap();
            let seek_user_key = internal_key.read_bytes(internal_key.len() - 8).unwrap();
            if self
                .comparator
                .compare(search_key.user_key(), seek_user_key)
                == Ordering::Equal
            {
                let record_type = internal_key.read_u64_le().unwrap() & 0xff;
                if record_type == ValueType::Value as u64 {
                    let value_len = seek_key.read_var_u32().unwrap();
                    let user_value = seek_key.read_bytes(value_len as usize).unwrap();
                    return Ok(Some(user_value.into()));
                } else if record_type == ValueType::Deletetion as u64 {
                    return Ok(None);
                }
            }
        }
        Err(Error::NotFoundError("no key".into()))
    }

    pub fn approximate_memory_usage(&self) -> usize {
        self.memory_usage.load(atomic::Ordering::Relaxed)
    }

    pub fn iter(&self) -> Box<dyn DBIterator> {
        Box::new(MemTableIterator::new(SkipListIter::new(self.table.clone())))
    }
}

pub struct MemTableIterator {
    iter: SkipListIter<Vec<u8>>,
    tmp: Vec<u8>,
}

impl MemTableIterator {
    pub fn new(iter: SkipListIter<Vec<u8>>) -> Self {
        MemTableIterator {
            iter,
            tmp: Vec::new(),
        }
    }
}

impl DBIterator for MemTableIterator {
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first();
    }

    fn seek_to_last(&mut self) {
        self.iter.seek_to_last();
    }

    fn seek(&mut self, target: &[u8]) {
        let tmp = &mut self.tmp;
        tmp.clear();
        tmp.write_length_prefixed_slice(target);

        self.iter.seek(&self.tmp);
    }

    fn next(&mut self) {
        self.iter.next();
    }

    fn prev(&mut self) {
        self.iter.prev();
    }

    fn key(&self) -> &[u8] {
        let raw = self.iter.key();
        let (result, _) = raw.decode_length_prefix_slice().unwrap();
        result
    }

    fn value(&self) -> &[u8] {
        let raw = self.iter.key();
        let (_, offset) = raw.decode_length_prefix_slice().unwrap();
        let (result, _) = raw[offset..].decode_length_prefix_slice().unwrap();
        result
    }

    fn status(&mut self) -> Result<()> {
        todo!()
    }
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

#[cfg(test)]
mod tests {
    use crate::cmp::BitWiseComparator;

    use super::*;

    #[test]
    fn test_memtable() {
        let user_comparator = BitWiseComparator {};
        let comparator = InternalKeyComparator::new(Arc::new(user_comparator));
        let table = MemTable::new(comparator);
        let datas = &[
            ("liuzhenzhong", 1u64, ValueType::Value, "zhong"),
            ("liuzhong", 2u64, ValueType::Value, "time"),
            ("time", 1u64, ValueType::Deletetion, ""),
        ];

        for (key, seq, typ, val) in datas {
            table.add(*seq, *typ, *key, val)
        }

        for &(key, seq, typ, val) in datas {
            let lookup_key = LookupKey::new(key, seq, typ);
            let result = table.get(lookup_key);
            if typ == ValueType::Value {
                assert!(result.is_ok(), "delete key");
                assert_eq!(result.unwrap().unwrap().as_slice(), val.as_bytes());
            } else {
                assert!(result.is_ok(), "delete key");
                assert!(result.unwrap().is_none());
            }
        }
    }
}
