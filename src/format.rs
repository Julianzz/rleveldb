use std::fmt::Debug;

use crate::{
    codec::{NumberReader, NumberWriter},
    types::{SequenceNumber, MAX_SEQUENCE_NUMBER},
    ValueType,
};

#[derive(Default, Clone)]
pub struct InternalKey {
    rep: Vec<u8>,
}

impl Debug for InternalKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InternalKey")
            .field("rep", &self.rep)
            .finish()
    }
}

impl InternalKey {
    pub fn empty() -> Self {
        InternalKey { rep: Vec::new() }
    }

    pub fn new(key: &[u8], s: SequenceNumber, t: ValueType) -> Self {
        let mut rep = Vec::with_capacity(key.len() + 8);
        let parsed_key = ParsedInternalKey {
            user_key: key,
            sequence: s,
            val_type: t,
        };
        parsed_key.append(&mut rep);
        InternalKey { rep }
    }

    pub fn clear(&mut self) {
        self.rep.clear();
    }

    pub fn user_key(&self) -> &[u8] {
        assert!(self.rep.len() > 8);
        &self.rep[..(self.rep.len() - 8)]
    }
    pub fn encode(&self) -> &[u8] {
        self.rep.as_slice()
    }
    pub fn decode(&mut self, s: &[u8]) -> bool {
        self.rep = Vec::from(s);
        !self.rep.is_empty()
    }
}

pub fn pack_sequence_and_type(seq: u64, t: ValueType) -> u64 {
    assert!(seq <= MAX_SEQUENCE_NUMBER);
    (seq << 8) | t as u64
}
pub struct ParsedInternalKey<'a> {
    pub user_key: &'a [u8],
    pub sequence: SequenceNumber,
    pub val_type: ValueType,
}

impl<'a> ParsedInternalKey<'a> {
    pub fn append(&self, result: &mut Vec<u8>) {
        result.extend_from_slice(self.user_key);
        let mut buf = [0u8; 8];
        buf.as_mut()
            .write_u64_le(pack_sequence_and_type(self.sequence, self.val_type))
            .unwrap();
        result.extend_from_slice(&buf);
    }

    pub fn parse(data: &'a [u8]) -> Self {
        assert!(data.len() >= 8);
        let user_key = &data[0..data.len() - 8];
        let mut buf = data;
        let tag = buf.read_u64_le().unwrap();
        let sequence = tag >> 8;
        let val_type = ValueType::try_from((tag & 0xff) as u8).unwrap();

        ParsedInternalKey {
            user_key,
            sequence,
            val_type,
        }
    }
}

pub fn extract_user_key(internal_key: &[u8]) -> &[u8] {
    let internal_key = internal_key;
    assert!(internal_key.len() >= 8);
    &internal_key[..internal_key.len() - 8]
}

pub fn extract_sequence_key<T: AsRef<[u8]>>(internal_key: T) -> u64 {
    let internal_key = internal_key.as_ref();
    assert!(internal_key.len() >= 8);
    let mut buf = &internal_key[internal_key.len() - 8..];
    buf.read_u64_le().unwrap()
}
