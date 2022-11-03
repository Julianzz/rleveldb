use byteorder::WriteBytesExt;

use crate::{
    codec::{NumberDecoder, NumberEncoder},
    types::{SequenceNumber, MAX_SEQUENCE_NUMBER},
    ValueType,
};

pub struct InternalKey {
    rep: Vec<u8>,
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
            .encode_u64_le(pack_sequence_and_type(self.sequence, self.val_type));
        result.extend_from_slice(&buf);
    }

    pub fn parse(data: &'a [u8]) -> Self {
        assert!(data.len() >= 8);
        let key = &data[0..data.len() - 8];
        let mut buf = data;
        let tag = buf.decode_u64_le().unwrap();
        let seq = tag >> 8;
        let val_type = ValueType::try_from((tag & 0xff) as u8).unwrap();

        ParsedInternalKey {
            user_key: key,
            sequence: seq,
            val_type: val_type,
        }
    }
}
