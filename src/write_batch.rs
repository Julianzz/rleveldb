use std::fmt::Write;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bytes::Buf;
use integer_encoding::VarIntWriter;

use crate::{
    codec::{decode_value, decode_var_u32, BufferReader, NumberDecoder, NumberEncoder},
    error::{Error, Result},
    types::SequenceNumber,
    ValueType,
};

const HEAD_SIZE: usize = 12;
pub struct WriteBatch {
    rep: Vec<u8>,
}

impl WriteBatch {
    pub fn new() -> Self {
        WriteBatch {
            rep: vec![0; HEAD_SIZE],
        }
    }

    pub fn set_sequence(&mut self, seq: SequenceNumber) {
        self.rep
            .as_mut_slice()
            .write_u64::<LittleEndian>(seq)
            .unwrap()
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.set_count(self.count() + 1);
        self.rep.push(ValueType::Value as u8);
        self.rep.write_varint(key.len() as u32).unwrap();
        self.rep.extend_from_slice(key);
        self.rep.write_varint(value.len() as u32).unwrap();
        self.rep.extend_from_slice(value);
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.set_count(self.count() + 1);
        self.rep.push(ValueType::Deletetion as u8);
        self.rep.write_varint(key.len() as u32).unwrap();
        self.rep.extend_from_slice(key);
    }

    pub fn clear(&mut self) {
        self.rep.clear();
        self.rep.resize(HEAD_SIZE, 0);
    }

    pub fn set_count(&mut self, n: u32) {
        let mut buf = self.rep[8..].as_mut();
        buf.write_u32::<LittleEndian>(n).unwrap()
    }

    pub fn count(&self) -> u32 {
        let mut buf = &self.rep[8..];
        buf.read_u32::<LittleEndian>().unwrap()
    }

    pub fn approximate_size(&self) -> usize {
        self.rep.len()
    }

    pub fn append(&mut self, source: &WriteBatch) {
        self.set_count(self.count() + source.count());
        assert!(source.rep.len() >= HEAD_SIZE);
        assert!(self.rep.len() >= HEAD_SIZE);

        self.rep.extend_from_slice(&source.rep[HEAD_SIZE..]);
    }

    pub fn sequence(&self) -> SequenceNumber {
        let mut buf = &self.rep[..8];
        buf.decode_u64_le().unwrap()
    }

    pub fn content(&self) -> &Vec<u8> {
        &self.rep
    }

    pub fn iterate<H: Handler>(&self, mut handler: H) -> Result<()> {
        let mut buf = self.rep.as_slice();
        if buf.len() < HEAD_SIZE {
            return Err(Error::Corruption(
                "malformed write batch( too small)".into(),
            ));
        }
        buf.advance(HEAD_SIZE);
        let mut found = 0;
        let mut offset = 0;
        while !buf.is_empty() {
            let tag = ValueType::try_from(buf[offset])?;
            offset += 1;
            found += 1;
            match tag {
                ValueType::Deletetion => {
                    let (key, key_offset) = decode_value(&buf[offset..])?;
                    handler.delete(key);
                    offset += key_offset;
                }
                ValueType::Value => {
                    // let key_size = buf.decode_u32_le()?;
                    let (key, key_offset) = decode_value(&buf[offset..])?;
                    offset += key_offset;
                    let (value, value_offset) = decode_value(&buf[offset..])?;
                    offset += value_offset;
                    handler.put(key, value);
                }
            }
        }
        if found != self.count() {
            return Err(Error::Corruption("writebatch has wrong count".into()));
        }

        Ok(())
    }
}

pub trait Handler {
    fn put(&mut self, key: &[u8], value: &[u8]);
    fn delete(&mut self, key: &[u8]);
}
