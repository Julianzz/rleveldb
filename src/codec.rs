use std::io::{self, Read, Write};

use crate::error::{Error, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

// macro_rules! impl_encode_fn {
//     ($type:ty) => {
//         fn  concat_idents!(encode_,$type,_le)(&mut self, v: $type) -> Result<()>{
//             Ok(())
//         }
//     };
// }

pub trait NumberEncoder: Write {
    fn encode_i64_le(&mut self, v: i64) -> Result<()> {
        self.write_i64::<LittleEndian>(v).map_err(From::from)
    }
    fn encode_u64_le(&mut self, v: u64) -> Result<()> {
        self.write_u64::<LittleEndian>(v).map_err(From::from)
    }
    fn encode_u32_le(&mut self, v: u32) -> Result<()> {
        self.write_u32::<LittleEndian>(v).map_err(From::from)
    }

    fn encode_i32_le(&mut self, v: i32) -> Result<()> {
        self.write_i32::<LittleEndian>(v).map_err(From::from)
    }

    fn encode_u16_le(&mut self, v: u16) -> Result<()> {
        self.write_u16::<LittleEndian>(v).map_err(From::from)
    }

    fn encode_i16_le(&mut self, v: i16) -> Result<()> {
        self.write_i16::<LittleEndian>(v).map_err(From::from)
    }

    fn encode_u8(&mut self, v: u8) -> Result<()> {
        self.write_u8(v).map_err(From::from)
    }
    fn encode_i8(&mut self, v: i8) -> Result<()> {
        self.write_i8(v).map_err(From::from)
    }
}

impl<T> NumberEncoder for T where T: Write {}

pub trait NumberDecoder: Read {
    fn decode_u64_le(&mut self) -> Result<u64> {
        self.read_u64::<LittleEndian>().map_err(From::from)
    }
    fn decode_i64_le(&mut self) -> Result<i64> {
        self.read_i64::<LittleEndian>().map_err(From::from)
    }
    fn decode_i32_le(&mut self) -> Result<i32> {
        self.read_i32::<LittleEndian>().map_err(From::from)
    }
    fn decode_u32_le(&mut self) -> Result<u32> {
        self.read_u32::<LittleEndian>().map_err(From::from)
    }
    fn decode_u16_le(&mut self) -> Result<u16> {
        self.read_u16::<LittleEndian>().map_err(From::from)
    }
    fn decode_i16_le(&mut self) -> Result<i16> {
        self.read_i16::<LittleEndian>().map_err(From::from)
    }
    fn decode_u8(&mut self) -> Result<u8> {
        self.read_u8().map_err(From::from)
    }
    fn decode_i8(&mut self) -> Result<i8> {
        self.read_i8().map_err(From::from)
    }
}

impl<T> NumberDecoder for T where T: Read {}

pub trait VarDecoder {
    fn decode_var_u32(&mut self) -> Result<u32>;
    fn decode_var_u64(&mut self) -> Result<u64>;
}

const MASK: u32 = 128;

impl VarDecoder for &[u8] {
    fn decode_var_u32(&mut self) -> Result<u32> {
        let mut shift = 0;
        let mut result = 0;
        while shift <= 28 {
            let byte = self.read_u8()?;
            if u32::from(byte) & MASK == 0 {
                result |= (u32::from(byte)) << shift;
                return Ok(result);
            } else {
                result |= ((u32::from(byte)) & 127) << shift;
            }
            shift += 7;
        }

        Err(Error::Corruption(
            "Error when decoding varint32".to_string(),
        ))
    }

    fn decode_var_u64(&mut self) -> Result<u64> {
        let mut shift = 0;
        let mut result = 0;
        while shift <= 63 {
            let byte = self.read_u8()?;
            if u64::from(byte) & u64::from(MASK) == 0 {
                result |= (u64::from(byte)) << shift;
                return Ok(result);
            } else {
                result |= ((u64::from(byte)) & 127) << shift;
            }
            shift += 7;
        }

        Err(Error::Corruption(
            "Error when decoding varint64".to_string(),
        ))
    }
}

pub fn decode_u32_le(mut data: &[u8]) -> u32 {
    data.read_u32::<LittleEndian>().unwrap()
}

pub fn decode_var_u32(data: &[u8]) -> Result<(u32, usize)> {
    let mut shift = 0;
    let mut result = 0;
    let mut offset = 0;
    while shift <= 28 {
        if offset >= data.len() {
            return Err(Error::Corruption("malformed var code".into()));
        }
        let byte = data[offset];
        offset += 1;
        if u32::from(byte) & MASK == 0 {
            result |= (u32::from(byte)) << shift;
            return Ok((result, offset));
        } else {
            result |= ((u32::from(byte)) & 127) << shift;
        }
        shift += 7;
    }

    Err(Error::Corruption(
        "Error when decoding varint32".to_string(),
    ))
}

pub fn decode_var_u64(data: &[u8]) -> Result<(u64, usize)> {
    let mut shift = 0;
    let mut result = 0;
    let mut offset = 0;
    while shift <= 63 {
        if offset >= data.len() {
            return Err(Error::Corruption("malformed var code".into()));
        }
        let byte = data[offset];
        offset += 1;
        if u64::from(byte) & u64::from(MASK) == 0 {
            result |= (u64::from(byte)) << shift;
            return Ok((result, offset));
        } else {
            result |= ((u64::from(byte)) & 127) << shift;
        }
        shift += 7;
    }

    Err(Error::Corruption(
        "Error when decoding varint64".to_string(),
    ))
}

pub fn decode_value(buf: &[u8]) -> Result<(&[u8], usize)> {
    let mut offset = 0;
    let (key_size, key_offset) = decode_var_u32(&buf[offset..])?;
    offset += key_offset;
    if offset + key_size as usize >= buf.len() {
        return Err(Error::Corruption("bad var key lenngth".into()));
    }
    let key = &buf[offset..offset + key_size as usize];
    offset += key_size as usize;
    Ok((key, offset))
}

pub trait BufferReader {
    fn read_bytes(&self, count: usize) -> Result<(&[u8], &[u8])>;
}

impl BufferReader for &[u8] {
    fn read_bytes(&self, count: usize) -> Result<(&[u8], &[u8])> {
        if self.len() < count {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected eof").into());
        }
        let (left, right) = self.split_at(count);
        Ok((left, right))
    }
}
