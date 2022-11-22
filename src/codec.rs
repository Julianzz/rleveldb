use crate::error::{Error, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::{
    intrinsics::unlikely,
    io::{Read, Write},
};

const MASK: u32 = 0b1000_0000;

pub trait NumberWriter: Write {
    fn write_u64_le(&mut self, v: u64) -> Result<()> {
        self.write_u64::<LittleEndian>(v).map_err(From::from)
    }

    fn write_u32_le(&mut self, v: u32) -> Result<()> {
        self.write_u32::<LittleEndian>(v).map_err(From::from)
    }
    fn write_u16_le(&mut self, v: u16) -> Result<()> {
        self.write_u16::<LittleEndian>(v).map_err(From::from)
    }
    fn write_u8_le(&mut self, v: u8) -> Result<()> {
        self.write_u8(v).map_err(From::from)
    }
}

impl<T> NumberWriter for T where T: Write {}

pub trait NumberReader: Read {
    fn read_u64_le(&mut self) -> Result<u64> {
        self.read_u64::<LittleEndian>().map_err(From::from)
    }
    fn read_u32_le(&mut self) -> Result<u32> {
        self.read_u32::<LittleEndian>().map_err(From::from)
    }
    fn read_u16_le(&mut self) -> Result<u16> {
        self.read_u16::<LittleEndian>().map_err(From::from)
    }
    fn read_u8_le(&mut self) -> Result<u8> {
        self.read_u8().map_err(From::from)
    }
}

impl NumberReader for &[u8] {}

pub trait VarIntWriter: NumberWriter {
    fn write_var_u32(&mut self, v: u32) -> Result<usize> {
        if v < (1 << 7) {
            self.write_u8(v as u8)?;
            Ok(1)
        } else if v < (1 << 14) {
            self.write_u8((v | MASK) as u8)?;
            self.write_u8((v >> 7) as u8)?;
            Ok(2)
        } else if v < (1 << 21) {
            self.write_u8((v | MASK) as u8)?;
            self.write_u8(((v >> 7) | MASK) as u8)?;
            self.write_u8((v >> 14) as u8)?;
            Ok(3)
        } else if v < (1 << 28) {
            self.write_u8((v | MASK) as u8)?;
            self.write_u8(((v >> 7) | MASK) as u8)?;
            self.write_u8(((v >> 14) | MASK) as u8)?;
            self.write_u8((v >> 21) as u8)?;
            Ok(4)
        } else {
            self.write_u8((v | MASK) as u8)?;
            self.write_u8(((v >> 7) | MASK) as u8)?;
            self.write_u8(((v >> 14) | MASK) as u8)?;
            self.write_u8(((v >> 21) | MASK) as u8)?;
            self.write_u8((v >> 28) as u8)?;
            Ok(5)
        }
    }
    fn write_var_u64(&mut self, mut v: u64) -> Result<usize> {
        let mut len = 0;
        while v >= u64::from(MASK) {
            let n = (v | u64::from(MASK)) & 0xFF;
            self.write_u8(n as u8).unwrap();
            v >>= 7;
            len += 1;
        }
        self.write_u8(v as u8)?;
        Ok(len + 1)
    }
}

impl VarIntWriter for &mut [u8] {}
impl VarIntWriter for Vec<u8> {}

pub trait VarIntReader: NumberReader {
    fn read_var_u32(&mut self) -> Result<(u32, usize)> {
        let mut shift = 0;
        let mut result = 0;
        let mut len = 0;
        while shift <= 28 {
            let byte = self.read_u8()?;
            len += 1;
            if u32::from(byte) & MASK == 0 {
                result |= (u32::from(byte)) << shift;
                return Ok((result, len));
            } else {
                result |= ((u32::from(byte)) & 127) << shift;
            }
            shift += 7;
        }

        Err(Error::Corruption(
            "Error when decoding varint32".to_string(),
        ))
    }

    fn read_var_u64(&mut self) -> Result<(u64, usize)> {
        let mut shift = 0;
        let mut result = 0;
        let mut len = 0;
        while shift <= 63 {
            let byte = self.read_u8()?;
            len += 1;
            if u64::from(byte) & u64::from(MASK) == 0 {
                result |= (u64::from(byte)) << shift;
                return Ok((result, len));
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

impl VarIntReader for &[u8] {}

pub fn required_space(data: u64) -> usize {
    let mut data = data;
    if data == 0 {
        return 1;
    }

    let mut logcounter = 0;
    while data > 0 {
        logcounter += 1;
        data >>= 7;
    }
    logcounter
}

pub fn write_length_prefixed_slice(target: &mut Vec<u8>, data: &[u8]) -> Result<()> {
    target.reserve(data.len() + required_space(data.len() as u64));
    target.write_var_u32(data.len() as u32)?;
    target.extend_from_slice(data);
    Ok(())
}

pub fn read_length_prefixed_slice<'a>(data: &mut &'a [u8]) -> Result<&'a [u8]> {
    let (len, _) = data.read_var_u32()?;
    // assert!(data.len() >= len as usize);
    if unlikely(data.len() < len as usize) {
        return Err(Error::Corruption(
            "error slice length when decoding length prefixed slice".to_string(),
        ));
    }
    let (left, right) = data.split_at(len as usize);
    *data = right;
    Ok(left)
}
