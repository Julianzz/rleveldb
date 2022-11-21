use std::{io::Read, ops::Deref};

use integer_encoding::VarInt;
use snap::read::FrameDecoder;

use crate::{
    codec::Decoder,
    env::RandomAccessFile,
    error::{Error, Result},
    options::{Compress, ReadOption},
};

pub const FOOTER_LENGTH: usize = 40;
pub const FULL_FOOTER_LENGTH: usize = FOOTER_LENGTH + 8;
pub const MAGIC_FOOTER_ENCODED: [u8; 8] = [0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb];
// 1-byte type + 32-bit crc
pub const BLOCK_TRAILER_SIZE: usize = 5;

#[derive(Clone, Copy, Default)]
pub struct BlockHandle {
    offset: u64,
    size: u64,
}

impl BlockHandle {
    pub fn new(offset: u64, size: u64) -> Self {
        BlockHandle { offset, size }
    }

    pub fn from_raw(data: &[u8]) -> Option<Self> {
        let mut handle = BlockHandle::default();
        handle.decode(data);
        Some(handle)
    }

    pub fn size(&self) -> u64 {
        self.size
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn set_offset(&mut self, offset: u64) {
        self.offset = offset;
    }
    pub fn set_size(&mut self, size: u64) {
        self.size = size;
    }

    pub fn encode(&self, dst: &mut [u8]) -> usize {
        assert!(dst.len() >= self.offset.required_space() + self.size.required_space());

        let offset = self.offset.encode_var(dst);
        self.size.encode_var(&mut dst[offset..]) + offset
    }

    pub fn decode(&mut self, data: &[u8]) -> Option<usize> {
        let (offset, offset_len) = u64::decode_var(data)?;
        let (size, size_len) = u64::decode_var(&data[offset_len..])?;
        self.offset = offset;
        self.size = size;

        Some(offset_len + size_len)
    }

    // pub fn decode_from(data: &[u8]) -> Option<(BlockHandle, usize)> {
    //     let (offset, offset_len) = u64::decode_var(data)?;
    //     let (size, size_len) = u64::decode_var(&data[offset_len..])?;

    //     Some((BlockHandle { offset, size }, offset_len + size_len))
    // }
}

#[derive(Default, Clone, Copy)]
pub struct Footer {
    pub meta_index_handle: BlockHandle,
    pub index_handle: BlockHandle,
}

impl Footer {
    pub fn new(meta: BlockHandle, index: BlockHandle) -> Footer {
        Footer {
            meta_index_handle: meta,
            index_handle: index,
        }
    }

    pub fn encode(&self, to: &mut [u8]) {
        // assert!()
        let s1 = self.meta_index_handle.encode(to);
        let _ = self.index_handle.encode(&mut to[s1..]);
        to[FOOTER_LENGTH..].copy_from_slice(&MAGIC_FOOTER_ENCODED[..]);
    }

    pub fn decode(&mut self, data: &[u8]) -> Result<()> {
        assert!(data.len() >= FULL_FOOTER_LENGTH);

        if data[FOOTER_LENGTH..] != MAGIC_FOOTER_ENCODED {
            return Err(Error::Corruption("not an sstable(bad magic number)".into()));
        }

        let offset = self
            .meta_index_handle
            .decode(data)
            .ok_or_else(|| Error::Corruption("wrong decode metaindex handle".into()))?;
        let _ = self
            .index_handle
            .decode(&data[offset..])
            .ok_or_else(|| Error::Corruption(("wrong decode block index handle").into()))?;

        Ok(())
    }
}

#[derive(Default)]
pub struct BlockContent {
    pub data: Vec<u8>,
    // pub cachable: bool,
    // pub heap_allocated: bool,
}

impl BlockContent {
    pub fn new(data: Vec<u8>) -> Self {
        BlockContent { data }
    }
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn read_block_from_file<R: RandomAccessFile>(
        file: &R,
        handle: &BlockHandle,
        option: &ReadOption,
    ) -> Result<Self> {
        let n = handle.size as usize;
        let mut buf = vec![0; n + BLOCK_TRAILER_SIZE];
        file.read_exact_at(buf.as_mut(), handle.offset)
            .map_err(|_| Error::Corruption("truncated block read".into()))?;

        let data = buf.as_slice();
        if option.verify_checksum {
            let (checksum, _) = data[n + 1..].decode_u32_le()?;
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&data[0..n + 1]);
            if checksum != hasher.finalize() {
                return Err(Error::Corruption("block check sum mismatch".into()));
            }
        }
        let compress_type = Compress::try_from(data[n])?;
        match compress_type {
            Compress::NO => {
                //TODO, how to deal with mmap
                buf.truncate(n);
                Ok(BlockContent {
                    data: buf,
                    // cachable: true,
                })
            }
            Compress::Snappy => {
                let mut uncompressed_data = Vec::new();
                let mut reader = FrameDecoder::new(&data[..n]);
                reader
                    .read_to_end(&mut uncompressed_data)
                    .map_err(|_| Error::Corruption("corrupted compressed block content".into()))?;
                Ok(BlockContent {
                    data: uncompressed_data,
                })
            }
        }
    }
}

impl AsRef<[u8]> for BlockContent {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl Deref for BlockContent {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
