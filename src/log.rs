use crc::{Crc, CRC_32_ISCSI};
use std::io::{Cursor, ErrorKind};

use crate::codec::{NumberDecoder, NumberEncoder};
use crate::env::{SequencialFile, WritableFile};
use crate::error::{Error, Result};

const BLOCK_SIZE: usize = 32 * 1024;
const HEADER_SIZE: usize = 4 + 2 + 1;

#[derive(Clone, Copy)]
pub enum RecordType {
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

impl From<u8> for RecordType {
    fn from(b: u8) -> Self {
        match b {
            1 => RecordType::Full,
            2 => RecordType::First,
            3 => RecordType::Middle,
            4 => RecordType::Last,
            _ => panic!("unrecognized record type"),
        }
    }
}

pub struct LogWriter<W: WritableFile> {
    writer: W,
    current_block_offset: usize,
    block_size: usize,
    digest: Crc<u32>,
}

impl<W: WritableFile> LogWriter<W> {
    pub fn new(writer: W) -> LogWriter<W> {
        LogWriter {
            writer,
            current_block_offset: 0,
            block_size: BLOCK_SIZE,
            digest: Crc::<u32>::new(&CRC_32_ISCSI),
        }
    }

    pub fn add_record<P: AsRef<[u8]>>(&mut self, record: P) -> Result<()> {
        let mut record = record.as_ref();
        let mut first_frag = true;
        while !record.is_empty() {
            assert!(self.block_size > HEADER_SIZE);

            let left = self.block_size - self.current_block_offset;
            if left < HEADER_SIZE {
                self.writer.append(&vec![0; left])?;
                self.current_block_offset = 0;
            }

            let avail_size = self.block_size - self.current_block_offset - HEADER_SIZE;
            let data_frag_size = if record.len() < avail_size {
                record.len()
            } else {
                avail_size
            };

            let record_type = if first_frag && data_frag_size == record.len() {
                RecordType::Full
            } else if first_frag {
                RecordType::First
            } else if data_frag_size == record.len() {
                RecordType::Last
            } else {
                RecordType::Middle
            };

            // write record
            self.emit_record(record_type, &record[..data_frag_size], data_frag_size)?;

            record = &record[data_frag_size..];
            first_frag = false;
        }
        Ok(())
    }

    fn emit_record(&mut self, t: RecordType, data: &[u8], len: usize) -> Result<()> {
        let mut digest = self.digest.digest();
        digest.update(&[t as u8]);
        digest.update(data);
        let chksum = digest.finalize();
        // let mut header: [u8; HEADER_SIZE] = [0; HEADER_SIZE];

        let mut buf = Cursor::new([0; HEADER_SIZE]);
        buf.encode_u32_le(chksum)?;
        buf.encode_u16_le(len as u16)?;
        buf.encode_u8(t as u8)?;

        self.writer.append(buf.get_ref())?;
        self.writer.append(data)?;

        self.writer.flush()?;

        self.current_block_offset = HEADER_SIZE + len;

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
    pub fn sync(&mut self) -> Result<()> {
        self.writer.sync()?;
        Ok(())
    }
}

pub struct LogReader<R: SequencialFile> {
    file: R,
    crc: Crc<u32>,
    blk_off: usize,
    block_size: usize,
    head_scratch: [u8; HEADER_SIZE],
    checksum: bool,

    eof: bool,
    buf: Vec<u8>,
    buf_length: usize,
}

impl<R: SequencialFile> LogReader<R> {
    pub fn new(file: R, checksum: bool) -> Self {
        LogReader {
            file,
            crc: Crc::<u32>::new(&CRC_32_ISCSI),
            blk_off: 0,
            block_size: BLOCK_SIZE,
            head_scratch: Default::default(),
            checksum,

            eof: false,
            buf: vec![0; BLOCK_SIZE],
            buf_length: 0,
        }
    }

    pub fn clear_buf(&mut self) {
        self.buf = vec![0; BLOCK_SIZE];
        self.buf_length = 0;
    }

    pub fn read_physical_record(&mut self, dst: &mut Vec<u8>) -> Result<Option<usize>> {
        dst.clear();
        let mut dst_offset: usize = 0;
        loop {
            if self.block_size - self.blk_off < HEADER_SIZE {
                self.file
                    .read_exact(&mut self.head_scratch[0..self.block_size - self.blk_off])?;
                self.blk_off = 0;
            }
            let res = self.file.read_exact(&mut self.head_scratch);
            if let Err(e) = res.as_ref() {
                if let Error::IOError { source } = e {
                    if source.kind() == ErrorKind::UnexpectedEof {
                        return Ok(None);
                    }
                }
            }
            res?;

            self.blk_off += HEADER_SIZE;

            let mut buf = Cursor::new(self.head_scratch);

            // let mut data = [..];
            let checksum = buf.decode_u32_le()?;
            let length = buf.decode_i16_le()?;
            let record_type = buf.decode_u8()?;

            // let checksum = data.read_u32::<LittleEndian>()?;
            // let length = data.read_u16::<LittleEndian>()?;
            // let r#type = data.read_u8()?;

            dst.resize(dst_offset + length as usize, 0);

            self.file
                .read_exact(&mut dst[dst_offset..dst_offset + length as usize])?;
            self.blk_off += length as usize;

            self.blk_off %= self.block_size;

            if self.checksum {
                let mut digest = self.crc.digest();
                digest.update(&[record_type]);
                digest.update(&dst[dst_offset..dst_offset + length as usize]);
                if digest.finalize() != checksum {
                    return Err(Error::Corruption("digest check failed".into()));
                }
            }

            dst_offset += length as usize;
            match RecordType::from(record_type) {
                RecordType::Full => {
                    return Ok(Some(dst_offset));
                }
                RecordType::First | RecordType::Middle => {
                    continue;
                }
                RecordType::Last => {
                    return Ok(Some(dst_offset));
                }
            }
            // self.src.read(buf);
        }
    }

    pub fn read_record(&mut self, dst: &mut Vec<u8>) -> Result<Option<usize>> {
        self.read_physical_record(dst)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::{self, TempDir};

    use crate::{
        env::{posix::PosixEnv, Env},
        LogReader,
    };

    use super::LogWriter;
    use std::str;

    fn create_tmp_file() -> TempDir {
        tempfile::Builder::new()
            .prefix("test_debug_file")
            .tempdir()
            .unwrap()
    }
    #[test]
    fn test_writer() {
        let datas = &["liu", "jump over a house", "time is good"];

        let dir = create_tmp_file();
        let file_path = dir.path().join("test.log");

        let env = PosixEnv {};
        eprintln!("file path{:?}", file_path);
        let file = env.new_writable_file(&file_path).unwrap();

        let mut writer = LogWriter::new(file);
        for d in datas {
            let result = writer.add_record(*d);
            assert!(matches!(result, Ok(_)));
        }
        let result = writer.flush();
        assert!(matches!(result, Ok(_)));
    }

    #[test]
    fn integrate_read_write_test() {
        let datas = &[
            "liu",
            "zhenzhong",
            "guojia",
            str::from_utf8(&[b'a'; 32 * 1024 * 2 + 20]).unwrap(),
        ];

        let dir = create_tmp_file();
        let file_path = dir.path().join("test.log");

        let env = PosixEnv {};
        {
            let file = env.new_writable_file(&file_path).unwrap();

            let mut writer = LogWriter::new(file);
            for data in datas {
                writer.add_record(*data).unwrap();
            }
            writer.flush().unwrap();
        }

        let file = env.new_sequential_file(&file_path).unwrap();
        let mut reader = LogReader::new(file, true);
        for i in 0..datas.len() {
            let mut dst = Vec::new();
            reader.read_record(&mut dst).unwrap();
            // println!("===={}", String::from_utf8(dst).unwrap());
            println!("read: {}", i);
            assert_eq!(String::from_utf8(dst).unwrap(), datas[i]);
        }
    }
}
