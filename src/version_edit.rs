use std::io::Write;

use crate::{
    codec::{put_varint32, put_varint64, DecodeVarint, VarLengthSliceReader},
    consts::NUM_LEVELS,
    error::{Error, Result},
    format::InternalKey,
    types::SequenceNumber,
    version::FileMetaData,
};

const COMPARATOR: u32 = 1;
const LOG_NUMBER: u32 = 2;
const NEXT_FILE_NUMBER: u32 = 3;
const LAST_SEQUENCE: u32 = 4;
const COMPACTION_POINTER: u32 = 5;
const DELETED_FILES: u32 = 6;
const NEW_FILE: u32 = 7;
// 8 was used for large value refs
const PREV_LOG_NUMBER: u32 = 9;

#[derive(Default)]
pub struct VersionEdit {
    pub comparator: Option<String>,
    pub log_number: Option<u64>,
    pub prev_log_number: Option<u64>,
    pub next_file_number: Option<u64>,
    pub last_sequence: Option<SequenceNumber>,

    pub compact_pointers: Vec<(u32, InternalKey)>,
    pub deleted_files: Vec<(u32, u64)>,
    pub new_files: Vec<(u32, FileMetaData)>,
}

impl VersionEdit {
    pub fn new() -> VersionEdit {
        VersionEdit {
            comparator: None,
            log_number: None,
            prev_log_number: None,
            next_file_number: None,
            last_sequence: None,
            compact_pointers: Vec::new(),
            deleted_files: Vec::new(),
            new_files: Vec::new(),
        }
    }

    pub fn set_comparator(&mut self, name: impl Into<String>) {
        self.comparator = Some(name.into());
    }
    pub fn set_log_number(&mut self, num: u64) {
        self.log_number = Some(num);
    }
    pub fn set_prev_log_number(&mut self, num: u64) {
        self.prev_log_number = Some(num);
    }
    pub fn set_next_file_number(&mut self, num: u64) {
        self.next_file_number = Some(num);
    }
    pub fn set_last_sequence(&mut self, num: u64) {
        self.last_sequence = Some(num);
    }
    pub fn add_compact_pointer(&mut self, level: u32, key: InternalKey) {
        self.compact_pointers.push((level, key));
    }
    pub fn add_new_file(
        &mut self,
        level: u32,
        file_num: u64,
        file_size: u64,
        smallest: InternalKey,
        largest: InternalKey,
    ) {
        let mut file_meta = FileMetaData {
            allowed_seeks: 0,
            number: file_num,
            file_size,
            smallest,
            largest,
        };
        self.new_files.push((level, file_meta));
    }

    pub fn add_delete_file(&mut self, level: u32, file_num: u64) {
        self.deleted_files.push((level, file_num));
    }

    pub fn encode(&self, dst: &mut Vec<u8>) {
        self.comparator.as_ref().map(|c| {
            put_varint32(dst, COMPARATOR);
            put_varint32(dst, c.as_bytes().len() as u32);
            dst.write_all(c.as_bytes()).unwrap();
        });

        self.log_number.as_ref().map(|c| {
            put_varint32(dst, LOG_NUMBER);
            put_varint64(dst, *c);
        });
        self.prev_log_number.as_ref().map(|c| {
            put_varint32(dst, PREV_LOG_NUMBER);
            put_varint64(dst, *c);
        });
        self.next_file_number.as_ref().map(|c| {
            put_varint32(dst, NEXT_FILE_NUMBER);
            put_varint64(dst, *c);
        });
        self.last_sequence.as_ref().map(|c| {
            put_varint32(dst, LAST_SEQUENCE);
            put_varint64(dst, *c);
        });
        for (n, k) in self.compact_pointers.iter() {
            put_varint32(dst, COMPACTION_POINTER);
            put_varint32(dst, *n);
            let key = k.encode();
            put_varint32(dst, key.len() as u32);
            dst.write_all(key);
        }
        for (n, m) in self.deleted_files.iter() {
            put_varint32(dst, DELETED_FILES);
            put_varint32(dst, *n);
            put_varint64(dst, *m);
        }

        for (n, f) in self.new_files.iter() {
            put_varint32(dst, NEW_FILE);
            put_varint32(dst, *n);
            put_varint64(dst, f.number);
            put_varint64(dst, f.file_size);
            let (small, large) = (f.smallest.encode(), f.largest.encode());
            put_varint32(dst, small.len() as u32);
            dst.write_all(small);
            put_varint32(dst, large.len() as u32);
            dst.write_all(large);
        }
    }

    pub fn decode(&mut self, mut src: &[u8]) -> Result<()> {
        let mut level = 0;
        let mut msg = None;
        let mut file_meta = FileMetaData::default();
        let mut key = InternalKey::default();

        while msg.is_none() && !src.is_empty() {
            if let Ok(tag) = src.decode_varint32() {
                match tag {
                    COMPARATOR => {
                        if let Ok(s) = src.get_length_prefixed_slice() {
                            self.comparator = Some(String::from_utf8_lossy(s).to_string());
                        } else {
                            msg = Some(String::from("compation pointer"));
                        }
                    }

                    LOG_NUMBER => {
                        if let Ok(n) = src.decode_varint64() {
                            self.log_number = Some(n);
                        } else {
                            msg = Some(String::from("log number"));
                        }
                    }

                    PREV_LOG_NUMBER => {
                        if let Ok(n) = src.decode_varint64() {
                            self.prev_log_number = Some(n);
                        } else {
                            msg = Some(String::from("prev log number"));
                        }
                    }

                    NEXT_FILE_NUMBER => {
                        if let Ok(n) = src.decode_varint64() {
                            self.next_file_number = Some(n);
                        } else {
                            msg = Some(String::from("next file number"));
                        }
                    }

                    LAST_SEQUENCE => {
                        if let Ok(n) = src.decode_varint64() {
                            self.last_sequence = Some(n);
                        } else {
                            msg = Some(String::from("last sequence"));
                        }
                    }

                    COMPACTION_POINTER => {
                        if get_level(&mut src, &mut level).is_ok()
                            && get_internal_key(&mut src, &mut key).is_ok()
                        {
                            self.compact_pointers.push((level, key.clone()))
                        } else {
                            msg = Some(String::from("compaction pointer"));
                        }
                    }

                    DELETED_FILES => {
                        let (level_ret, num_res) =
                            (get_level(&mut src, &mut level), src.decode_varint64());
                        if level_ret.is_ok() && num_res.is_ok() {
                            self.deleted_files.push((level, num_res.unwrap()));
                        } else {
                            msg = Some(String::from("deleted files"));
                        }
                    }

                    NEW_FILE => {
                        let level_res = get_level(&mut src, &mut level);
                        let num_res = src.decode_varint64();
                        let size_res = src.decode_varint64();
                        let small_res = get_internal_key(&mut src, &mut file_meta.smallest);
                        let large_res = get_internal_key(&mut src, &mut file_meta.largest);
                        if level_res.is_ok()
                            && num_res.is_ok()
                            && size_res.is_ok()
                            && small_res.is_ok()
                            && large_res.is_ok()
                        {
                            file_meta.number = num_res.unwrap();
                            file_meta.file_size = size_res.unwrap();
                            self.new_files.push((level, file_meta.clone()))
                        } else {
                            msg = Some(String::from("new files"));
                        }
                    }

                    _ => {
                        msg = Some(String::from("unknown tag"));
                    }
                }
            } else {
                break;
            }
        }

        if msg.is_none() && !src.is_empty() {
            msg = Some("invalid tag".to_string());
        }

        if let Some(s) = msg {
            Err(Error::Corruption(format!("VersionEdit {}", s)))
        } else {
            Ok(())
        }
    }
}

fn get_level(src: &mut &[u8], level: &mut u32) -> Result<()> {
    let l = (*src).decode_varint32()?;
    if l < NUM_LEVELS as u32 {
        *level = l;
        Ok(())
    } else {
        Err(Error::Corruption(
            "level larger than configed max".to_string(),
        ))
    }
}

fn get_internal_key(src: &mut &[u8], dst: &mut InternalKey) -> Result<()> {
    let data = (*src).get_length_prefixed_slice()?;
    if !dst.decode(data.into()) {
        Err(Error::Corruption("internal key decode failed".to_string()))
    } else {
        Ok(())
    }
}
