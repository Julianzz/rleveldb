use std::{
    cell::UnsafeCell,
    collections::{HashSet, LinkedList},
    fmt::Debug,
    path::Path,
    sync::Arc,
};

use crate::{
    cmp::{Comparator, InternalKeyComparator},
    codec::{NumberReader, NumberWriter},
    consts::{L0_COMPACTION_TRIGGER, NUM_LEVELS},
    env::{read_file_to_vec, Env},
    error::{Error, Result},
    filenames::{current_file_name, descriptor_file_name, set_current_file},
    format::InternalKey,
    iterator::DBIterator,
    options::Options,
    sstable::{
        block::BlockIter,
        Table, TableBlockIterBuilder,
        two_level_iterator::{BlockIterBuilder, TwoLevelIterator},
    },
    table_cache::TableCache,
    types::SequenceNumber,
    version::{max_bytes_for_level, FileMetaData, Version, VersionBuilder},
    version_edit::VersionEdit,
    LogReader, LogWriter,
};

pub struct VersionSet<E: Env> {
    env: E,
    db_name: String,
    table_cache: TableCache<E>,
    options: Arc<Options>,
    icmp: InternalKeyComparator,
    last_sequence: SequenceNumber,
    next_file_number: u64,
    manifest_file_number: u64,
    log_number: u64,
    prev_log_number: u64,

    versions: LinkedList<Arc<Version<E>>>,

    compact_pointer: [Vec<u8>; NUM_LEVELS],
    descriptor_log: Option<LogWriter<E::WritableFile>>,

    pub pending_outputs: HashSet<u64>,
    // pub stats: Vec<CompactionState>,
}

impl<E: Env> VersionSet<E> {
    pub fn new(
        env: E,
        db_name: String,
        options: Arc<Options>,
        table_cache: TableCache<E>,
        icmp: InternalKeyComparator,
    ) -> Self {
        let v = Version::new(icmp.clone(), options.clone(), table_cache.clone());
        let mut versions = LinkedList::new();
        versions.push_front(Arc::new(v));

        VersionSet {
            env,
            db_name,
            table_cache,
            options,
            icmp,
            last_sequence: 0,
            next_file_number: 2,
            manifest_file_number: 0,
            log_number: 0,
            prev_log_number: 0,
            versions,
            compact_pointer: Default::default(),
            descriptor_log: None,
            pending_outputs: HashSet::new(),
        }
    }

    pub fn current(&self) -> Option<Arc<Version<E>>> {
        self.versions.front().cloned()
    }
    pub fn last_sequence(&self) -> SequenceNumber {
        self.last_sequence
    }
    pub fn set_last_sequence(&mut self, n: SequenceNumber) {
        self.last_sequence = n;
    }

    pub fn manifest_file_number(&self) -> u64 {
        self.manifest_file_number
    }

    pub fn new_file_number(&mut self) -> u64 {
        let ret = self.next_file_number;
        self.next_file_number += 1;
        ret
    }
    pub fn log_number(&self) -> u64 {
        self.log_number
    }
    pub fn prev_log_number(&self) -> u64 {
        self.prev_log_number
    }

    pub fn live_files(&self, live: &mut HashSet<u64>) {
        for v in self.versions.iter() {
            for level in v.files.iter() {
                for f in level.iter() {
                    live.insert(f.number);
                }
            }
        }
    }

    pub fn mark_file_number_used(&mut self, file_number: u64) {
        if self.next_file_number <= file_number {
            self.next_file_number = file_number + 1;
        }
    }

    pub fn finalize(&self, version: &mut Version<E>) {
        let (best_level, best_score) = (0..NUM_LEVELS - 1)
            .map(|level| {
                let score = if level == 0 {
                    (version.files.len() / L0_COMPACTION_TRIGGER) as f64
                } else {
                    version.level_total_file_size(level) as f64 / max_bytes_for_level(level)
                };
                (level, score)
            })
            .fold(
                (-1i32, -1f64),
                |(best_level, best_score), (level, score)| {
                    if score > best_score {
                        (level as i32, score)
                    } else {
                        (best_level, best_score)
                    }
                },
            );
        version.compaction_level = best_level;
        version.compaction_score = best_score;
    }

    pub fn recover(&mut self) -> Result<bool> {
        let mut current = String::with_capacity(1024);
        let current_file = current_file_name(&self.db_name);
        read_file_to_vec(self.env.clone(), &current_file, &mut current)?;
        if current.is_empty() || !current.ends_with('\n') {
            return Err(Error::Corruption(
                "CURRENT file does not end with new line".into(),
            ));
        }
        current.truncate(current.len() - 1);
        let description_name = Path::new(&self.db_name).join(current);
        let file = self.env.new_sequential_file(&description_name)?;
        let mut reader = LogReader::new(file, true);
        let mut record = Vec::new();
        let mut builder = VersionBuilder::new(self.current().unwrap(), self.icmp.clone());

        let mut log_number = None;
        let mut prev_log_number = None;
        let mut next_file_number = None;
        let mut last_sequence = None;

        loop {
            let res = reader.read_record(&mut record)?;
            if res.is_none() {
                break;
            }

            let mut edit = VersionEdit::default();
            edit.decode(record.as_slice())?;
            if edit.comparator.is_some()
                && edit.comparator.as_ref().unwrap() != self.icmp.user_comparator().name()
            {
                return Err(Error::InvalidArgument(format!(
                    "{} comparator name does not match with {}",
                    edit.comparator.as_ref().unwrap(),
                    self.icmp.user_comparator().name()
                )));
            }
            builder.apply(&edit, &mut self.compact_pointer);

            if edit.log_number.is_some() {
                log_number = edit.log_number;
            }
            if edit.prev_log_number.is_some() {
                prev_log_number = edit.prev_log_number;
            }
            if edit.next_file_number.is_some() {
                next_file_number = edit.next_file_number
            }
            if edit.last_sequence.is_some() {
                last_sequence = edit.last_sequence;
            }
        }
        if next_file_number.is_none() {
            return Err(Error::Corruption(
                "no meta-nextfile entry in descriptor".to_string(),
            ));
        } else if log_number.is_none() {
            return Err(Error::Corruption(
                "no meta-lognumber entry in descriptor".to_string(),
            ));
        } else if last_sequence.is_none() {
            return Err(Error::Corruption(
                "no last-sequence-number entry in descriptor".to_string(),
            ));
        }
        if prev_log_number.is_none() {
            prev_log_number = Some(0);
        }
        self.mark_file_number_used(prev_log_number.unwrap());
        self.mark_file_number_used(log_number.unwrap());

        // build new versioin
        let mut version = Version::new(
            self.icmp.clone(),
            self.options.clone(),
            self.table_cache.clone(),
        );
        builder.save_to(&mut version);
        self.finalize(&mut version);
        self.versions.push_front(Arc::new(version));

        self.manifest_file_number = next_file_number.unwrap();
        self.next_file_number = next_file_number.unwrap() + 1;
        self.last_sequence = last_sequence.unwrap();
        self.log_number = log_number.unwrap();
        self.prev_log_number = prev_log_number.unwrap();

        let save_manifest = false;
        // if self.reuse_manifest(description_name, &current_name) {
        //     save_manifest = true;
        // }
        Ok(save_manifest)
    }

    pub fn log_and_apply(&mut self, edit: &mut VersionEdit) -> Result<()> {
        if edit.log_number.is_some() {
            assert!(edit.log_number.unwrap() >= self.log_number);
            assert!(edit.log_number.unwrap() < self.next_file_number);
        } else {
            edit.set_log_number(self.log_number);
        }

        if edit.prev_log_number.is_none() {
            edit.set_prev_log_number(self.prev_log_number);
        }
        edit.set_next_file_number(self.next_file_number);
        edit.set_last_sequence(self.last_sequence);

        let mut version = Version::new(
            self.icmp.clone(),
            self.options.clone(),
            self.table_cache.clone(),
        );
        let mut builder = VersionBuilder::new(self.current().unwrap(), self.icmp.clone());
        builder.apply(edit, &mut self.compact_pointer);
        builder.save_to(&mut version);
        self.finalize(&mut version);

        let mut create_new_manifest = false;
        if self.descriptor_log.is_none() {
            create_new_manifest = true;
            let manifest_name = descriptor_file_name(&self.db_name, self.manifest_file_number);
            let manifest_file = self.env.new_writable_file(&manifest_name)?;
            let mut writer = LogWriter::new(manifest_file);
            match self.write_snapshot(&mut writer) {
                Ok(_) => self.descriptor_log = Some(writer),
                Err(e) => {
                    self.env.delete_file(&manifest_name)?;
                    return Err(e);
                }
            }
        }

        let mut record = Vec::new();
        edit.encode(&mut record);
        let writer = self.descriptor_log.as_mut().unwrap();
        writer.add_record(&record)?;
        writer.sync()?;

        if create_new_manifest {
            set_current_file(self.env.clone(), &self.db_name, self.manifest_file_number)?;
        }

        self.versions.push_front(Arc::new(version));

        //TODO??
        self.log_number = edit.log_number.unwrap();
        self.prev_log_number = edit.prev_log_number.unwrap();

        Ok(())
    }

    fn write_snapshot(&self, writer: &mut LogWriter<E::WritableFile>) -> Result<()> {
        let mut edit = VersionEdit::default();
        edit.set_comparator(self.icmp.user_comparator().name());

        self.compact_pointer.iter().enumerate().for_each(|(i, c)| {
            if !c.is_empty() {
                let mut key = InternalKey::empty();
                key.decode(c);
                edit.add_compact_pointer(i as u32, key);
            }
        });

        for (i, files) in self.current().unwrap().files.iter().enumerate() {
            for f in files.iter() {
                edit.add_new_file(
                    i as u32,
                    f.number,
                    f.file_size,
                    f.smallest.clone(),
                    f.largest.clone(),
                );
            }
        }
        let mut record = Vec::with_capacity(1024);
        edit.encode(&mut record);
        writer.add_record(&record)?;

        Ok(())
    }

    fn reuse_manifest(&mut self, _dscname: &str, _dscbase: &str) -> bool {
        false
    }
}

impl<E: Env> Debug for VersionSet<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VersionSet")
            .field("db_name", &self.db_name)
            .field("last_sequence", &self.last_sequence)
            .field("next_file_number", &self.next_file_number)
            .field("manifest_file_number", &self.manifest_file_number)
            .field("log_number", &self.log_number)
            .field("prev_log_number", &self.prev_log_number)
            .field("versions", &self.versions)
            .field("compact_pointer", &self.compact_pointer)
            // .field("descriptor_log", &self.descriptor_log)
            .field("pending_outputs", &self.pending_outputs)
            .finish()
    }
}

pub struct LevelFileNumIterator {
    icmp: InternalKeyComparator,
    files: Vec<Arc<FileMetaData>>,
    index: usize,
    value_buf: UnsafeCell<[u8; 16]>,
}

impl LevelFileNumIterator {
    pub fn new(icmp: InternalKeyComparator, files: Vec<Arc<FileMetaData>>) -> Self {
        let index = files.len();
        LevelFileNumIterator {
            icmp,
            files,
            index,
            value_buf: UnsafeCell::new([0; 16]),
        }
    }

    pub fn find_file(&self, target: &[u8]) -> usize {
        match self
            .files
            .binary_search_by(|f| self.icmp.compare(f.largest.encode(), target))
        {
            Ok(index) => index,
            Err(index) => index,
        }
    }
}

impl DBIterator for LevelFileNumIterator {
    fn valid(&self) -> bool {
        self.index < self.files.len()
    }

    fn seek_to_first(&mut self) {
        self.index = 0;
    }

    fn seek_to_last(&mut self) {
        self.index = if self.files.is_empty() {
            0
        } else {
            self.files.len() - 1
        }
    }

    fn seek(&mut self, target: &[u8]) {
        self.index = self.find_file(target);
    }

    fn next(&mut self) {
        assert!(self.valid());
        self.index += 1;
    }

    fn prev(&mut self) {
        assert!(self.index != 0);
        self.index -= 1;
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid());
        self.files[self.index].largest.encode()
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid());
        let num = self.files[self.index].number;
        let size = self.files[self.index].file_size;
        unsafe {
            let buf = &mut *self.value_buf.get();
            let mut write_buf = buf.as_mut();
            write_buf.write_u64_le(num).unwrap();
            write_buf.write_u64_le(size).unwrap();
            write_buf
        }
    }

    fn status(&mut self) -> Result<()> {
        Ok(())
    }
}

pub struct LevelTableIterBuilder<E: Env> {
    pub table_cache: TableCache<E>,
}
impl<E: Env> BlockIterBuilder for LevelTableIterBuilder<E> {
    type Iter = TwoLevelIterator<BlockIter, TableBlockIterBuilder<E::RandomAccessFile>>;

    fn build(&self, option: &crate::ReadOption, index_val: &[u8]) -> Result<Self::Iter> {
        let mut buf = index_val;
        let file_num = buf.read_u64_le().unwrap();
        let file_size = buf.read_u64_le().unwrap();
        let table = self.table_cache.find_table(file_num, file_size)?;
        Ok(Table::iter(table, option))
    }
}
