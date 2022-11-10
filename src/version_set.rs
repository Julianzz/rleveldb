use std::{
    collections::{HashSet, LinkedList},
    path::Path,
    sync::Arc,
};

use crate::{
    cmp::InternalKeyComparator,
    consts::{L0_COMPACTION_TRIGGER, NUM_LEVELS},
    env::{read_file_to_vec, Env},
    error::{Error, Result},
    filenames::current_file_name,
    options::Options,
    table_cache::TableCache,
    types::SequenceNumber,
    version::{max_bytes_for_level, Version, VersionBuilder},
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
            versions: versions,
            compact_pointer: Default::default(),
            descriptor_log: None,
            pending_outputs: HashSet::new(),
        }
    }

    pub fn current(&self) -> Option<Arc<Version<E>>> {
        self.versions.front().map(|f| f.clone())
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
        let mut description_name = Path::new(&self.db_name).join(current);
        let mut file = self.env.new_sequential_file(&description_name)?;
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
                return Err(Error::InvalidArgument(
                    format!(
                        "{} comparator name does not match with {}",
                        edit.comparator.as_ref().unwrap(),
                        self.icmp.user_comparator().name()
                    )
                    .into(),
                ));
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

        let mut save_manifest = false;
        // if self.reuse_manifest(description_name, &current_name) {
        //     save_manifest = true;
        // }
        Ok(save_manifest)
    }

    pub fn log_and_apply(&mut self, edit: &mut VersionEdit) -> Result<()> {
        Ok(())
    }
    fn reuse_manifest(&mut self, dscname: &String, dscbase: &String) -> bool {
        false
    }
}
