use std::collections::{HashSet, VecDeque};

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
// use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;

use crossbeam::channel::{unbounded, Receiver, Sender};

use crate::builder::build_table;
use crate::cmp::InternalKeyComparator;
use crate::env::WritableFile;
use crate::error::{Error, Result};
use crate::filenames::{
    current_file_name, descriptor_file_name, log_file_name, parse_file_name, set_current_file,
    FileType,
};
use crate::options::{ReadOption, WriteOption};
use crate::table_cache::TableCache;
use crate::types::SequenceNumber;
use crate::version::{FileMetaData, Version};
use crate::version_edit::VersionEdit;
use crate::version_set::VersionSet;
use crate::{env::Env, options::Options, write_batch::WriteBatch};
use crate::{LogReader, LogWriter, LookupKey, MemTable, ValueType};

pub struct LevelDB<E: Env> {
    inner: Arc<DBImplInner<E>>,
}

impl<E: Env> LevelDB<E> {
    pub fn open(options: Options, db_name: impl Into<String>, env: E) -> Result<Self> {
        let db_name = db_name.into();
        let db = DBImplInner::new(options, &db_name, env.clone());
        let mut edit = VersionEdit::default();
        let mut save_manifest = false;
        db.recovery(&mut edit, &mut save_manifest)?;

        {
            let mut mem = db.mem.write().unwrap();
            let mut versions = db.versions.lock().unwrap();
            let mut wal = db.wal.lock().unwrap();
            if mem.is_none() {
                let new_log_number = versions.new_file_number();
                let file = env.new_writable_file(&log_file_name(&db_name, new_log_number))?;
                wal.log_file_number = new_log_number;
                wal.log = Some(LogWriter::new(file));
                *mem = Some(Arc::new(MemTable::new(db.internal_comparator.clone())));
            }

            if save_manifest {
                edit.set_prev_log_number(0);
                edit.set_log_number(wal.log_file_number);
                versions.log_and_apply(&mut edit)?;
            }
        }

        Ok(LevelDB {
            inner: Arc::new(db),
        })
    }

    pub fn write(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        let write_option = WriteOption::default();
        self.inner.write(&write_option, Some(batch))
    }

    fn run_compaction_worker(&self) {
        let inner = self.inner.clone();
        thread::Builder::new()
            .name("compaction".to_string())
            .spawn(move || {
                while let Ok(_) = inner.compaction_trigger.1.recv() {
                    if inner.shutdown.load(Ordering::Acquire) {
                        break;
                    }
                }
            })
            .unwrap();
    }
}

struct Wal<W: WritableFile> {
    pub log_file_number: u64,
    pub log: Option<LogWriter<W>>,
}

const NUM_NON_TABLE_CACHE_FILES: u64 = 10;

fn table_cache_size(sanitized_options: &Arc<Options>) -> u64 {
    sanitized_options.max_open_files - NUM_NON_TABLE_CACHE_FILES
}

struct Writer {
    batch: Option<WriteBatch>,
    notifier: Sender<Result<()>>,
    sync: bool,
}

enum BatchTask {
    Write(Writer),
    close,
}

pub struct DBImplInner<E: Env> {
    db_name: String,
    env: E,
    internal_comparator: InternalKeyComparator,
    options: Arc<Options>,
    table_cache: TableCache<E>,

    mem: RwLock<Option<Arc<MemTable>>>,
    imm: RwLock<Option<Arc<MemTable>>>,

    versions: Mutex<VersionSet<E>>,

    batch_write_queue: Mutex<VecDeque<BatchTask>>,
    batch_write_cond: Condvar,

    //background_error: RwLock<Option<Error>>,
    shutdown: AtomicBool,

    compaction_trigger: (Sender<()>, Receiver<()>),
    background_work_finish: Condvar,

    wal: Mutex<Wal<E::WritableFile>>,
}

unsafe impl<E: Env> Send for DBImplInner<E> {}
unsafe impl<E: Env> Sync for DBImplInner<E> {}

impl<E: Env> DBImplInner<E> {
    pub fn new(options: Options, db_name: impl Into<String>, env: E) -> Self {
        let db_name = db_name.into();
        let options = Arc::new(options);
        let table_cache = TableCache::new(
            db_name.clone(),
            options.clone(),
            env.clone(),
            table_cache_size(&options),
        );
        let icmp = InternalKeyComparator::new(options.comparator.clone());
        DBImplInner {
            internal_comparator: icmp.clone(),
            db_name: db_name.clone(),
            env: env.clone(),
            options: options.clone(),
            table_cache: table_cache.clone(),
            mem: RwLock::new(None),
            imm: RwLock::new(None),
            versions: Mutex::new(VersionSet::new(env, db_name, options, table_cache, icmp)),
            batch_write_queue: Mutex::new(VecDeque::new()),
            batch_write_cond: Condvar::new(),
            shutdown: AtomicBool::new(false),
            compaction_trigger: unbounded(),
            background_work_finish: Condvar::new(),
            wal: Mutex::new(Wal {
                log_file_number: 0,
                log: None,
            }),
        }
    }

    fn make_room_for_write(&self, force: bool) {
        let mut allow_delay = !force;
        let mut versions = self.versions.lock().unwrap();
        loop {
            // process error
        }
    }

    pub fn write(&self, options: &WriteOption, updates: Option<WriteBatch>) -> Result<()> {
        // let (sender, receiver) = unbounded();
        // let task = BatchTask::Write(Writer {
        //     batch: updates,
        //     notifier: sender,
        //     sync: options.sync,
        // });

        // self.batch_write_queue.lock().unwrap().push_back(task);
        // self.batch_write_cond.notify_all();
        // //
        // receiver.recv()?;
        if updates.is_some() {
            self.write_inner(&mut updates.unwrap(), options)?;
        }

        Ok(())
    }

    pub fn recovery(&self, edit: &mut VersionEdit, save_manifest: &mut bool) -> Result<()> {
        let db_path = Path::new(&self.db_name);
        let _ = self.env.create_dir(&db_path);
        if !self.env.file_exists(&current_file_name(db_path)) {
            if self.options.create_if_missing {
                self.new_db()?;
            } else {
                return Err(Error::InvalidArgument("db not exists".into()));
            }
        } else if self.options.error_if_exists {
            return Err(Error::InvalidArgument("db exists".into()));
        }

        let mut versions = self.versions.lock().unwrap();
        *save_manifest = versions.recover()?;

        // recovery logs TODO
        let min_log = versions.log_number();
        let prev_log = versions.prev_log_number();
        let mut file_names = Vec::new();
        self.env.get_children(&db_path, &mut file_names)?;
        let mut expect = HashSet::new();
        versions.live_files(&mut expect);

        let mut logs = Vec::new();
        for f in file_names.iter() {
            if let Ok((number, file_type)) = parse_file_name(f) {
                expect.remove(&number);
                if file_type == FileType::Log && (number >= min_log || number == prev_log) {
                    logs.push(number);
                }
            }
        }
        if !expect.is_empty() {
            return Err(Error::Corruption(
                format!("missing files: {:?}", expect).into(),
            ));
        }
        drop(versions);

        logs.sort();
        let mut max_sequence = 0;
        for (i, number) in logs.iter().enumerate() {
            self.recovery_log_file(
                *number,
                i == logs.len() - 1,
                save_manifest,
                edit,
                &mut max_sequence,
            )?;
        }

        let mut versions = self.versions.lock().unwrap();
        if logs.len() > 0 {
            versions.mark_file_number_used(*logs.last().unwrap());
        }
        if versions.last_sequence() < max_sequence {
            versions.set_last_sequence(max_sequence);
        }

        Ok(())
    }

    pub fn recovery_log_file(
        &self,
        log_number: u64,
        last_log: bool,
        save_manifest: &mut bool,
        edit: &mut VersionEdit,
        max_sequence: &mut SequenceNumber,
    ) -> Result<()> {
        let fname = log_file_name(&self.db_name, log_number);
        let file = self.env.new_sequential_file(&fname)?;
        let mut log_reader = LogReader::new(file, true);

        let mut mem = None;

        let buffer_size = self.options.write_buffer_size;
        let paranoid_checks = self.options.paranoid_checks;
        let mut compaction = 0;
        loop {
            let mut batch = WriteBatch::new();
            let mut record = Vec::with_capacity(1024);

            //finish read
            if log_reader.read_record(&mut record)? == None {
                break;
            };

            batch.set_content(record);

            if mem.is_none() {
                mem.replace(Arc::new(MemTable::new(self.internal_comparator.clone())));
            }
            let memtable = mem.as_ref().unwrap();
            batch.insert_into(memtable.clone())?;

            let last_sequence = batch.sequence() + batch.count() as SequenceNumber - 1;
            if last_sequence > *max_sequence {
                *max_sequence = last_sequence;
            }

            if memtable.approximate_memory_usage() > buffer_size as usize {
                compaction += 1;
                *save_manifest = true;
                self.write_level0_table(memtable.clone(), edit, None)?;
                mem = None;
            }
        }

        //todo resuse log
        // if res.is_ok() && self.options.reuse_log && last_log && compaction == 0 {
        //     let mut inner_mem = self.mem.write().unwrap();
        //     assert!(inner_mem.is_none());

        //     let log_file = self.env.new_appendable_file(&fname);
        //     let lfile_size = self.env.get_file_size(&fname);
        //     if let (Ok(log), Ok(s)) = (log_file, lfile_size) {
        //         let mut wal = self.wal.lock().unwrap();
        //         assert!(wal.log.is_none());
        //         let writer = LogWriter::new_with_dest_len(log, s);
        //         wal.log = Some(writer);
        //         wal.log_file_number = log_number;
        //         if mem.is_some() {
        //             inner_mem.get_or_insert(mem.take().unwrap());
        //         } else {
        //             inner_mem
        //                 .get_or_insert(Arc::new(MemTable::new(self.internal_comparator.clone())));
        //         }
        //     }
        // }

        if let Some(m) = mem {
            self.write_level0_table(m, edit, None)?;
            *save_manifest = true;
        }
        Ok(())
    }

    pub fn get(&self, option: &ReadOption, key: &[u8], value: &mut Vec<u8>) -> Result<()> {
        let snapshot = self.versions.lock().unwrap().last_sequence();
        let lookup_key = LookupKey::new(key, snapshot, ValueType::Value);
        let memtable = self.mem.read().unwrap().as_ref().unwrap();
        // if let Some(v) = memtable.
        Ok(())
    }

    fn write_inner(&self, batch: &mut WriteBatch, options: &WriteOption) -> Result<()> {
        let versions: std::sync::MutexGuard<VersionSet<E>> = self.versions.lock().unwrap();
        let mut last_sequence = versions.last_sequence();
        batch.set_sequence(last_sequence + 1);
        last_sequence += batch.count() as u64;
        drop(versions);

        let mut wal = self.wal.lock().unwrap();
        let log_writter = wal.log.as_mut().unwrap();
        log_writter.add_record(batch.content())?;

        if options.sync {
            let res = log_writter.sync();
            if res.is_err() {
                // record sync error
            }
            res?;
        }

        let mem = self.mem.read().unwrap();
        let mem = mem.as_ref().unwrap();
        batch.insert_into(mem.clone())?;

        Ok(())
    }

    pub fn write_batch_task(&mut self) {
        let mut queue = self.batch_write_queue.lock().unwrap();
    }

    pub fn delete_obsoleted_files(&self) {}

    fn write_level0_table(
        &self,
        mem: Arc<MemTable>,
        edit: &mut VersionEdit,
        base: Option<Arc<Version<E>>>,
    ) -> Result<()> {
        let mut versions = self.versions.lock().unwrap();
        let mut meta = FileMetaData::default();
        meta.number = versions.new_file_number();
        versions.pending_outputs.insert(meta.number);
        drop(versions);

        let iter = mem.iter();
        let res = build_table(
            &self.db_name,
            self.env.clone(),
            &self.options,
            self.table_cache.clone(),
            iter,
            &mut meta,
        );

        let mut versions = self.versions.lock().unwrap();
        versions.pending_outputs.remove(&meta.number);

        let mut level = 0;
        if res.is_ok() && meta.file_size > 0 {
            let smallest_user_key = meta.smallest.user_key();
            let largest_user_key = meta.largest.user_key();
            if let Some(v) = base {
                level = v.pick_level_for_memtable_output(
                    &Some(smallest_user_key),
                    &Some(largest_user_key),
                );
            }
            edit.add_new_file(
                level as u32,
                meta.number,
                meta.file_size,
                meta.smallest.clone(),
                meta.largest.clone(),
            );
        }

        //TODO add stats
        res
    }

    fn background_compaction(&self) {
        if self.imm.read().unwrap().is_some() {
            self.compaction_memtable();
        }
    }

    pub fn compaction_memtable(&self) {
        if let Err(e) = self.do_compaction_memtable() {
            //TODO
            panic!("error in compaction table");
        }
    }

    pub fn do_compaction_memtable(&self) -> Result<()> {
        let imm = self.imm.read().unwrap().as_ref().unwrap().clone();
        let mut edit = VersionEdit::default();
        let current = self.versions.lock().unwrap().current();

        self.write_level0_table(imm, &mut edit, current);
        if self.shutdown.load(Ordering::Acquire) {
            return Err(Error::CustomError(
                "deleting db during memtable compaction".into(),
            ));
        }

        edit.set_prev_log_number(0);
        edit.set_log_number(self.wal.lock().unwrap().log_file_number);

        let mut imm = self.imm.write().unwrap();
        *imm = None;

        self.delete_obsoleted_files();

        Ok(())
    }

    fn maybe_schedule_compaction(&self) {
        if self.shutdown.load(Ordering::Acquire) {
            return;
        }

        self.compaction_trigger.0.send(()).unwrap();
    }

    fn new_db(&self) -> Result<()> {
        let mut edit = VersionEdit::default();
        let comparator = self.internal_comparator.user_comparator();
        edit.set_comparator(comparator.name());
        edit.set_log_number(0);
        edit.set_next_file_number(2);
        edit.set_last_sequence(0);

        let manifest = descriptor_file_name(&self.db_name, 1);
        let file = self.env.new_writable_file(&manifest)?;
        let mut log = LogWriter::new(file);
        let mut record = Vec::new();
        edit.encode(&mut record);

        let mut res = log.add_record(record);
        if res.is_ok() {
            res = set_current_file(self.env.clone(), &self.db_name, 1);
        } else {
            let _ = self.env.delete_file(&manifest);
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use crate::env::posix::PosixEnv;

    use super::*;

    #[test]
    fn test_base_insert() {
        let mut options = Options::default();
        options.create_if_missing = true;
        let db_name = "demo";
        let env = PosixEnv {};
        let db = LevelDB::open(options, db_name, env).unwrap();

        db.write("liu".as_bytes(), "zhong".as_bytes());
    }
}
