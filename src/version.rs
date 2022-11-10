use rand::seq::index;

use crate::{
    cmp::{Comparator, InternalKeyComparator},
    consts::{MAX_MEM_COMPACT_LEVEL, NUM_LEVELS},
    env::Env,
    format::InternalKey,
    options::Options,
    table_cache::TableCache,
    types::MAX_SEQUENCE_NUMBER,
    version_edit::VersionEdit,
    ValueType,
};
use std::{
    cmp::Ordering,
    collections::HashSet,
    sync::{Arc, RwLock},
};

#[derive(Default, Clone)]
pub struct FileMetaData {
    pub allowed_seeks: i32,
    pub number: u64,
    pub file_size: u64,
    pub smallest: InternalKey,
    pub largest: InternalKey,
}

pub struct Version<E: Env> {
    pub table_cache: TableCache<E>,
    pub options: Arc<Options>,
    pub files: [Vec<Arc<FileMetaData>>; NUM_LEVELS],
    pub file_to_compact: RwLock<Option<(Arc<FileMetaData>, usize)>>,
    pub cmp: InternalKeyComparator,
    pub compaction_score: f64,
    pub compaction_level: i32,
}

impl<E: Env> Version<E> {
    pub fn new(
        cmp: InternalKeyComparator,
        options: Arc<Options>,
        table_cache: TableCache<E>,
    ) -> Self {
        Version {
            table_cache,
            options,
            files: Default::default(),
            file_to_compact: RwLock::new(None),
            cmp,
            compaction_score: -1f64,
            compaction_level: -1,
        }
    }

    // pub fn get(
    //     &self,
    //     option: &ReadOption,
    //     k: &LookupKey,
    //     val: &mut Vec<u8>,
    // ) -> Result<(Arc<FileMetaData>, usize)> {
    // }

    fn get_overlapping_inputs(
        &self,
        level: usize,
        begin: &Option<InternalKey>,
        end: &Option<InternalKey>,
        input: &mut Vec<Arc<FileMetaData>>,
    ) {
        assert!(level < NUM_LEVELS);
        let user_cmp = self.cmp.user_comparator();
        let mut user_begin = begin.as_ref().map(|k| k.user_key());
        let mut user_end = end.as_ref().map(|k| k.user_key());
        let mut i = 0;
        while i < self.files[level].len() {
            let f = &self.files[level][i];
            i += 1;
            if !self.before_file(&user_cmp, &user_end, &f)
                && !self.after_file(&user_cmp, &user_begin, &f)
            {
                input.push(f.clone());
                if level == 0 {
                    if begin.is_some()
                        && user_cmp.compare(&f.smallest.user_key(), user_begin.as_ref().unwrap())
                            == Ordering::Less
                    {
                        i = 0;
                        input.clear();
                        user_begin = user_begin.map(|_| f.smallest.user_key());
                    } else if end.is_some()
                        && user_cmp.compare(&f.largest.user_key(), user_end.as_ref().unwrap())
                            == Ordering::Greater
                    {
                        i = 0;
                        input.clear();
                        user_end = user_end.map(|_| f.largest.user_key())
                    }
                }
            }
        }
    }

    pub fn pick_level_for_memtable_output(
        &self,
        smallest_user_key: &Option<&[u8]>,
        largest_user_key: &Option<&[u8]>,
    ) -> usize {
        let mut level = 0;
        if !self.overlap_in_level(0, smallest_user_key, largest_user_key) {
            let start = smallest_user_key
                .as_ref()
                .map(|&f| InternalKey::new(f, MAX_SEQUENCE_NUMBER, ValueType::Value));
            let limit = smallest_user_key
                .as_ref()
                .map(|&f| InternalKey::new(f, 0, ValueType::Deletetion));

            let mut overlaps = Vec::new();
            while level < MAX_MEM_COMPACT_LEVEL {
                if self.overlap_in_level(level + 1, smallest_user_key, largest_user_key) {
                    break;
                }
                if level + 2 < NUM_LEVELS {
                    self.get_overlapping_inputs(level + 2, &start, &limit, &mut overlaps);
                    let sum = Self::total_file_size(&overlaps);
                    if sum > grand_parent_overlap_bytes(&self.options) as u64 {
                        break;
                    }
                }
                level += 1;
            }
        }
        level
    }

    fn overlap_in_level(
        &self,
        level: usize,
        smallest: &Option<&[u8]>,
        largest: &Option<&[u8]>,
    ) -> bool {
        let files = &self.files[level];
        self.some_file_overlaps_range(level > 0, files, smallest, largest)
    }

    pub fn some_file_overlaps_range(
        &self,
        disjoint_sorted_files: bool,
        files: &Vec<Arc<FileMetaData>>,
        smallest: &Option<&[u8]>,
        largest: &Option<&[u8]>,
    ) -> bool {
        let ucmp = self.cmp.user_comparator();
        if !disjoint_sorted_files {
            for file in files {
                if self.before_file(&ucmp, smallest, file) || self.after_file(&ucmp, largest, file)
                {
                    continue;
                } else {
                    return true;
                }
            }
            false
        } else {
            let mut index = 0;
            if let &Some(k) = smallest {
                index = match files.binary_search_by(|f| ucmp.compare(f.largest.user_key(), k)) {
                    Ok(index) => index,
                    Err(index) => index,
                }
            }
            if index >= files.len() {
                false
            } else {
                !self.before_file(&ucmp, largest, &files[index])
            }
        }
    }

    fn before_file(
        &self,
        ucmp: &Arc<dyn Comparator>,
        user_key: &Option<&[u8]>,
        file: &Arc<FileMetaData>,
    ) -> bool {
        if let &Some(key) = user_key {
            ucmp.compare(key, &file.smallest.user_key()) == Ordering::Less
        } else {
            false
        }
    }
    fn after_file(
        &self,
        ucmp: &Arc<dyn Comparator>,
        user_key: &Option<&[u8]>,
        file: &Arc<FileMetaData>,
    ) -> bool {
        if let &Some(key) = user_key {
            ucmp.compare(key, file.largest.user_key()) == Ordering::Greater
        } else {
            false
        }
    }

    fn find_file(
        &self,
        icmp: &InternalKeyComparator,
        files: &Vec<Arc<FileMetaData>>,
        key: &[u8],
    ) -> usize {
        match files.binary_search_by(|f| icmp.compare(key, f.largest.encode())) {
            Ok(index) => index,
            Err(index) => index,
        }
    }

    pub fn level_total_file_size(&self, level: usize) -> u64 {
        assert!(level < NUM_LEVELS);
        self.files[level]
            .iter()
            .map(|m| m.file_size)
            .fold(0, |acc, i| acc + i)
    }
    pub fn total_file_size(files: &Vec<Arc<FileMetaData>>) -> u64 {
        files.iter().map(|m| m.file_size).fold(0, |acc, i| acc + i)
    }
}

pub fn max_bytes_for_level(mut level: usize) -> f64 {
    // Note: the result for level zero is not really used since we set
    // the level-0 compaction threshold based on number of files.

    // Result for both level-0 and level-1
    let mut result = 10f64 * 1048576.0f64;
    while level > 1 {
        result *= 10f64;
        level -= 1;
    }

    result
}
pub struct VersionBuilder<E: Env> {
    base: Arc<Version<E>>,
    icmp: InternalKeyComparator,
    deleted_files: Vec<HashSet<u64>>,
    added_files: Vec<Vec<Arc<FileMetaData>>>,
}

impl<E: Env> VersionBuilder<E> {
    pub fn new(base: Arc<Version<E>>, icmp: InternalKeyComparator) -> Self {
        VersionBuilder {
            base,
            icmp,
            deleted_files: vec![HashSet::new(); NUM_LEVELS],
            added_files: vec![Vec::new(); NUM_LEVELS],
        }
    }

    pub fn apply(&mut self, edit: &VersionEdit, compact_pointers: &mut [Vec<u8>]) {
        for (level, key) in edit.compact_pointers.iter() {
            let v = &mut compact_pointers[*level as usize];
            v.clear();
            v.extend_from_slice(key.encode());
        }

        for (level, f) in edit.deleted_files.iter() {
            let deleted = &mut self.deleted_files[*level as usize];
            deleted.insert(*f);
        }

        for (level, file) in edit.new_files.iter() {
            let mut file_meta = file.clone();

            // todo
            file_meta.allowed_seeks = (file_meta.file_size / 16384) as i32;
            if file_meta.allowed_seeks < 100 {
                file_meta.allowed_seeks = 100;
            }
            self.deleted_files[*level as usize].remove(&file_meta.number);
            self.added_files[*level as usize].push(Arc::new(file_meta));
        }
    }

    pub fn save_to(&mut self, version: &mut Version<E>) {
        let icmp = self.icmp.clone();
        for x in self.added_files.iter_mut() {
            x.sort_by(|f1, f2| icmp.compare(f1.smallest.encode(), f2.smallest.encode()));
        }
        for level in 0..NUM_LEVELS {
            let mut base_iter = self.base.files[level].iter().peekable();
            let mut add_iter = self.added_files[level].iter().peekable();
            while let Some(add_file) = add_iter.next() {
                while let Some(&base_file) = base_iter.peek() {
                    if icmp.compare(base_file.smallest.encode(), add_file.smallest.encode())
                        == Ordering::Less
                    {
                        self.maybe_add_file(version, level, base_file.clone());
                        base_iter.next();
                    }
                }
                self.maybe_add_file(version, level, add_file.clone());
            }
            base_iter.for_each(|f| self.maybe_add_file(version, level, f.clone()));
        }
    }

    fn maybe_add_file(&self, version: &mut Version<E>, level: usize, file_meta: Arc<FileMetaData>) {
        if self.deleted_files[level].contains(&file_meta.number) {
            return;
        }

        let last = &mut version.files[level].last();

        //check last order
        let check_last = last
            .map(|f| {
                self.icmp
                    .compare(f.largest.encode(), file_meta.smallest.encode())
                    == Ordering::Less
            })
            .unwrap_or(true);
        assert!(level != 0 || check_last);

        version.files[level].push(file_meta);
    }
}

fn target_file_size(options: &Arc<Options>) -> usize {
    options.max_file_size
}

fn grand_parent_overlap_bytes(options: &Arc<Options>) -> usize {
    10 * target_file_size(options)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pick_memtable_files() {}
}
