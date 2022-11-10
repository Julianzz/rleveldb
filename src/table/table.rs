use std::{cmp::Ordering, io::Write, sync::Arc};

use snap::write::FrameEncoder;

use crate::{
    cmp::{BitWiseComparator, Comparator},
    codec::NumberEncoder,
    env::{RandomAccessFile, WritableFile},
    error::{Error, Result},
    iterator::DBIterator,
    options::{Compress, Options, ReadOption},
};

use super::{
    block::{Block, BlockIter},
    block_builder::BlockBuilder,
    filter_block::FilterBlockBuilder,
    format::{BlockContent, BlockHandle, Footer, BLOCK_TRAILER_SIZE, FULL_FOOTER_LENGTH},
    two_level_iterator::{BlockIterBuilder, TwoLevelIterator},
};

pub struct Table<R: RandomAccessFile> {
    file: R,
    options: Arc<Options>,

    meta_index_handle: BlockHandle,
    index_block: Block,
    filter_block_data: Option<BlockContent>,
}

impl<R: RandomAccessFile> Table<R> {
    pub fn open(options: Arc<Options>, file: R, size: u64) -> Result<Self> {
        if FULL_FOOTER_LENGTH > size as usize {
            return Err(Error::Corruption("file is too short to be sstable".into()));
        }

        let mut scratch = [0u8; FULL_FOOTER_LENGTH];
        file.read_exact_at(&mut scratch, size - FULL_FOOTER_LENGTH as u64)?;
        let mut footer = Footer::default();
        footer.decode(&scratch)?;

        let read_options = ReadOption {
            verify_checksum: true,
            fill_cache: false,
        };
        let index_content =
            BlockContent::read_block_from_file(&file, &footer.index_handle, &read_options)?;
        let index_block = Block::from_raw(index_content)?;

        // read meta , ignore error
        let filter_meta_data = if let Ok(meta_data) = Self::read_meta(&file, &options, &footer) {
            meta_data
        } else {
            None
        };
        // TODO cache
        let table = Table {
            file: file,
            options: options,
            meta_index_handle: footer.meta_index_handle,
            index_block: index_block,
            filter_block_data: filter_meta_data,
        };
        Ok(table)
    }

    fn read_meta(
        file: &R,
        options: &Arc<Options>,
        footer: &Footer,
    ) -> Result<Option<BlockContent>> {
        if let Some(ref policy) = options.filter_policy {
            let read_option = ReadOption {
                verify_checksum: true,
                fill_cache: false,
            };
            let meta_block_content =
                BlockContent::read_block_from_file(file, &footer.meta_index_handle, &read_option)?;
            let meta_block = Block::from_raw(meta_block_content)?;
            let comparator = BitWiseComparator {};
            let mut iter = meta_block.iter(Arc::new(comparator));
            let mut key = Vec::from("filter");
            key.extend_from_slice(policy.name().as_bytes());

            iter.seek(&key);
            if iter.valid() && comparator.compare(&key, iter.key()) == Ordering::Equal {
                let mut handle = BlockHandle::default();
                handle.decode(iter.value());
                let filter_block_content =
                    BlockContent::read_block_from_file(file, &handle, &read_option)?;
                return Ok(Some(filter_block_content));
            }
        }
        Ok(None)
    }

    fn block_iter_from_index(
        &self,
        read_option: &ReadOption,
        index_value: &[u8],
    ) -> Result<BlockIter> {
        let block_handle = BlockHandle::from_raw(index_value)
            .ok_or_else(|| Error::Corruption("decode block handle corruption".into()))?;
        //TODO add cache

        let block_content =
            BlockContent::read_block_from_file(&self.file, &block_handle, read_option)?;
        let block = Block::from_raw(block_content)?;
        Ok(block.iter(self.options.comparator.clone()))
    }
    pub(crate) fn iter(
        self: Arc<Table<R>>,
        option: ReadOption,
    ) -> TwoLevelIterator<BlockIter, TableBlockIterBuilder<R>> {
        let index_iter = self.index_block.iter(self.options.comparator.clone());
        let block_iter_builder = TableBlockIterBuilder { table: self };

        TwoLevelIterator::new(index_iter, block_iter_builder, option)
    }

    pub(crate) fn print_indexes(&self) {
        let mut index_iter = self.index_block.iter(self.options.comparator.clone());
        index_iter.seek_to_first();
        loop {
            if !index_iter.valid() {
                break;
            }
            let key = index_iter.key();
            let value = index_iter.value();

            let block_handle = BlockHandle::from_raw(value).unwrap();
            eprintln!(
                "{}  -> {} {}",
                String::from_utf8_lossy(key),
                block_handle.offset(),
                block_handle.size()
            );

            index_iter.next();
        }
    }
}

pub(crate) struct TableBlockIterBuilder<R: RandomAccessFile> {
    table: Arc<Table<R>>,
}
impl<R: RandomAccessFile> BlockIterBuilder for TableBlockIterBuilder<R> {
    type Iter = BlockIter;

    fn build(&self, option: &ReadOption, index_val: &[u8]) -> Result<Self::Iter> {
        self.table.block_iter_from_index(option, index_val)
    }
}

pub struct TableBuiler<W: WritableFile> {
    options: Arc<Options>,
    file: W,

    offset: u64,
    data_block: Option<BlockBuilder>,
    index_block: Option<BlockBuilder>,

    last_key: Vec<u8>,
    num_entries: u64,

    filter_block: Option<FilterBlockBuilder>,

    pending_index_entry: bool,
    pending_handle: BlockHandle,
    compress_out: Vec<u8>,
}

impl<W: WritableFile> TableBuiler<W> {
    pub fn new(options: Arc<Options>, file: W) -> Self {
        let data_block =
            BlockBuilder::new(options.comparator.clone(), options.block_restart_interval);
        let index_block = BlockBuilder::new(options.comparator.clone(), 1);
        let filter_block = options.filter_policy.clone().map(|policy| {
            let mut filter_block_builder = FilterBlockBuilder::new(policy);
            filter_block_builder.start_block(0);
            filter_block_builder
        });
        TableBuiler {
            options: options,
            file: file,
            offset: 0,
            data_block: Some(data_block),
            index_block: Some(index_block),
            last_key: Vec::new(),
            num_entries: 0,
            filter_block,
            pending_index_entry: false,
            pending_handle: Default::default(),
            compress_out: Vec::new(),
        }
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(self.data_block.is_some());
        assert!(self.index_block.is_some());

        if self.num_entries > 0 {
            assert_eq!(
                self.options.comparator.compare(key, &self.last_key),
                Ordering::Greater
            );
        }

        if self.pending_index_entry {
            assert!(self.data_block.as_ref().unwrap().is_empty());
            self.options
                .comparator
                .find_shortest_separator(&mut self.last_key, key);
            let mut handle_encoding = vec![0; 16];
            self.pending_handle.encode(&mut handle_encoding);
            self.index_block
                .as_mut()
                .map(|b| b.add(&self.last_key, &handle_encoding));
            self.pending_index_entry = false;
        }

        if let Some(ref mut filter) = self.filter_block {
            filter.add_key(key);
        }

        self.last_key.clear();
        self.last_key.extend_from_slice(&key);
        self.num_entries += 1;
        let data_block = self.data_block.as_mut().unwrap();
        data_block.add(key, value);

        let estimated_size = data_block.current_size_estimate();
        if estimated_size >= self.options.block_size {
            self.flush()?;
        }
        Ok(())
    }
    pub fn flush(&mut self) -> Result<()> {
        assert!(self.data_block.is_some());

        let mut data_block = self
            .data_block
            .replace(BlockBuilder::new(
                self.options.comparator.clone(),
                self.options.block_restart_interval,
            ))
            .unwrap();
        if data_block.is_empty() {
            return Ok(());
        }
        assert!(!self.pending_index_entry);

        let offset = write_block(
            &mut self.file,
            data_block,
            &mut self.pending_handle,
            self.options.compression_type,
            &mut self.compress_out,
            self.offset,
        )?;
        self.offset = offset;
        self.pending_index_entry = true;
        self.file.flush()?;

        self.filter_block
            .as_mut()
            .map(|b| b.start_block(offset as usize));

        Ok(())
    }

    pub fn finish(mut self, sync: bool) -> Result<u64> {
        self.flush()?;

        let mut meta_index_block = BlockBuilder::new(
            self.options.comparator.clone(),
            self.options.block_restart_interval,
        );
        let mut meta_index_block_handle: BlockHandle = Default::default();
        if let Some(filter_builder) = self.filter_block {
            let mut filter_block_handle = BlockHandle::new(0, 0);
            let block_content = filter_builder.finish();
            self.offset = write_raw_block(
                &mut self.file,
                &block_content,
                self.options.compression_type,
                &mut filter_block_handle,
                self.offset,
            )?;
            let mut key = Vec::from("filter".as_bytes());
            if let Some(policy) = &self.options.filter_policy {
                key.extend_from_slice(policy.name().as_bytes());
                let mut handle_encoding = vec![0; 16];
                let off = filter_block_handle.encode(&mut handle_encoding);
                meta_index_block.add(&key[..], &handle_encoding[..off]);
            }
        }

        self.offset = write_block(
            &mut self.file,
            meta_index_block,
            &mut meta_index_block_handle,
            self.options.compression_type,
            &mut self.compress_out,
            self.offset,
        )?;

        let mut index_block_handle = BlockHandle::default();
        let mut index_block = self.index_block.take().unwrap();
        if self.pending_index_entry {
            self.options
                .comparator
                .find_shortest_successor(&mut self.last_key);
            let mut handle_encoding = vec![0; 16];
            let off = self.pending_handle.encode(&mut handle_encoding);
            index_block.add(&self.last_key[..], &handle_encoding[..off]);
            self.pending_index_entry = false;
        }
        self.offset = write_block(
            &mut self.file,
            index_block,
            &mut index_block_handle,
            self.options.compression_type,
            &mut self.compress_out,
            self.offset,
        )?;

        let footer = Footer::new(meta_index_block_handle, index_block_handle);
        let mut buf = vec![0; FULL_FOOTER_LENGTH];
        footer.encode(&mut buf);

        self.file.append(&buf)?;
        self.offset += buf.len() as u64;

        if sync {
            self.file.sync();
        }

        Ok(self.offset)
    }
}

fn write_block<W: WritableFile>(
    file: &mut W,
    mut block: BlockBuilder,
    handle: &mut BlockHandle,
    compress_type: Compress,
    compress_out: &mut Vec<u8>,
    offset: u64,
) -> Result<u64> {
    let raw = block.finish();
    let (compress_type, block_content) = match compress_type {
        Compress::NO => (Compress::NO, raw.as_slice()),
        Compress::Snappy => {
            compress_out.clear();
            {
                let mut encoder = FrameEncoder::new(&mut (*compress_out));
                encoder.write_all(&raw)?;
            }
            if compress_out.len() < raw.len() - (raw.len() / 8) {
                (Compress::Snappy, compress_out.as_slice())
            } else {
                (Compress::NO, raw.as_slice())
            }
        }
    };

    let offset = write_raw_block(file, block_content, compress_type, handle, offset)?;
    Ok(offset)
}

fn write_raw_block<W: WritableFile>(
    file: &mut W,
    block_content: &[u8],
    compress_type: Compress,
    handle: &mut BlockHandle,
    offset: u64,
) -> Result<u64> {
    handle.set_offset(offset);
    handle.set_size(block_content.len() as u64);

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(block_content);
    hasher.update(&[compress_type.as_byte()]);
    let checksum = hasher.finalize();

    let mut trailer = [0u8; BLOCK_TRAILER_SIZE];
    let mut buf = trailer.as_mut();
    buf.encode_u8(compress_type.as_byte()).unwrap();
    buf.encode_u32_le(checksum).unwrap();

    file.append(block_content)?;
    file.append(&trailer)?;

    Ok(offset + block_content.len() as u64 + BLOCK_TRAILER_SIZE as u64)
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use crate::{cmp::BitWiseComparator, env::RandomAccessFile, filter::BloomFilterPolicy};

    use super::*;
    pub struct MemFs {
        data: Rc<RefCell<Vec<u8>>>,
    }

    impl MemFs {
        pub fn new(data: Rc<RefCell<Vec<u8>>>) -> Self {
            MemFs { data: data }
        }
    }

    impl RandomAccessFile for MemFs {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
            let data = self.data.borrow();
            let data = &data[offset as usize..offset as usize + buf.len()];
            buf.copy_from_slice(data);
            Ok(buf.len())
        }

        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
            self.read_at(buf, offset)?;
            Ok(())
        }
    }

    impl WritableFile for MemFs {
        fn append(&mut self, data: &[u8]) -> Result<()> {
            let mut v = self.data.as_ref().borrow_mut();
            v.extend_from_slice(data);
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }

        fn sync(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_build_table() {
        let mut datas: Vec<(String, String)> = Vec::new();
        for i in 0..10000 {
            let key = format!("liuzhenzhong{:06}", i);
            let value = format!("zhong:{:06}", i);
            datas.push((key, value));
        }

        let data = Rc::new(RefCell::new(Vec::new()));
        let file = MemFs::new(data.clone());
        let options = Arc::new(Options {
            comparator: Arc::new(BitWiseComparator {}),
            filter_policy: Some(Arc::new(BloomFilterPolicy::new(3))),
            block_restart_interval: 3,
            block_size: 1024,
            compression_type: Compress::NO,
            ..Default::default()
        });

        let mut table_builder = TableBuiler::new(options.clone(), file);
        for (k, v) in datas.iter() {
            table_builder.add(k.as_bytes(), v.as_bytes()).unwrap();
        }

        table_builder.finish(true).unwrap();
        let v = data.as_ref().borrow();
        // assert_eq!(v.len(), 20);

        let file = MemFs::new(data.clone());
        let table = Arc::new(Table::open(options.clone(), file, v.len() as u64).unwrap());

        let read_option = ReadOption {
            verify_checksum: true,
            fill_cache: false,
        };

        table.print_indexes();

        let mut data_iter = datas.iter();
        let mut iter = table.iter(read_option);
        iter.seek_to_first();
        loop {
            if !iter.valid() {
                break;
            }

            let (origin_key, origin_value) = data_iter.next().unwrap();

            assert_eq!(String::from_utf8_lossy(iter.key()), *origin_key);
            assert_eq!(String::from_utf8_lossy(iter.value()), *origin_value);

            iter.next();
        }
    }
}
