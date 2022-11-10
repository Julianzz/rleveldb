use std::{mem::forget, sync::Arc};

use crate::{
    env::Env,
    error::Result,
    filenames::table_file_name,
    format::InternalKey,
    iterator::DBIterator,
    options::{Options, ReadOption},
    table::table::{Table, TableBuiler},
    table_cache::TableCache,
    utils::release::DropRelease,
    version::FileMetaData,
};

pub fn build_table<E: Env>(
    db_name: &str,
    env: E,
    options: &Arc<Options>,
    table_cache: TableCache<E>,
    mut iter: Box<dyn DBIterator>,
    meta: &mut FileMetaData,
) -> Result<()> {
    meta.file_size = 0;
    iter.seek_to_first();
    let file_name = table_file_name(db_name, meta.number);
    let release_drop = DropRelease::new(|| {
        let _ = env.delete_file(&file_name);
    });

    if iter.valid() {
        let file = env.new_writable_file(&file_name)?;
        let mut builder = TableBuiler::new(options.clone(), file);
        let mut smallest = InternalKey::empty();

        smallest.decode(iter.key());
        meta.smallest = smallest;
        meta.largest = InternalKey::empty();

        let mut next: &[u8] = &[];

        while iter.valid() {
            next = iter.value();
            builder.add(iter.key(), iter.value()).unwrap();
        }
        meta.largest.decode(next);

        meta.file_size = builder.finish(true)?;

        // verify file
        let table = table_cache.find_table(meta.number, meta.file_size)?;
        let mut iter = Table::iter(table, ReadOption::default());
        iter.status()?;
    }

    // if write success ,not release file
    if meta.file_size > 0 {
        forget(release_drop);
    }

    Ok(())
}
