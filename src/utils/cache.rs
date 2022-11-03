use std::{
    collections::hash_map::DefaultHasher,
    hash::Hasher,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
};

use lru::LruCache;
use std::hash::Hash;

const NUM_SHARD_BITS: u32 = 4;
const NUM_SHARDS: u32 = 1 << NUM_SHARD_BITS;

pub trait Cache<K: Sized, V: Sized> {
    fn insert(&self, key: K, value: V, charge: u64) -> Option<Arc<V>>;
    fn lookup(&self, key: &K) -> Option<Arc<V>>;
    fn erase(&self, key: &K);

    fn new_id(&self) -> u64;
    fn total_charge(&self) -> u64;
}

pub struct ShardLruCache<K, V>
where
    K: Eq + Hash,
{
    shards: Box<[Arc<Mutex<LruCacheInner<K, V>>>]>,
    last_id: AtomicU64,
}

impl<K, V> ShardLruCache<K, V>
where
    K: Eq + Hash,
{
    pub fn new(capacity: u64) -> Self {
        let per_shard = (capacity + NUM_SHARDS as u64 - 1) / NUM_SHARDS as u64;
        let mut cache = Vec::with_capacity(NUM_SHARDS as usize);
        for _ in 0..NUM_SHARDS {
            let shard = Arc::new(Mutex::new(LruCacheInner::new(per_shard)));
            cache.push(shard);
        }
        ShardLruCache {
            shards: cache.into_boxed_slice(),
            last_id: AtomicU64::new(0),
        }
    }

    fn shard(key: &K) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let h = hasher.finish();
        h >> (64 - NUM_SHARD_BITS)
    }

    fn get_shard(&self, key: &K) -> MutexGuard<LruCacheInner<K, V>> {
        let shard = Self::shard(&key);
        assert!(shard < NUM_SHARDS as u64);
        self.shards.get(shard as usize).unwrap().lock().unwrap()
    }
}

impl<K, V> Cache<K, V> for ShardLruCache<K, V>
where
    K: Eq + Hash,
{
    fn insert(&self, key: K, value: V, charge: u64) -> Option<Arc<V>> {
        let mut lru = self.get_shard(&key);
        lru.insert(key, value, charge)
    }

    fn lookup(&self, key: &K) -> Option<Arc<V>> {
        let mut lru = self.get_shard(&key);
        lru.lookup(key)
    }

    fn erase(&self, key: &K) {
        let mut lru = self.get_shard(&key);
        lru.erase(key);
    }

    fn new_id(&self) -> u64 {
        self.last_id.fetch_add(1, Ordering::SeqCst)
    }

    fn total_charge(&self) -> u64 {
        let mut total = 0;
        for shard in self.shards.iter() {
            total += shard.lock().unwrap().total_charge();
        }
        total
    }
}

struct LruValue<V> {
    value: Arc<V>,
    charge: u64,
}

struct LruCacheInner<K: Eq + Hash, V> {
    lru: LruCache<K, LruValue<V>>,
    usage: u64,
    capacity: u64,
}

impl<K: Eq + Hash, V> LruCacheInner<K, V> {
    pub fn new(capacity: u64) -> Self {
        let lru = LruCache::unbounded();
        LruCacheInner {
            lru,
            usage: 0,
            capacity,
        }
    }

    pub fn insert(&mut self, key: K, value: V, charge: u64) -> Option<Arc<V>> {
        if self.capacity == 0 {
            return None;
        }

        self.usage += charge;
        while self.usage > self.capacity && !self.lru.is_empty() {
            let (k, evicted_val) = self.lru.pop_lru().unwrap();
            self.usage -= evicted_val.charge;
        }
        let value = Arc::new(value);

        self.lru.put(
            key,
            LruValue {
                value: value.clone(),
                charge,
            },
        );

        Some(value)
    }

    pub fn lookup(&mut self, key: &K) -> Option<Arc<V>> {
        if let Some(h) = self.lru.get(key) {
            Some(h.value.clone())
        } else {
            None
        }
    }

    pub fn erase(&mut self, key: &K) {
        if let Some(v) = self.lru.pop(key) {
            self.usage -= v.charge;
        }
    }
    pub fn clear(&mut self) {
        self.lru.clear();
        self.usage = 0;
    }

    pub fn total_charge(&self) -> u64 {
        self.usage
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_lookup() {
        let cache = ShardLruCache::new(2000);
        cache.insert(43, 200, 1);
        cache.insert(40, 200, 1);

        let ret = cache.lookup(&43);
        assert!(ret.is_some());
        assert_eq!(*ret.unwrap(), 200);
    }
}
