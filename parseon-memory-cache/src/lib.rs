use std::sync::Arc;

use moka::sync::Cache;

use parseon_core::ports::{BlockCache, BlockCacheFactory};
use parseon_core::{Chain, SourceBlock};

pub struct MemoryBlockCache {
    inner: Cache<(i64, i64), SourceBlock>,
}

impl MemoryBlockCache {
    pub fn new(capacity: usize) -> Self {
        let max_capacity = u64::try_from(capacity.max(1)).unwrap_or(u64::MAX);
        Self {
            inner: Cache::builder()
                .max_capacity(max_capacity)
                .support_invalidation_closures()
                .build(),
        }
    }
}

impl BlockCache for MemoryBlockCache {
    fn get(&self, chain: Chain, block_number: i64) -> Option<SourceBlock> {
        self.inner.get(&(chain.id, block_number))
    }

    fn put(&self, chain: Chain, block: SourceBlock) {
        self.inner.insert((chain.id, block.number), block);
    }

    fn evict_before(&self, chain: Chain, block_number: i64) {
        self.inner
            .invalidate_entries_if(move |key, _| key.0 == chain.id && key.1 < block_number)
            .expect("invalidation closures are enabled for the block cache");
    }
}

pub struct MemoryBlockCacheFactory {
    capacity: usize,
}

impl MemoryBlockCacheFactory {
    pub fn new(capacity: usize) -> Self {
        Self { capacity }
    }
}

impl BlockCacheFactory for MemoryBlockCacheFactory {
    fn create(&self) -> Arc<dyn BlockCache> {
        Arc::new(MemoryBlockCache::new(self.capacity))
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    fn block(number: i64) -> SourceBlock {
        SourceBlock {
            number,
            transactions: Vec::new(),
        }
    }

    #[test]
    fn keys_and_evicts_by_chain() {
        let cache = MemoryBlockCache::new(4);
        let second_chain = Chain::new(2).unwrap();
        let mainnet = Chain::new(1).unwrap();
        cache.put(second_chain, block(10));
        cache.put(second_chain, block(11));
        cache.put(mainnet, block(10));
        cache.evict_before(second_chain, 11);
        assert!(cache.get(second_chain, 10).is_none());
        assert!(cache.get(second_chain, 11).is_some());
        assert!(cache.get(mainnet, 10).is_some());
    }

    #[test]
    fn stores_and_retrieves_blocks() {
        let cache = MemoryBlockCache::new(1);
        let chain = Chain::new(1).unwrap();
        cache.put(chain, block(10));
        assert_eq!(cache.get(chain, 10).map(|block| block.number), Some(10));
    }

    #[test]
    fn configures_entry_capacity() {
        let cache = MemoryBlockCache::new(2);
        assert_eq!(cache.inner.policy().max_capacity(), Some(2));

        let minimum = MemoryBlockCache::new(0);
        assert_eq!(minimum.inner.policy().max_capacity(), Some(1));
    }

    #[test]
    fn accepts_parallel_writes() {
        const THREADS: i64 = 8;
        const BLOCKS_PER_THREAD: i64 = 32;

        let cache = Arc::new(MemoryBlockCache::new(
            usize::try_from(THREADS * BLOCKS_PER_THREAD).unwrap(),
        ));
        let chain = Chain::new(1).unwrap();
        let handles = (0..THREADS)
            .map(|thread_id| {
                let cache = cache.clone();
                thread::spawn(move || {
                    let start = thread_id * BLOCKS_PER_THREAD;
                    for block_number in start..start + BLOCKS_PER_THREAD {
                        cache.put(chain, block(block_number));
                    }
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle.join().unwrap();
        }
        for block_number in 0..THREADS * BLOCKS_PER_THREAD {
            assert!(cache.get(chain, block_number).is_some());
        }
    }
}
