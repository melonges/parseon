use std::num::NonZeroUsize;
use std::sync::Arc;

use moka::sync::Cache;

use parseon_core::ports::{BlockCache, BlockCacheFactory};
use parseon_core::{BlockNumber, Chain, ChainId, SourceBlock};

pub struct MemoryBlockCache {
    inner: Cache<(ChainId, BlockNumber), Arc<SourceBlock>>,
}

impl MemoryBlockCache {
    pub fn new(capacity: NonZeroUsize) -> Self {
        let max_capacity = u64::try_from(capacity.get()).unwrap_or(u64::MAX);
        Self {
            inner: Cache::builder()
                .max_capacity(max_capacity)
                .support_invalidation_closures()
                .build(),
        }
    }
}

impl BlockCache for MemoryBlockCache {
    fn get(&self, chain: Chain, block_number: BlockNumber) -> Option<Arc<SourceBlock>> {
        self.inner.get(&(chain.id, block_number))
    }

    fn put(&self, chain: Chain, block: Arc<SourceBlock>) {
        self.inner.insert((chain.id, block.number), block);
    }

    fn evict_before(&self, chain: Chain, block_number: BlockNumber) {
        self.inner
            .invalidate_entries_if(move |key, _| key.0 == chain.id && key.1 < block_number)
            .expect("invalidation closures are enabled for the block cache");
    }

    fn evict_after(&self, chain: Chain, block_number: BlockNumber) {
        self.inner
            .invalidate_entries_if(move |key, _| key.0 == chain.id && key.1 > block_number)
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
        match NonZeroUsize::new(self.capacity) {
            Some(capacity) => Arc::new(MemoryBlockCache::new(capacity)),
            None => Arc::new(DisabledBlockCache),
        }
    }
}

struct DisabledBlockCache;

impl BlockCache for DisabledBlockCache {
    fn get(&self, _chain: Chain, _block_number: BlockNumber) -> Option<Arc<SourceBlock>> {
        None
    }

    fn put(&self, _chain: Chain, _block: Arc<SourceBlock>) {}

    fn evict_before(&self, _chain: Chain, _block_number: BlockNumber) {}

    fn evict_after(&self, _chain: Chain, _block_number: BlockNumber) {}
}

#[cfg(test)]
mod tests {
    use std::thread;

    use parseon_core::{B256, BlockMetadata};

    use super::*;

    fn block(number: BlockNumber) -> Arc<SourceBlock> {
        Arc::new(SourceBlock {
            number,
            metadata: BlockMetadata {
                number,
                hash: B256::from([number as u8; 32]),
                parent_hash: B256::ZERO,
                timestamp: 0,
            },
            transactions: Vec::new(),
        })
    }

    #[test]
    fn returns_the_cached_block_allocation() {
        let cache = MemoryBlockCache::new(NonZeroUsize::new(1).unwrap());
        let chain = Chain::new(1);
        let block = block(10);

        cache.put(chain, block.clone());
        let cached = cache.get(chain, block.number).unwrap();

        assert!(Arc::ptr_eq(&block, &cached));
    }

    #[test]
    fn keys_and_evicts_by_chain() {
        let cache = MemoryBlockCache::new(NonZeroUsize::new(4).unwrap());
        let second_chain = Chain::new(2);
        let mainnet = Chain::new(1);
        cache.put(second_chain, block(10));
        cache.put(second_chain, block(11));
        cache.put(mainnet, block(10));
        cache.evict_before(second_chain, 11);
        cache.evict_after(second_chain, 11);
        assert!(cache.get(second_chain, 10).is_none());
        assert!(cache.get(second_chain, 11).is_some());
        assert!(cache.get(mainnet, 10).is_some());
    }

    #[test]
    fn accepts_parallel_writes() {
        const THREADS: u64 = 8;
        const BLOCKS_PER_THREAD: u64 = 32;

        let cache = Arc::new(MemoryBlockCache::new(
            NonZeroUsize::new(usize::try_from(THREADS * BLOCKS_PER_THREAD).unwrap()).unwrap(),
        ));
        let chain = Chain::new(1);
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

    #[test]
    fn zero_capacity_disables_caching() {
        let cache = MemoryBlockCacheFactory::new(0).create();
        let chain = Chain::new(1);
        cache.put(chain, block(10));
        assert!(cache.get(chain, 10).is_none());
        cache.evict_before(chain, 11);
        cache.evict_after(chain, 11);
    }
}
