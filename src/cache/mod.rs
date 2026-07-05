use std::num::NonZeroUsize;
use std::sync::Mutex;

use lru::LruCache;

use crate::core::ports::BlockCache;
use crate::core::{Chain, SourceBlock};

pub struct MemoryBlockCache {
    inner: Mutex<LruCache<(i64, i64), SourceBlock>>,
}

impl MemoryBlockCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(LruCache::new(
                NonZeroUsize::new(capacity.max(1)).expect("cache capacity is positive"),
            )),
        }
    }
}

impl BlockCache for MemoryBlockCache {
    fn get(&self, chain: Chain, block_number: i64) -> Option<SourceBlock> {
        self.inner
            .lock()
            .ok()
            .and_then(|mut cache| cache.get(&(chain.id, block_number)).cloned())
    }

    fn put(&self, chain: Chain, block: SourceBlock) {
        if let Ok(mut cache) = self.inner.lock() {
            cache.put((chain.id, block.number), block);
        }
    }

    fn evict_before(&self, chain: Chain, block_number: i64) {
        if let Ok(mut cache) = self.inner.lock() {
            let keys = cache
                .iter()
                .filter_map(|(key, _)| (key.0 == chain.id && key.1 < block_number).then_some(*key))
                .collect::<Vec<_>>();
            for key in keys {
                cache.pop(&key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
        cache.put(mainnet, block(10));
        cache.evict_before(second_chain, 11);
        assert!(cache.get(second_chain, 10).is_none());
        assert!(cache.get(mainnet, 10).is_some());
    }
}
