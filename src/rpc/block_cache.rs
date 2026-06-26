use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Mutex;

use crate::rpc::fetch::MatchedTx;
use alloy::primitives::B256;

/// A simple per-chain block cache keyed by block number.
///
/// The coordinator fetches each block once and fans it out to all monitors
/// whose range covers it. Entries are evicted once all monitors have advanced
/// past them (via `evict_below`).
pub struct BlockCache {
    inner: Mutex<LruCache<i64, CachedBlock>>,
}

#[derive(Clone)]
pub struct CachedBlock {
    pub block_hash: B256,
    pub txs: Vec<MatchedTx>,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).unwrap();
        Self {
            inner: Mutex::new(LruCache::new(cap)),
        }
    }

    pub fn get(&self, block_number: i64) -> Option<CachedBlock> {
        self.inner
            .lock()
            .ok()
            .and_then(|mut c| c.get(&block_number).cloned())
    }

    pub fn put(&self, block_number: i64, block: CachedBlock) {
        if let Ok(mut c) = self.inner.lock() {
            c.put(block_number, block);
        }
    }

    /// Evict all entries with block number strictly less than `threshold`.
    pub fn evict_below(&self, threshold: i64) {
        if let Ok(mut c) = self.inner.lock() {
            let keys: Vec<i64> = c.iter().map(|(k, _)| *k).collect();
            for k in keys {
                if k < threshold {
                    c.pop(&k);
                }
            }
        }
    }
}
