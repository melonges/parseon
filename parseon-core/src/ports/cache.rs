//! Block cache ports.
//!
//! [`BlockCache`] is a short-lived, chain-aware cache for fetched blocks.
//! `parseon-memory-cache` provides the in-memory LRU implementation; the
//! worker asks the cache before fetching from the block source.

use std::sync::Arc;

use crate::{BlockNumber, Chain, SourceBlock};

/// Chain-aware cache for fetched [`SourceBlock`]s.
///
/// Keys must be `(chain_id, block_number)` so that two chains with overlapping
/// block numbers cannot collide. Implementations are free to evict entries at
/// any time; the worker always re-fetches on a miss.
pub trait BlockCache: Send + Sync {
    /// Returns the cached block for `(chain, block_number)`, if present.
    fn get(&self, chain: Chain, block_number: BlockNumber) -> Option<Arc<SourceBlock>>;
    /// Stores `block` under `(chain, block.number)`.
    fn put(&self, chain: Chain, block: Arc<SourceBlock>);
    /// Evicts every entry for `chain` whose block number is less than
    /// `block_number`. Called after the worker commits past a block.
    fn evict_before(&self, chain: Chain, block_number: BlockNumber);
}

/// Factory for per-worker [`BlockCache`] instances.
///
/// Each worker gets its own cache so that eviction policies cannot interfere
/// across chains.
pub trait BlockCacheFactory: Send + Sync {
    /// Returns a fresh block cache for one worker.
    fn create(&self) -> Arc<dyn BlockCache>;
}
