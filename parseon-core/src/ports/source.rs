//! Canonical EVM data access ports.
//!
//! [`BlockSource] is the read-side adapter contract implemented by
//! `parseon-rpc`. The worker uses it to fetch canonical blocks, transaction
//! execution outcomes, and exact-target event logs.

use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    BlockMetadata, BlockNumber, ChainId, ExecutionOutcome, SourceBlock, SourceLog, TxHash, Url,
};
use alloy::primitives::{Address, B256};

/// Factory that constructs a connected [`BlockSource`] for a given RPC URL.
///
/// The supervisor and application services use this to obtain a block source
/// for chain validation and worker startup.
pub trait BlockSourceFactory: Send + Sync {
    /// Returns a connected block source for `rpc_url`, or an error if the URL
    /// is unusable (e.g. wrong scheme, unreachable host). Source validation
    /// happens later via [`BlockSource::chain_id`] and
    /// [`BlockSource::finalized_head`].
    fn connect(&self, rpc_url: &Url) -> anyhow::Result<Arc<dyn BlockSource>>;
}

/// Error returned by block-source requests when the underlying transport or
/// endpoint fails.
///
/// The status layer uses this marker type to downcast a chained `anyhow::Error`
/// and replace the message with a generic "block source request failed" so
/// that private endpoint URLs never leak through error messages.
#[derive(Debug, thiserror::Error)]
#[error("block source request failed")]
pub struct BlockSourceRequestError {
    #[source]
    source: anyhow::Error,
}

impl BlockSourceRequestError {
    /// Wraps an arbitrary backend error as a block-source request error.
    pub fn new(source: anyhow::Error) -> Self {
        Self { source }
    }
}

/// Canonical EVM data required by the indexing application.
///
/// Implementations must return the exact requested block, complete full
/// transaction data, and one execution outcome per requested transaction hash
/// in the same order. Log results must cover only the inclusive query range
/// and exact [`LogTarget`] pairs; result order is not significant.
#[async_trait]
pub trait BlockSource: Send + Sync {
    /// Returns the endpoint's EIP-155 chain ID.
    async fn chain_id(&self) -> anyhow::Result<u64>;
    /// Returns the current latest head block number. Sources that only expose
    /// finalized data may use the default finalized value.
    async fn latest_head(&self) -> anyhow::Result<BlockNumber> {
        self.finalized_head().await
    }
    /// Returns the current finalized head block number. Must succeed only if
    /// the endpoint supports the `finalized` block tag.
    async fn finalized_head(&self) -> anyhow::Result<BlockNumber>;
    /// Fetches canonical metadata for a block without requiring full transactions.
    async fn fetch_block_header(
        &self,
        _block_number: BlockNumber,
    ) -> anyhow::Result<BlockMetadata> {
        anyhow::bail!("block header fetching is not implemented")
    }
    /// Fetches the full block at `block_number`, including every transaction's
    /// sender, recipient, and calldata.
    async fn fetch_block(&self, block_number: BlockNumber) -> anyhow::Result<SourceBlock>;
    /// Fetches execution outcomes for `transaction_hashes` in the same order
    /// as the input slice. Each receipt must belong to `block`; used to skip
    /// reverted calls.
    async fn fetch_execution_outcomes(
        &self,
        block: &BlockMetadata,
        transaction_hashes: &[TxHash],
    ) -> anyhow::Result<Vec<ExecutionOutcome>>;
    /// Fetches logs for an exact set of emitter-address and event-signature
    /// pairs over an inclusive block range. The default implementation bails;
    /// sources that support `eth_getLogs` should override it.
    async fn fetch_logs(&self, _query: LogQuery) -> anyhow::Result<Vec<SourceLog>> {
        anyhow::bail!("log fetching is not implemented")
    }
    /// Rotates the endpoint URL of an already-connected source in place,
    /// keeping cached state such as the chain ID. The default implementation
    /// bails; sources that support in-place rotation should override it.
    /// Callers must guarantee the new URL serves the same chain.
    fn set_rpc_url(&self, _rpc_url: &Url) -> anyhow::Result<()> {
        anyhow::bail!("RPC URL rotation is not supported by this block source")
    }
}

/// An inclusive, non-empty range of canonical EVM block numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRange {
    start: BlockNumber,
    end: BlockNumber,
}

impl BlockRange {
    /// Creates a range `[start, end]`. Returns `None` if `start > end`.
    pub const fn new(start: BlockNumber, end: BlockNumber) -> Option<Self> {
        if start <= end { Some(Self { start, end }) } else { None }
    }

    /// Creates a single-block range `[block_number, block_number]`.
    pub const fn single(block_number: BlockNumber) -> Self {
        Self { start: block_number, end: block_number }
    }

    /// Range start (inclusive).
    pub const fn start(self) -> BlockNumber {
        self.start
    }

    /// Range end (inclusive).
    pub const fn end(self) -> BlockNumber {
        self.end
    }
}

/// An exact emitter-address and event-signature pair for an EVM log query.
///
/// The worker builds a list of `LogTarget`s from the monitors covering a
/// window of blocks and asks the source for exactly those pairs. The source
/// must not return logs for any other (address, topic0) pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogTarget {
    address: Address,
    topic0: B256,
}

impl LogTarget {
    /// Creates a log target.
    pub const fn new(address: Address, topic0: B256) -> Self {
        Self { address, topic0 }
    }

    /// Emitter address.
    pub const fn address(self) -> Address {
        self.address
    }

    /// Event signature hash.
    pub const fn topic0(self) -> B256 {
        self.topic0
    }
}

/// A finalized log request that preserves exact monitor target pairs.
///
/// `LogQuery` is constructed with a list of [`LogTarget`]s which the worker
/// deduplicates and sorts so that sources can hash the request deterministically.
#[derive(Debug, Clone)]
pub struct LogQuery {
    range: BlockRange,
    targets: Vec<LogTarget>,
}

impl LogQuery {
    /// Creates a log query, sorting and deduplicating `targets`.
    pub fn new(range: BlockRange, mut targets: Vec<LogTarget>) -> Self {
        targets.sort_unstable();
        targets.dedup();
        Self { range, targets }
    }

    /// Inclusive block range to query.
    pub const fn range(&self) -> BlockRange {
        self.range
    }

    /// Exact (address, topic0) pairs to query.
    pub fn targets(&self) -> &[LogTarget] {
        &self.targets
    }

    /// Whether the query has any targets.
    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    /// Decomposes the query into its range and targets.
    pub fn into_parts(self) -> (BlockRange, Vec<LogTarget>) {
        (self.range, self.targets)
    }
}

/// RAII guard that increments (on construction) and decrements (on drop) the
/// in-flight counter for a telemetry stage.
///
/// Used by the worker and the RPC adapter to track how many operations are
/// simultaneously in each pipeline stage (`block`, `storage`, `rpc`, …).
pub struct InFlightGuard<'a> {
    telemetry: &'a dyn crate::ports::Telemetry,
    chain_id: ChainId,
    stage: &'static str,
}

impl<'a> InFlightGuard<'a> {
    /// Creates a guard that records one in-flight operation for `stage` on
    /// `chain_id`.
    pub fn new(
        telemetry: &'a dyn crate::ports::Telemetry,
        chain_id: ChainId,
        stage: &'static str,
    ) -> Self {
        telemetry.adjust_in_flight(chain_id, stage, 1);
        Self { telemetry, chain_id, stage }
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.telemetry.adjust_in_flight(self.chain_id, self.stage, -1);
    }
}
