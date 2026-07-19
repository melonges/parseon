//! Adapter contracts for chain data, storage, caching, telemetry, and sinks.
//!
//! The ports in this directory are the stable boundaries implemented by
//! Parseon's adapters:
//!
//! - [`storage`]: repository and atomic-block-commit traits (`Storage`,
//!   `IndexStorage`, `ChainRepository`, `MonitorRepository`,
//!   `ResultRepository`) and the records they exchange.
//! - [`source`]: finalized EVM data access (`BlockSource`,
//!   `BlockSourceFactory`) plus the bounded log-query primitives
//!   (`BlockRange`, `LogTarget`, `LogQuery`).
//! - [`cache`]: short-lived block caching (`BlockCache`, `BlockCacheFactory`).
//! - [`telemetry`]: in-process observability (`Telemetry`, `NoopTelemetry`).
//! - [`sink`]: optional post-commit delivery (`Sink`, `SinkBatch`,
//!   `SinkResult`, `NoopSink`).
//!
//! Core never depends on a concrete adapter; adapters implement these traits.

pub mod cache;
pub mod sink;
pub mod source;
pub mod storage;
pub mod telemetry;

pub use cache::{BlockCache, BlockCacheFactory};
pub use sink::{NoopSink, Sink, SinkBatch, SinkResult};
pub use source::{
    BlockRange, BlockSource, BlockSourceFactory, BlockSourceRequestError, InFlightGuard, LogQuery,
    LogTarget,
};
pub use storage::{
    BlockCommit, ChainRecord, ChainRepository, ChainUpdate, IndexStorage, MonitorKind,
    MonitorRecord, MonitorRepository, NewChain, NewMonitor, RegisteredChain, ResultRecord,
    ResultRepository, canonical_params,
};
pub use telemetry::{NoopTelemetry, Telemetry};

/// Composite storage port: a single backend that implements all four
/// repository roles.
///
/// Adapters implement the four sub-traits; the blanket implementation below
/// composes them into a [`Storage`] automatically. The server depends on this
/// composite trait so it can hold a single `Arc<dyn Storage>` and reach every
/// repository role through it.
pub trait Storage:
    IndexStorage + ChainRepository + MonitorRepository + ResultRepository + Send + Sync
{
}

impl<T> Storage for T where
    T: IndexStorage + ChainRepository + MonitorRepository + ResultRepository + Send + Sync
{
}
