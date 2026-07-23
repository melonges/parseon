//! Domain models and application services for finalized EVM indexing.
//!
//! Parseon core owns monitor planning, ABI decoding, filtering, ordered block
//! commits, and the ports implemented by storage, block-source, cache,
//! telemetry, and sink adapters. It does not construct or depend on concrete
//! infrastructure adapters.
//!
//! ## Module map
//!
//! - [`model`]: root domain types (`Chain`, `MonitorId`, `Target`,
//!   `DecodedValue`, `DecodedCall`, `DecodedEvent`, `SourceBlock`, …).
//! - [`abi`]: ABI parameter and decoder types built on top of `alloy`.
//! - [`commands`]: application-layer command and query DTOs.
//! - [`filter`]: the versioned JSON filter DSL, compiler, and evaluator.
//! - [`monitor`]: the immutable monitor definition and its range/cursor helpers.
//! - [`pipeline`]: a tiny ordered concurrency primitive used by the worker.
//! - [`ports`]: trait contracts implemented by adapters (`Storage`,
//!   `BlockSource`, `BlockCache`, `Sink`, `Telemetry`) and the records they
//!   exchange.
//! - [`services`]: application services invoked by HTTP handlers.
//! - [`status`]: in-process worker status snapshots.
//! - [`supervisor`]: the per-chain worker supervisor.
//! - [`views`]: read-optimized projections of [`ports`] records for API
//!   responses.
//!
//! The `indexer`, `scheduler`, and `worker` modules are crate-private indexing
//! internals and are not re-exported.

pub mod abi;
pub mod commands;
pub mod filter;
pub mod monitor;
pub mod pipeline;
pub mod ports;
pub mod services;
pub mod status;
pub mod supervisor;
pub mod views;

mod indexer;
mod scheduler;
#[cfg(test)]
pub(crate) mod testkit;
mod worker;

pub mod model;

pub use model::{
    Address, B256, BlockNumber, BlockTransaction, Bytes, CallTarget, Chain, ChainId, Cursor,
    DecodedCall, DecodedEvent, DecodedResult, DecodedValue, EventTarget, ExecutionOutcome,
    InvalidMonitorId, MonitorId, Selector, SourceBlock, SourceLog, Target, TxHash, Url,
};
