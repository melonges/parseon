//! Root domain types shared across core and adapters.
//!
//! This module owns the primitive identity and decoded-payload shapes that flow
//! through every Parseon port: chains, monitors, targets, cursors, source blocks
//! and logs, and decoded calls and events. Adapters construct and consume these
//! types; core never depends on an adapter's concrete representation.

use std::fmt;
use std::num::NonZeroU64;

pub use alloy::primitives::{Address, B256, BlockNumber, Bytes, ChainId, Selector, TxHash};
use alloy::primitives::{I256, U256};
pub use url::Url;

use crate::abi::AbiParam;

/// A registered EVM chain indexed by Parseon.
///
/// A chain is identified solely by its EIP-155 chain ID. Endpoint URLs and
/// enable state live on [`crate::ports::ChainRecord`] and
/// [`crate::ports::RegisteredChain`], not here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Chain {
    /// EIP-155 chain ID.
    pub id: ChainId,
}

impl Chain {
    /// Creates a chain handle from its EIP-155 chain ID.
    pub const fn new(id: ChainId) -> Self {
        Self { id }
    }
}

/// Surrogate identifier for a monitor.
///
/// Wraps a non-zero `u64` so that the database `BIGSERIAL id` used for REST
/// endpoints (`/monitors/{id}`) and per-monitor result-table names can never be
/// zero. Construct with [`MonitorId::new`]; fallible conversions return
/// [`InvalidMonitorId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MonitorId(NonZeroU64);

/// Error returned when a monitor id of zero is parsed or constructed.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("monitor id must be positive")]
pub struct InvalidMonitorId;

impl MonitorId {
    /// Creates a monitor id from a raw `u64`, rejecting zero.
    pub fn new(id: u64) -> Result<Self, InvalidMonitorId> {
        NonZeroU64::new(id).map(Self).ok_or(InvalidMonitorId)
    }

    /// Returns the underlying `u64` value.
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for MonitorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Per-monitor indexing progress: the last successfully committed block, if any.
///
/// `Cursor(None)` means the monitor has not indexed any block yet; the next
/// block to process is the monitor's `start_block`. `Cursor(Some(n))` means the
/// monitor has committed through block `n` and the next block is `n + 1`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Cursor(pub Option<BlockNumber>);

impl Cursor {
    /// Returns the next block this cursor should process, given a fallback
    /// `start_block` used when the cursor is empty.
    pub fn next(self, start_block: BlockNumber) -> BlockNumber {
        self.0.map_or(start_block, |block| block.saturating_add(1))
    }
}

/// What a monitor matches: either a contract call or an event log.
#[derive(Debug, Clone)]
pub enum Target {
    /// A function call on a specific contract.
    Call(CallTarget),
    /// An event emitted by a specific contract.
    Event(EventTarget),
}

/// A function-call target: address, four-byte selector, and decoded inputs.
#[derive(Debug, Clone)]
pub struct CallTarget {
    /// Contract address that receives the call.
    pub address: Address,
    /// Four-byte function selector.
    pub selector: Selector,
    /// ABI parameter schema for the call's inputs.
    pub inputs: Vec<AbiParam>,
}

/// An event target: emitter address, topic0 signature hash, and params.
#[derive(Debug, Clone)]
pub struct EventTarget {
    /// Contract address that emits the event.
    pub address: Address,
    /// 32-byte event signature hash (keccak of the canonical event signature).
    pub topic0: B256,
    /// ABI parameter schema for the event's indexed and non-indexed params.
    pub params: Vec<AbiParam>,
}

/// A single decoded ABI parameter value.
///
/// Composite types are rejected at decode time; only the scalar kinds listed
/// here are supported. `Bytes` holds cheaply-clonable [`Bytes`] (an
/// `Arc<[u8]>`), so values can be passed through the indexing pipeline without
/// per-stage copies.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodedValue {
    /// An unsigned integer of any width up to 256 bits.
    Uint(U256),
    /// A signed integer of any width up to 256 bits.
    Int(I256),
    /// A boolean.
    Bool(bool),
    /// A 20-byte address.
    Address(Address),
    /// A UTF-8 string.
    String(String),
    /// A dynamic or fixed-size byte array.
    Bytes(Bytes),
}

/// One transaction in a fetched block, with the data the indexer needs.
#[derive(Debug, Clone)]
pub struct BlockTransaction {
    /// Transaction hash.
    pub hash: B256,
    /// Sender address.
    pub from: Address,
    /// Recipient address (zero for contract-creation transactions).
    pub to: Address,
    /// Raw calldata, including the four-byte selector.
    pub input: Bytes,
}

/// A fetched finalized block and its transactions.
#[derive(Debug, Clone)]
pub struct SourceBlock {
    /// Block number.
    pub number: BlockNumber,
    /// Transactions in this block, in on-chain order.
    pub transactions: Vec<BlockTransaction>,
}

/// The execution outcome of one transaction, used to filter reverted calls.
#[derive(Debug, Clone)]
pub struct ExecutionOutcome {
    /// Transaction hash this outcome applies to.
    pub transaction_hash: TxHash,
    /// Whether the transaction succeeded (did not revert).
    pub succeeded: bool,
}

/// A decoded call for one monitor at one block.
#[derive(Debug, Clone)]
pub struct DecodedCall {
    /// Monitor that produced this decoded call.
    pub monitor_id: MonitorId,
    /// Block the call was included in.
    pub block_number: BlockNumber,
    /// Transaction hash of the call.
    pub transaction_hash: TxHash,
    /// Sender address.
    pub from: Address,
    /// Recipient address.
    pub to: Address,
    /// Decoded ABI parameter values, in ABI order.
    pub params: Vec<DecodedValue>,
}

/// A raw EVM log fetched from a block source.
#[derive(Debug, Clone)]
pub struct SourceLog {
    /// Block number the log was emitted in, if the source provides it.
    pub block_number: Option<BlockNumber>,
    /// Transaction hash that emitted the log, if the source provides it.
    pub transaction_hash: Option<B256>,
    /// Log index within the block, if the source provides it.
    pub log_index: Option<u64>,
    /// Emitter address.
    pub address: Address,
    /// Log topics, in order. `topics[0]` is the event signature hash for
    /// non-anonymous events.
    pub topics: Vec<B256>,
    /// Non-indexed event data.
    pub data: Bytes,
    /// Whether the log was removed by a reorg. Finalized sources must never
    /// return removed logs.
    pub removed: bool,
}

/// A decoded event for one monitor at one block.
#[derive(Debug, Clone)]
pub struct DecodedEvent {
    /// Monitor that produced this decoded event.
    pub monitor_id: MonitorId,
    /// Block the event was emitted in.
    pub block_number: BlockNumber,
    /// Transaction hash that emitted the log.
    pub transaction_hash: B256,
    /// Log index within the block.
    pub log_index: u64,
    /// Decoded ABI parameter values, in ABI order (indexed and non-indexed
    /// interleaved per the event definition).
    pub params: Vec<DecodedValue>,
}

/// A decoded result for one monitor: either a call or an event.
#[derive(Debug, Clone)]
pub enum DecodedResult {
    /// A decoded call.
    Call(DecodedCall),
    /// A decoded event.
    Event(DecodedEvent),
}
