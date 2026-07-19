//! Read-optimized projections of [`crate::ports`] records for API responses.
//!
//! The server's DTO layer converts these views into JSON response bodies.
//! Views deliberately omit write-only fields (like RPC URLs) and expose only
//! what the API contract requires.

use chrono::{DateTime, Utc};

use crate::abi::AbiParam;
use crate::filter::FilterExpression;
use crate::ports::{ChainRecord, MonitorKind, MonitorRecord, ResultRecord};
use crate::{Address, B256, BlockNumber, ChainId, MonitorId, Selector, Target, TxHash};

/// API view of a registered chain. Omits the RPC URL, which is write-only.
#[derive(Debug, Clone)]
pub struct ChainView {
    /// EIP-155 chain ID.
    pub chain_id: ChainId,
    /// Whether the chain runs a worker.
    pub enabled: bool,
    /// When the record was first created.
    pub created_at: DateTime<Utc>,
    /// When the record was last updated.
    pub updated_at: DateTime<Utc>,
}

impl From<ChainRecord> for ChainView {
    fn from(record: ChainRecord) -> Self {
        Self {
            chain_id: record.chain.id,
            enabled: record.enabled,
            created_at: record.created_at,
            updated_at: record.updated_at,
        }
    }
}

/// API view of a monitor: the full immutable definition plus operational
/// state (cursor, completed, enabled).
#[derive(Debug, Clone)]
pub struct MonitorView {
    /// Surrogate monitor identifier.
    pub id: MonitorId,
    /// Owning chain ID.
    pub chain_id: ChainId,
    /// Contract address.
    pub address: Address,
    /// Whether the monitor targets a call or an event.
    pub kind: MonitorKind,
    /// Function selector (call monitors only).
    pub selector: Option<Selector>,
    /// Event topic0 (event monitors only).
    pub topic0: Option<B256>,
    /// ABI parameter schema.
    pub param_schema: Vec<AbiParam>,
    /// First block to index (inclusive).
    pub start_block: BlockNumber,
    /// Last block to index (inclusive), or `None` for live indexing.
    pub end_block: Option<BlockNumber>,
    /// Last committed block, or `None` if no block has been committed yet.
    pub cursor: Option<BlockNumber>,
    /// Whether the monitor has reached its `end_block` and stopped.
    pub completed: bool,
    /// Whether the worker should index this monitor.
    pub enabled: bool,
    /// Canonical filter expression, if any.
    pub filter: Option<FilterExpression>,
    /// When the record was first created.
    pub created_at: DateTime<Utc>,
    /// When the record was last updated.
    pub updated_at: DateTime<Utc>,
}

impl From<MonitorRecord> for MonitorView {
    fn from(record: MonitorRecord) -> Self {
        let filter = record.filter.map(|filter| filter.expression);
        let (address, kind, selector, topic0, param_schema) = match record.target {
            Target::Call(target) => {
                (target.address, MonitorKind::Call, Some(target.selector), None, target.inputs)
            }
            Target::Event(target) => {
                (target.address, MonitorKind::Event, None, Some(target.topic0), target.params)
            }
        };
        Self {
            id: record.id,
            chain_id: record.chain.id,
            address,
            kind,
            selector,
            topic0,
            param_schema,
            start_block: record.start_block,
            end_block: record.end_block,
            cursor: record.cursor,
            completed: record.completed,
            enabled: record.enabled,
            filter,
            created_at: record.created_at,
            updated_at: record.updated_at,
        }
    }
}

/// API view of one persisted decoded result.
#[derive(Debug, Clone)]
pub enum MonitorResultView {
    /// A decoded call result.
    Call {
        /// Transaction hash.
        tx_hash: TxHash,
        /// Block the call was included in.
        block_number: BlockNumber,
        /// Canonical JSON-encoded parameters.
        params: serde_json::Value,
    },
    /// A decoded event result.
    Event {
        /// Transaction hash that emitted the log.
        tx_hash: TxHash,
        /// Log index within the block.
        log_index: u64,
        /// Block the event was emitted in.
        block_number: BlockNumber,
        /// Canonical JSON-encoded parameters.
        params: serde_json::Value,
    },
}

impl From<ResultRecord> for MonitorResultView {
    fn from(record: ResultRecord) -> Self {
        match record {
            ResultRecord::Call { tx_hash, block_number, params } => {
                Self::Call { tx_hash, block_number, params }
            }
            ResultRecord::Event { tx_hash, log_index, block_number, params } => {
                Self::Event { tx_hash, log_index, block_number, params }
            }
        }
    }
}
