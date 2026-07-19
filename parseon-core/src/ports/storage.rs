//! Storage ports: monitor and chain repositories, atomic block commits, and
//! decoded-result queries.
//!
//! These traits are implemented by `parseon-postgres` and `parseon-mongodb`.
//! The server composes them through the [`Storage`](crate::ports::Storage)
//! blanket implementation in [`crate::ports`].

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::abi::AbiParam;
use crate::filter::FilterDefinition;
use crate::monitor::Monitor;
use crate::{BlockNumber, Chain, DecodedResult, DecodedValue, MonitorId, Target, TxHash, Url};

/// One block's atomic commit payload: decoded results and the monitors whose
/// cursors must advance together.
///
/// Implementations of [`IndexStorage::commit_block`] must persist every result
/// and update every covering monitor's cursor in a single transaction so that
/// crashes between writes cannot leave gaps or duplicates.
#[derive(Debug, Clone)]
pub struct BlockCommit {
    /// Chain the block belongs to.
    pub chain: Chain,
    /// Block number being committed.
    pub block_number: BlockNumber,
    /// Monitors whose cursors must advance to this block. The worker passes
    /// only the monitors whose plan covered this block.
    pub monitors: Vec<Arc<Monitor>>,
    /// Decoded results for this block, in any order.
    pub results: Vec<DecodedResult>,
}

/// Per-chain indexing storage: monitor loading and atomic block commits.
#[async_trait]
pub trait IndexStorage: Send + Sync {
    /// Loads every monitor belonging to `chain`, regardless of `enabled` or
    /// `completed` state. The worker filters and indexes them per poll.
    async fn load_monitors(&self, chain: Chain) -> anyhow::Result<Vec<Monitor>>;

    /// Atomically commits `commit`'s results and advances every covering
    /// monitor's cursor to `commit.block_number`.
    async fn commit_block(&self, commit: &BlockCommit) -> anyhow::Result<()>;
}

/// A chain known to Parseon at process startup, with its RPC URL and enabled
/// state. The supervisor reads this list once and starts one worker per
/// enabled chain.
#[derive(Clone, PartialEq, Eq)]
pub struct RegisteredChain {
    /// Chain identity.
    pub chain: Chain,
    /// Write-only RPC URL. Persisted for workers; never returned or logged by
    /// the HTTP API.
    pub rpc_url: Url,
    /// Whether the supervisor should run a worker for this chain. Disabled
    /// chains still appear in the registry but get no worker.
    pub enabled: bool,
}

/// Chain registry port: CRUD over registered chains.
#[async_trait]
pub trait ChainRepository: Send + Sync {
    /// Lists every chain known to the supervisor at startup.
    async fn list_registered_chains(&self) -> anyhow::Result<Vec<RegisteredChain>>;
    /// Creates a new chain registration.
    async fn create_chain(&self, chain: NewChain) -> anyhow::Result<ChainRecord>;
    /// Lists every chain record (including disabled ones).
    async fn list_chains(&self) -> anyhow::Result<Vec<ChainRecord>>;
    /// Returns one chain record by identity.
    async fn get_chain(&self, chain: Chain) -> anyhow::Result<ChainRecord>;
    /// Updates a chain's RPC URL and/or enabled state.
    async fn update_chain(&self, chain: Chain, update: ChainUpdate) -> anyhow::Result<ChainRecord>;
    /// Deletes a chain and its dependent data.
    async fn delete_chain(&self, chain: Chain) -> anyhow::Result<()>;
}

/// Command payload for [`ChainRepository::create_chain`].
#[derive(Debug, Clone)]
pub struct NewChain {
    /// Chain identity.
    pub chain: Chain,
    /// Write-only RPC URL for the worker.
    pub rpc_url: Url,
    /// Whether the chain should start a worker on next startup.
    pub enabled: bool,
}

/// Command payload for [`ChainRepository::update_chain`]. Either field may be
/// `None` to leave it unchanged.
#[derive(Debug, Clone)]
pub struct ChainUpdate {
    /// New RPC URL, if changing.
    pub rpc_url: Option<Url>,
    /// New enabled state, if changing.
    pub enabled: Option<bool>,
}

/// Persisted chain record returned by the chain repository.
#[derive(Debug, Clone)]
pub struct ChainRecord {
    /// Chain identity.
    pub chain: Chain,
    /// Write-only RPC URL. Persisted for workers; never returned by the HTTP
    /// API.
    pub rpc_url: Url,
    /// Whether the chain runs a worker on startup.
    pub enabled: bool,
    /// When the record was first created.
    pub created_at: DateTime<Utc>,
    /// When the record was last updated.
    pub updated_at: DateTime<Utc>,
}

/// Whether a monitor targets a function call or an event log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonitorKind {
    /// A function call.
    Call,
    /// An event log.
    Event,
}

impl MonitorKind {
    /// Lowercase string used in API responses and persisted records.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Call => "call",
            Self::Event => "event",
        }
    }
}

/// Command payload for [`MonitorRepository::create_monitor`].
#[derive(Debug, Clone)]
pub struct NewMonitor {
    /// Chain the monitor belongs to. Immutable after creation.
    pub chain: Chain,
    /// What the monitor matches. Immutable after creation.
    pub target: Target,
    /// First block to index (inclusive). Immutable after creation.
    pub start_block: BlockNumber,
    /// Last block to index (inclusive), or `None` to index indefinitely.
    /// Immutable after creation.
    pub end_block: Option<BlockNumber>,
    /// Optional compiled filter. Immutable after creation.
    pub filter: Option<FilterDefinition>,
}

/// Persisted monitor record returned by the monitor repository.
#[derive(Debug, Clone)]
pub struct MonitorRecord {
    /// Surrogate monitor identifier.
    pub id: MonitorId,
    /// Owning chain.
    pub chain: Chain,
    /// What the monitor matches.
    pub target: Target,
    /// First block to index (inclusive).
    pub start_block: BlockNumber,
    /// Last block to index (inclusive), or `None` for live indexing.
    pub end_block: Option<BlockNumber>,
    /// Last committed block, or `None` if no block has been committed yet.
    pub cursor: Option<BlockNumber>,
    /// Whether the monitor has reached its `end_block` and stopped.
    pub completed: bool,
    /// Whether the worker should index this monitor. User-mutable for pause
    /// and resume.
    pub enabled: bool,
    /// Compiled filter, if any.
    pub filter: Option<FilterDefinition>,
    /// When the record was first created.
    pub created_at: DateTime<Utc>,
    /// When the record was last updated.
    pub updated_at: DateTime<Utc>,
}

/// Persisted decoded result returned by [`ResultRepository::query_results`].
#[derive(Debug, Clone)]
pub enum ResultRecord {
    /// A decoded call.
    Call {
        /// Transaction hash.
        tx_hash: TxHash,
        /// Block the call was included in.
        block_number: BlockNumber,
        /// Canonical JSON encoding of the decoded parameters.
        params: serde_json::Value,
    },
    /// A decoded event.
    Event {
        /// Transaction hash that emitted the log.
        tx_hash: TxHash,
        /// Log index within the block.
        log_index: u64,
        /// Block the event was emitted in.
        block_number: BlockNumber,
        /// Canonical JSON encoding of the decoded parameters.
        params: serde_json::Value,
    },
}

/// Monitor registry port: CRUD over monitors.
#[async_trait]
pub trait MonitorRepository: Send + Sync {
    /// Counts every persisted monitor across all chains.
    async fn count_monitors(&self) -> anyhow::Result<usize>;
    /// Creates a new monitor.
    async fn create_monitor(&self, monitor: NewMonitor) -> anyhow::Result<MonitorRecord>;
    /// Lists monitors, optionally filtered to one chain.
    async fn list_monitors(&self, chain: Option<Chain>) -> anyhow::Result<Vec<MonitorRecord>>;
    /// Returns one monitor by id.
    async fn get_monitor(&self, id: MonitorId) -> anyhow::Result<MonitorRecord>;
    /// Pauses (`enabled = false`) or resumes (`enabled = true`) a monitor.
    /// Cursor progress and existing results are preserved.
    async fn set_monitor_enabled(
        &self,
        id: MonitorId,
        enabled: bool,
    ) -> anyhow::Result<MonitorRecord>;
    /// Deletes a monitor and its dependent data.
    async fn delete_monitor(&self, id: MonitorId) -> anyhow::Result<()>;
}

/// Result query port: reads persisted decoded results.
#[async_trait]
pub trait ResultRepository: Send + Sync {
    /// Queries a page of decoded results for `monitor`.
    async fn query_results(
        &self,
        monitor: &MonitorRecord,
        query: crate::commands::ResultQuery,
    ) -> anyhow::Result<Vec<ResultRecord>>;
}

/// Canonical JSON encoding of a decoded parameter tuple.
///
/// Each parameter name is paired with its decoded value, encoded as:
/// - `Uint`/`Int` → decimal string (to preserve full 256-bit precision).
/// - `Bool` → JSON boolean.
/// - `Address` → 0x-prefixed lowercase checksummed hex string.
/// - `String` → JSON string.
/// - `Bytes` → 0x-prefixed lowercase hex string.
///
/// Returns an error if `schema` and `values` differ in length.
pub fn canonical_params(
    schema: &[AbiParam],
    values: &[DecodedValue],
) -> anyhow::Result<serde_json::Value> {
    anyhow::ensure!(schema.len() == values.len(), "parameter count mismatch");
    Ok(serde_json::Value::Object(
        schema
            .iter()
            .zip(values)
            .map(|(param, value)| {
                let value = match value {
                    DecodedValue::Uint(value) => serde_json::Value::String(value.to_string()),
                    DecodedValue::Int(value) => serde_json::Value::String(value.to_string()),
                    DecodedValue::Bool(value) => serde_json::Value::Bool(*value),
                    DecodedValue::Address(value) => {
                        serde_json::Value::String(format!("{value:#x}"))
                    }
                    DecodedValue::String(value) => serde_json::Value::String(value.clone()),
                    DecodedValue::Bytes(value) => {
                        serde_json::Value::String(format!("0x{}", alloy::hex::encode(value)))
                    }
                };
                (param.name.clone(), value)
            })
            .collect(),
    ))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, I256, U256};

    use super::*;
    use crate::abi::parse_abi_type;

    #[test]
    fn canonically_encodes_every_scalar_parameter_kind() {
        let param = |name, ty| AbiParam::new(name, parse_abi_type(ty).unwrap()).unwrap();
        let schema = [
            param("uint", "uint256"),
            param("int", "int256"),
            param("flag", "bool"),
            param("owner", "address"),
            param("label", "string"),
            param("data", "bytes"),
        ];
        let values = [
            DecodedValue::Uint(U256::from(42)),
            DecodedValue::Int(I256::try_from(-7).unwrap()),
            DecodedValue::Bool(true),
            DecodedValue::Address(Address::repeat_byte(1)),
            DecodedValue::String("hello".into()),
            DecodedValue::Bytes(vec![0xde, 0xad].into()),
        ];
        assert_eq!(
            canonical_params(&schema, &values).unwrap(),
            serde_json::json!({
                "uint": "42",
                "int": "-7",
                "flag": true,
                "owner": format!("{:#x}", Address::repeat_byte(1)),
                "label": "hello",
                "data": "0xdead"
            })
        );
        assert!(canonical_params(&schema, &values[..5]).is_err());
    }
}
