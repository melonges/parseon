use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use parseon_core::status::{ChainStatusSnapshot, WorkerState};
use parseon_core::abi::AbiParam;
use parseon_core::views::{ChainView, MonitorResultView, MonitorView};
use parseon_core::{Address, B256, Selector, TxHash, Url};

// ----- Chains -----

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateChain {
    #[schema(write_only)]
    pub rpc_url: Url,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateChain {
    #[schema(write_only)]
    pub rpc_url: Option<Url>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ChainRow {
    pub chain_id: u64,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<ChainView> for ChainRow {
    fn from(record: ChainView) -> Self {
        Self {
            chain_id: record.chain_id,
            enabled: record.enabled,
            created_at: record.created_at,
            updated_at: record.updated_at,
        }
    }
}

// ----- Monitors -----

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateMonitor {
    pub chain_id: u64,
    #[schema(value_type = String, pattern = "^0x[0-9a-fA-F]{40}$")]
    pub address: Address,
    /// Human-readable function or event signature, e.g. `function transfer(address to,
    /// uint256 value)` or `event Transfer(address indexed from, address indexed to, uint256 value)`.
    pub signature: String,
    pub start_block: u64,
    #[serde(default)]
    pub end_block: Option<u64>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateMonitor {
    pub start_block: Option<u64>,
    /// `null` clears end_block (open-ended/live); a number sets a finite end.
    pub end_block: Option<Option<u64>>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AbiParamSchema {
    pub name: String,
    pub sol_type: String,
    pub indexed: bool,
}

impl From<AbiParam> for AbiParamSchema {
    fn from(param: AbiParam) -> Self {
        let sol_type = param.sol_type();
        Self {
            name: param.name,
            sol_type,
            indexed: param.indexed,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MonitorRow {
    pub id: u64,
    pub chain_id: u64,
    #[schema(value_type = String, pattern = "^0x[0-9a-f]{40}$")]
    pub address: Address,
    pub signature: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>, pattern = "^0x[0-9a-f]{8}$")]
    pub selector: Option<Selector>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>, pattern = "^0x[0-9a-f]{64}$")]
    pub topic0: Option<B256>,
    pub param_schema: Vec<AbiParamSchema>,
    pub start_block: u64,
    pub end_block: Option<u64>,
    pub cursor: Option<u64>,
    pub completed: bool,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<MonitorView> for MonitorRow {
    fn from(record: MonitorView) -> Self {
        Self {
            id: record.id.get(),
            chain_id: record.chain_id,
            address: record.address,
            signature: record.signature,
            kind: record.kind.as_str().into(),
            selector: record.selector,
            topic0: record.topic0,
            param_schema: record.param_schema.into_iter().map(Into::into).collect(),
            start_block: record.start_block,
            end_block: record.end_block,
            cursor: record.cursor,
            completed: record.completed,
            enabled: record.enabled,
            created_at: record.created_at,
            updated_at: record.updated_at,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Health {
    pub status: &'static str,
    pub monitors: usize,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Status {
    pub mode: &'static str,
    pub chains: Vec<ChainStatusRow>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ChainStatusRow {
    pub chain_id: u64,
    pub enabled: bool,
    pub worker_state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finalized_head: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_successful_poll_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

impl From<ChainStatusSnapshot> for ChainStatusRow {
    fn from(snapshot: ChainStatusSnapshot) -> Self {
        Self {
            chain_id: snapshot.chain_id,
            enabled: snapshot.enabled,
            worker_state: match snapshot.worker_state {
                WorkerState::Starting => "starting",
                WorkerState::Running => "running",
                WorkerState::Degraded => "degraded",
                WorkerState::Disabled => "disabled",
            },
            finalized_head: snapshot.finalized_head,
            last_successful_poll_at: snapshot.last_successful_poll_at,
            last_error: snapshot.last_error,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
}

// ----- Results search -----

#[derive(Debug, Deserialize, ToSchema)]
pub struct ResultsQuery {
    /// Maximum number of results (default 50, clamped to 200).
    #[serde(default = "default_limit")]
    pub limit: u64,
    /// Pagination offset (default 0).
    #[serde(default)]
    pub offset: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CallMonitorResult {
    #[schema(value_type = String, pattern = "^0x[0-9a-f]{64}$")]
    pub tx_hash: TxHash,
    pub block_number: u64,
    #[schema(value_type = Object)]
    pub params: serde_json::Value,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct EventMonitorResult {
    #[schema(value_type = String, pattern = "^0x[0-9a-f]{64}$")]
    pub tx_hash: TxHash,
    pub log_index: u64,
    pub block_number: u64,
    #[schema(value_type = Object)]
    pub params: serde_json::Value,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MonitorResult {
    Call(CallMonitorResult),
    Event(EventMonitorResult),
}

impl From<MonitorResultView> for MonitorResult {
    fn from(record: MonitorResultView) -> Self {
        match record {
            MonitorResultView::Call { tx_hash, block_number, params } => Self::Call(CallMonitorResult {
                tx_hash,
                block_number,
                params,
            }),
            MonitorResultView::Event { tx_hash, log_index, block_number, params } => Self::Event(EventMonitorResult {
                tx_hash,
                log_index,
                block_number,
                params,
            }),
        }
    }
}

fn default_limit() -> u64 {
    50
}

fn default_enabled() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_rpc_url_is_write_only() {
        let create: CreateChain = serde_json::from_value(serde_json::json!({
            "rpc_url": "https://user:secret@example.invalid"
        }))
        .unwrap();
        assert!(create.enabled);
        assert!(serde_json::from_value::<CreateChain>(serde_json::json!({
            "rpc_url": "not a URL"
        })).is_err());

        let row = ChainRow {
            chain_id: 1,
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let value = serde_json::to_value(row).unwrap();
        assert!(value.get("rpc_url").is_none());
        assert!(!value.to_string().contains("secret"));
    }

    #[test]
    fn rejects_invalid_typed_monitor_fields() {
        let base = serde_json::json!({
            "chain_id": 1,
            "address": "0x0000000000000000000000000000000000000001",
            "signature": "function transfer(address to, uint256 value)",
            "start_block": 0
        });
        assert!(serde_json::from_value::<CreateMonitor>(base).is_ok());
        assert!(serde_json::from_value::<CreateMonitor>(serde_json::json!({
            "chain_id": -1,
            "address": "0x0000000000000000000000000000000000000001",
            "signature": "function transfer(address to, uint256 value)",
            "start_block": 0
        })).is_err());
        assert!(serde_json::from_value::<CreateMonitor>(serde_json::json!({
            "chain_id": 1,
            "address": "not-an-address",
            "signature": "function transfer(address to, uint256 value)",
            "start_block": 0
        })).is_err());
    }

    #[test]
    fn serializes_minimal_call_result() {
        let tx_hash = TxHash::repeat_byte(0x11);
        let result = MonitorResult::from(MonitorResultView::Call {
            tx_hash,
            block_number: 10,
            params: serde_json::json!({"value": "42"}),
        });
        assert_eq!(
            serde_json::to_value(result).unwrap(),
            serde_json::json!({
                "kind": "call",
                "tx_hash": tx_hash.to_string(),
                "block_number": 10,
                "params": {"value": "42"}
            })
        );
    }

    #[test]
    fn serializes_minimal_event_result() {
        let tx_hash = TxHash::repeat_byte(0x22);
        let result = MonitorResult::from(MonitorResultView::Event {
            tx_hash,
            log_index: 3,
            block_number: 11,
            params: serde_json::json!({"owner": "0x1"}),
        });
        assert_eq!(
            serde_json::to_value(result).unwrap(),
            serde_json::json!({
                "kind": "event",
                "tx_hash": tx_hash.to_string(),
                "log_index": 3,
                "block_number": 11,
                "params": {"owner": "0x1"}
            })
        );
    }
}
