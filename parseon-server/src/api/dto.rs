use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use parseon_core::status::{ChainStatusSnapshot, WorkerState};
use parseon_core::ports::ParamSchema;
use parseon_core::views::{ChainView, MonitorResultView, MonitorView};
use parseon_core::Url;

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
    pub chain_id: i64,
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
    pub chain_id: i64,
    pub address: String,
    /// Human-readable function or event signature, e.g. `function transfer(address to,
    /// uint256 value)` or `event Transfer(address indexed from, address indexed to, uint256 value)`.
    pub signature: String,
    pub start_block: i64,
    #[serde(default)]
    pub end_block: Option<i64>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateMonitor {
    pub start_block: Option<i64>,
    /// `null` clears end_block (open-ended/live); a number sets a finite end.
    pub end_block: Option<Option<i64>>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AbiParamSchema {
    pub name: String,
    pub sol_type: String,
    pub indexed: bool,
}

impl From<ParamSchema> for AbiParamSchema {
    fn from(param: ParamSchema) -> Self {
        Self {
            name: param.name,
            sol_type: param.sol_type,
            indexed: param.indexed,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MonitorRow {
    pub id: i64,
    pub chain_id: i64,
    pub address: String,
    pub signature: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic0: Option<String>,
    pub param_schema: Vec<AbiParamSchema>,
    pub start_block: i64,
    pub end_block: Option<i64>,
    pub cursor: Option<i64>,
    pub completed: bool,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<MonitorView> for MonitorRow {
    fn from(record: MonitorView) -> Self {
        Self {
            id: record.id,
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
    pub chain_id: i64,
    pub enabled: bool,
    pub worker_state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finalized_head: Option<i64>,
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
    pub limit: i64,
    /// Pagination offset (default 0).
    #[serde(default)]
    pub offset: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CallMonitorResult {
    pub tx_hash: String,
    pub block_number: i64,
    #[schema(value_type = Object)]
    pub params: serde_json::Value,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct EventMonitorResult {
    pub tx_hash: String,
    pub log_index: i64,
    pub block_number: i64,
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

fn default_limit() -> i64 {
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
    fn serializes_minimal_call_result() {
        let result = MonitorResult::from(MonitorResultView::Call {
            tx_hash: "0xcall".into(),
            block_number: 10,
            params: serde_json::json!({"value": "42"}),
        });
        assert_eq!(
            serde_json::to_value(result).unwrap(),
            serde_json::json!({
                "kind": "call",
                "tx_hash": "0xcall",
                "block_number": 10,
                "params": {"value": "42"}
            })
        );
    }

    #[test]
    fn serializes_minimal_event_result() {
        let result = MonitorResult::from(MonitorResultView::Event {
            tx_hash: "0xevent".into(),
            log_index: 3,
            block_number: 11,
            params: serde_json::json!({"owner": "0x1"}),
        });
        assert_eq!(
            serde_json::to_value(result).unwrap(),
            serde_json::json!({
                "kind": "event",
                "tx_hash": "0xevent",
                "log_index": 3,
                "block_number": 11,
                "params": {"owner": "0x1"}
            })
        );
    }
}
