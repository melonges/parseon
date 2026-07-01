use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::db::dyn_table::ResultRecord;
use crate::db::monitor_repo::{MonitorRecord, StoredParam};

// ----- Monitors -----

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateMonitor {
    pub address: String,
    /// Human-readable function signature, e.g.
    /// `function transfer(address to, uint256 value) returns (bool)`
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
}

impl From<StoredParam> for AbiParamSchema {
    fn from(param: StoredParam) -> Self {
        Self {
            name: param.name,
            sol_type: param.sol_type,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MonitorRow {
    pub id: i64,
    pub address: String,
    pub signature: String,
    pub selector: String,
    pub param_schema: Vec<AbiParamSchema>,
    pub start_block: i64,
    pub end_block: Option<i64>,
    pub cursor: Option<i64>,
    pub completed: bool,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<MonitorRecord> for MonitorRow {
    fn from(record: MonitorRecord) -> Self {
        Self {
            id: record.id,
            address: record.address,
            signature: record.signature,
            selector: record.selector,
            param_schema: record.param_schema.0.into_iter().map(Into::into).collect(),
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
    pub chain_id: i64,
    pub finalized_head: i64,
    pub worker_state: &'static str,
    pub last_successful_poll_at: DateTime<Utc>,
    pub last_error: Option<String>,
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
    /// Filter by transaction sender (normalized, case-insensitive).
    pub from_addr: Option<String>,
    /// Filter by transaction status (1 = success, 0 = reverted).
    pub status: Option<i16>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MonitorResult {
    pub tx_hash: String,
    pub block_number: i64,
    pub block_hash: String,
    pub from_addr: String,
    pub to_addr: String,
    pub value: String,
    pub gas_used: String,
    pub gas_price: String,
    pub status: i16,
    pub created_at: DateTime<Utc>,
    #[schema(value_type = Object)]
    pub params: serde_json::Value,
}

impl From<ResultRecord> for MonitorResult {
    fn from(record: ResultRecord) -> Self {
        Self {
            tx_hash: record.tx_hash,
            block_number: record.block_number,
            block_hash: record.block_hash,
            from_addr: record.from_addr,
            to_addr: record.to_addr,
            value: record.value,
            gas_used: record.gas_used,
            gas_price: record.gas_price,
            status: record.status,
            created_at: record.created_at,
            params: record.params,
        }
    }
}

fn default_limit() -> i64 {
    50
}
