use chrono::{DateTime, Utc};

use crate::ports::{ChainRecord, MonitorKind, MonitorRecord, ResultRecord};

#[derive(Debug, Clone)]
pub struct ChainView {
    pub chain_id: i64,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
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

#[derive(Debug, Clone)]
pub struct MonitorView {
    pub id: i64,
    pub chain_id: i64,
    pub address: String,
    pub signature: String,
    pub kind: MonitorKind,
    pub selector: Option<String>,
    pub topic0: Option<String>,
    pub param_schema: Vec<crate::ports::ParamSchema>,
    pub start_block: i64,
    pub end_block: Option<i64>,
    pub cursor: Option<i64>,
    pub completed: bool,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<MonitorRecord> for MonitorView {
    fn from(record: MonitorRecord) -> Self {
        let (selector, topic0) = match record.kind {
            MonitorKind::Call => (Some(record.signature_hash.clone()), None),
            MonitorKind::Event => (None, Some(record.signature_hash.clone())),
        };
        Self {
            id: record.id,
            chain_id: record.chain.id,
            address: record.address,
            signature: record.signature,
            kind: record.kind,
            selector,
            topic0,
            param_schema: record.param_schema,
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

#[derive(Debug, Clone)]
pub enum MonitorResultView {
    Call {
        tx_hash: String,
        block_number: i64,
        params: serde_json::Value,
    },
    Event {
        tx_hash: String,
        log_index: i64,
        block_number: i64,
        params: serde_json::Value,
    },
}

impl From<ResultRecord> for MonitorResultView {
    fn from(record: ResultRecord) -> Self {
        match record {
            ResultRecord::Call { tx_hash, block_number, params } => Self::Call {
                tx_hash,
                block_number,
                params,
            },
            ResultRecord::Event { tx_hash, log_index, block_number, params } => Self::Event {
                tx_hash,
                log_index,
                block_number,
                params,
            },
        }
    }
}
