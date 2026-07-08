use chrono::{DateTime, Utc};

use crate::abi::AbiParam;
use crate::ports::{ChainRecord, MonitorKind, MonitorRecord, ResultRecord};
use crate::{Address, B256, BlockNumber, ChainId, MonitorId, Selector, Target, TxHash};

#[derive(Debug, Clone)]
pub struct ChainView {
    pub chain_id: ChainId,
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
    pub id: MonitorId,
    pub chain_id: ChainId,
    pub address: Address,
    pub kind: MonitorKind,
    pub selector: Option<Selector>,
    pub topic0: Option<B256>,
    pub param_schema: Vec<AbiParam>,
    pub start_block: BlockNumber,
    pub end_block: Option<BlockNumber>,
    pub cursor: Option<BlockNumber>,
    pub completed: bool,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<MonitorRecord> for MonitorView {
    fn from(record: MonitorRecord) -> Self {
        let (address, kind, selector, topic0, param_schema) = match record.target {
            Target::Call(target) => (
                target.address,
                MonitorKind::Call,
                Some(target.selector),
                None,
                target.inputs,
            ),
            Target::Event(target) => (
                target.address,
                MonitorKind::Event,
                None,
                Some(target.topic0),
                target.params,
            ),
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
            created_at: record.created_at,
            updated_at: record.updated_at,
        }
    }
}

#[derive(Debug, Clone)]
pub enum MonitorResultView {
    Call {
        tx_hash: TxHash,
        block_number: BlockNumber,
        params: serde_json::Value,
    },
    Event {
        tx_hash: TxHash,
        log_index: u64,
        block_number: BlockNumber,
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
