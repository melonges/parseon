use std::fmt;
use std::num::NonZeroU64;

pub use alloy::primitives::{Address, B256, BlockNumber, ChainId, Selector, TxHash};
use alloy::primitives::{I256, U256};
pub use url::Url;

pub mod abi;
pub mod commands;
pub mod filter;
pub mod indexer;
pub mod monitor;
pub mod pipeline;
pub mod ports;
pub mod scheduler;
pub mod services;
pub mod status;
pub mod supervisor;
pub mod worker;
pub mod views;

use self::abi::AbiParam;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Chain {
    pub id: ChainId,
}

impl Chain {
    pub const fn new(id: ChainId) -> Self {
        Self { id }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MonitorId(NonZeroU64);

impl MonitorId {
    pub fn new(id: u64) -> anyhow::Result<Self> {
        NonZeroU64::new(id)
            .map(Self)
            .ok_or_else(|| anyhow::anyhow!("monitor id must be positive"))
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for MonitorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Cursor(pub Option<BlockNumber>);

impl Cursor {
    pub fn next(self, start_block: BlockNumber) -> Option<BlockNumber> {
        self.0.map_or(Some(start_block), |block| block.checked_add(1))
    }
}

#[derive(Debug, Clone)]
pub enum Target {
    Call(CallTarget),
    Event(EventTarget),
}

#[derive(Debug, Clone)]
pub struct CallTarget {
    pub address: Address,
    pub selector: Selector,
    pub inputs: Vec<AbiParam>,
}

#[derive(Debug, Clone)]
pub struct EventTarget {
    pub address: Address,
    pub topic0: B256,
    pub params: Vec<AbiParam>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DecodedValue {
    Uint(U256),
    Int(I256),
    Bool(bool),
    Address(Address),
    String(String),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct BlockTransaction {
    pub hash: B256,
    pub from: Address,
    pub to: Address,
    pub input: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct SourceBlock {
    pub number: BlockNumber,
    pub transactions: Vec<BlockTransaction>,
}

#[derive(Debug, Clone)]
pub struct ExecutedTransaction {
    pub transaction: BlockTransaction,
    pub succeeded: bool,
}

#[derive(Debug, Clone)]
pub struct DecodedCall {
    pub monitor_id: MonitorId,
    pub block_number: BlockNumber,
    pub transaction: ExecutedTransaction,
    pub params: Vec<DecodedValue>,
}

#[derive(Debug, Clone)]
pub struct SourceLog {
    pub block_number: Option<BlockNumber>,
    pub transaction_hash: Option<B256>,
    pub log_index: Option<u64>,
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Vec<u8>,
    pub removed: bool,
}

#[derive(Debug, Clone)]
pub struct DecodedEvent {
    pub monitor_id: MonitorId,
    pub block_number: BlockNumber,
    pub transaction_hash: B256,
    pub log_index: u64,
    pub params: Vec<DecodedValue>,
}

#[derive(Debug, Clone)]
pub enum DecodedResult {
    Call(DecodedCall),
    Event(DecodedEvent),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures_util::StreamExt;

    use super::{Cursor, MonitorId};

    #[test]
    fn cursor_computes_next_block() {
        assert_eq!(Cursor(None).next(0), Some(0));
        assert_eq!(Cursor(Some(12)).next(10), Some(13));
        assert_eq!(Cursor(Some(u64::MAX)).next(10), None);
    }

    #[test]
    fn monitor_ids_are_positive() {
        assert_eq!(MonitorId::new(1).unwrap().get(), 1);
        assert!(MonitorId::new(0).is_err());
    }

    #[tokio::test]
    async fn pipeline_bounds_work_and_yields_in_input_order() {
        let current = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let futures = (0..8).map(|value| {
            let current = current.clone();
            let maximum = maximum.clone();
            async move {
                let in_flight = current.fetch_add(1, Ordering::SeqCst) + 1;
                maximum.fetch_max(in_flight, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis((8 - value) as u64)).await;
                current.fetch_sub(1, Ordering::SeqCst);
                value
            }
        });

        let values = super::pipeline::ordered(futures, 3)
            .collect::<Vec<_>>()
            .await;

        assert_eq!(values, (0..8).collect::<Vec<_>>());
        assert_eq!(maximum.load(Ordering::SeqCst), 3);
    }
}
