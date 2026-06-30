use alloy::primitives::{Address, B256, I256, U256};

pub mod abi;
pub mod filter;
pub mod indexer;
pub mod monitor;
pub mod ports;
pub mod scheduler;
pub mod worker;

use self::abi::AbiParam;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Chain {
    pub id: i64,
}

impl Chain {
    pub fn new(id: i64) -> anyhow::Result<Self> {
        anyhow::ensure!(id >= 0, "chain ID must be non-negative");
        Ok(Self { id })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Cursor(pub Option<i64>);

impl Cursor {
    pub fn next(self, start_block: i64) -> i64 {
        self.0.map_or(start_block, |block| block + 1)
    }
}

#[derive(Debug, Clone)]
pub struct Target {
    pub address: Address,
    pub selector: [u8; 4],
    pub signature: String,
    pub inputs: Vec<AbiParam>,
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
    pub value: U256,
}

#[derive(Debug, Clone)]
pub struct SourceBlock {
    pub number: i64,
    pub hash: B256,
    pub transactions: Vec<BlockTransaction>,
}

#[derive(Debug, Clone)]
pub struct ExecutedTransaction {
    pub transaction: BlockTransaction,
    pub gas_used: u64,
    pub gas_price: u128,
    pub succeeded: bool,
}

#[derive(Debug, Clone)]
pub struct DecodedCall {
    pub monitor_id: i64,
    pub block_number: i64,
    pub block_hash: B256,
    pub transaction: ExecutedTransaction,
    pub params: Vec<DecodedValue>,
}

#[cfg(test)]
mod tests {
    use super::{Chain, Cursor};

    #[test]
    fn validates_chain_ids() {
        assert_eq!(Chain::new(8453).unwrap().id, 8453);
        assert!(Chain::new(-1).is_err());
    }

    #[test]
    fn cursor_computes_next_block() {
        assert_eq!(Cursor(None).next(10), 10);
        assert_eq!(Cursor(Some(12)).next(10), 13);
    }
}
