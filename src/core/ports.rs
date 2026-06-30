use async_trait::async_trait;

use super::monitor::Monitor;
use super::{BlockTransaction, Chain, DecodedCall, ExecutedTransaction, SourceBlock};

pub struct BlockCommit {
    pub block_number: i64,
    pub monitors: Vec<Monitor>,
    pub calls: Vec<DecodedCall>,
}

#[async_trait]
pub trait Storage: Send + Sync {
    async fn load_monitors(&self) -> anyhow::Result<Vec<Monitor>>;
    async fn commit_block(&self, commit: BlockCommit) -> anyhow::Result<usize>;
}

#[async_trait]
pub trait BlockSource: Send + Sync {
    async fn chain_id(&self) -> anyhow::Result<u64>;
    async fn finalized_head(&self) -> anyhow::Result<i64>;
    async fn fetch_block(&self, block_number: i64) -> anyhow::Result<SourceBlock>;
    async fn fetch_receipts(
        &self,
        transactions: &[BlockTransaction],
    ) -> anyhow::Result<Vec<ExecutedTransaction>>;
}

pub trait BlockCache: Send + Sync {
    fn get(&self, chain: Chain, block_number: i64) -> Option<SourceBlock>;
    fn put(&self, chain: Chain, block: SourceBlock);
    fn evict_before(&self, chain: Chain, block_number: i64);
}
