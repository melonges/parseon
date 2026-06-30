pub mod fetch;
pub mod provider;

use async_trait::async_trait;

use crate::core::{BlockTransaction, ExecutedTransaction, SourceBlock};

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
