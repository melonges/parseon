use async_trait::async_trait;

use crate::core::DecodedCall;
use crate::monitor::Monitor;

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
