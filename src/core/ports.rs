use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

use super::monitor::Monitor;
use super::{BlockTransaction, Chain, DecodedResult, ExecutedTransaction, SourceBlock, SourceLog};
use alloy::primitives::{Address, B256};

pub struct BlockCommit {
    pub chain: Chain,
    pub block_number: i64,
    pub monitors: Vec<Monitor>,
    pub results: Vec<DecodedResult>,
}

#[async_trait]
pub trait Storage: Send + Sync {
    async fn load_monitors(&self, chain: Chain) -> anyhow::Result<Vec<Monitor>>;
    async fn commit_block(&self, commit: BlockCommit) -> anyhow::Result<usize>;
}

#[derive(Clone, PartialEq, Eq)]
pub struct RegisteredChain {
    pub chain: Chain,
    pub rpc_url: String,
    pub enabled: bool,
}

#[async_trait]
pub trait ChainRegistry: Send + Sync {
    async fn list_registered_chains(&self) -> anyhow::Result<Vec<RegisteredChain>>;
}

pub trait BlockSourceFactory: Send + Sync {
    fn connect(&self, rpc_url: &str) -> anyhow::Result<Arc<dyn BlockSource>>;
}

#[async_trait]
pub trait BlockSource: Send + Sync {
    async fn chain_id(&self) -> anyhow::Result<u64>;
    async fn finalized_head(&self) -> anyhow::Result<i64>;
    async fn fetch_block(&self, block_number: i64) -> anyhow::Result<SourceBlock>;
    async fn fetch_executed_transactions(
        &self,
        block: &SourceBlock,
        transactions: &[BlockTransaction],
    ) -> anyhow::Result<Vec<ExecutedTransaction>>;
    async fn fetch_logs(
        &self,
        _block_number: i64,
        _addresses: &[Address],
        _topic0s: &[B256],
    ) -> anyhow::Result<Vec<SourceLog>> {
        anyhow::bail!("log fetching is not implemented")
    }
}

pub trait BlockCache: Send + Sync {
    fn get(&self, chain: Chain, block_number: i64) -> Option<SourceBlock>;
    fn put(&self, chain: Chain, block: SourceBlock);
    fn evict_before(&self, chain: Chain, block_number: i64);
}

pub trait BlockCacheFactory: Send + Sync {
    fn create(&self) -> Arc<dyn BlockCache>;
}

pub trait Telemetry: Send + Sync {
    fn record_rpc(
        &self,
        chain_id: i64,
        operation: &'static str,
        strategy: &'static str,
        outcome: &'static str,
        elapsed: Duration,
    );
    fn record_cache(&self, chain_id: i64, hit: bool);
    fn record_commit(
        &self,
        chain_id: i64,
        calls: u64,
        events: u64,
        outcome: &'static str,
        elapsed: Duration,
    );
    fn set_worker_lag(&self, chain_id: i64, lag: i64);
    fn adjust_in_flight(&self, chain_id: i64, stage: &'static str, delta: i64);
    fn render(&self) -> anyhow::Result<String>;
}

pub struct InFlightGuard<'a> {
    telemetry: &'a dyn Telemetry,
    chain_id: i64,
    stage: &'static str,
}

impl<'a> InFlightGuard<'a> {
    pub fn new(telemetry: &'a dyn Telemetry, chain_id: i64, stage: &'static str) -> Self {
        telemetry.adjust_in_flight(chain_id, stage, 1);
        Self {
            telemetry,
            chain_id,
            stage,
        }
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.telemetry
            .adjust_in_flight(self.chain_id, self.stage, -1);
    }
}

#[derive(Default)]
pub struct NoopTelemetry;

impl Telemetry for NoopTelemetry {
    fn record_rpc(&self, _: i64, _: &'static str, _: &'static str, _: &'static str, _: Duration) {}
    fn record_cache(&self, _: i64, _: bool) {}
    fn record_commit(&self, _: i64, _: u64, _: u64, _: &'static str, _: Duration) {}
    fn set_worker_lag(&self, _: i64, _: i64) {}
    fn adjust_in_flight(&self, _: i64, _: &'static str, _: i64) {}
    fn render(&self) -> anyhow::Result<String> {
        Ok(String::new())
    }
}
