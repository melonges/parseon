use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

use super::filter::FilterDefinition;
use super::monitor::Monitor;
use super::{
    BlockNumber, BlockTransaction, Chain, ChainId, DecodedResult, ExecutedTransaction, MonitorId,
    SourceBlock, SourceLog, Target, TxHash, Url,
};
use alloy::primitives::{Address, B256};
use chrono::{DateTime, Utc};

pub struct BlockCommit {
    pub chain: Chain,
    pub block_number: BlockNumber,
    pub monitors: Vec<Monitor>,
    pub results: Vec<DecodedResult>,
}

#[async_trait]
pub trait IndexStorage: Send + Sync {
    async fn load_monitors(&self, chain: Chain) -> anyhow::Result<Vec<Monitor>>;
    async fn commit_block(&self, commit: BlockCommit) -> anyhow::Result<usize>;
}

#[derive(Clone, PartialEq, Eq)]
pub struct RegisteredChain {
    pub chain: Chain,
    pub rpc_url: Url,
    pub enabled: bool,
}

#[async_trait]
pub trait ChainRepository: Send + Sync {
    async fn list_registered_chains(&self) -> anyhow::Result<Vec<RegisteredChain>>;
    async fn create_chain(&self, chain: NewChain) -> anyhow::Result<ChainRecord>;
    async fn list_chains(&self) -> anyhow::Result<Vec<ChainRecord>>;
    async fn get_chain(&self, chain: Chain) -> anyhow::Result<ChainRecord>;
    async fn update_chain(&self, chain: Chain, update: ChainUpdate) -> anyhow::Result<ChainRecord>;
    async fn delete_chain(&self, chain: Chain) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct NewChain {
    pub chain: Chain,
    pub rpc_url: Url,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct ChainUpdate {
    pub rpc_url: Option<Url>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct ChainRecord {
    pub chain: Chain,
    pub rpc_url: Url,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonitorKind {
    Call,
    Event,
}

impl MonitorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Call => "call",
            Self::Event => "event",
        }
    }
}

#[derive(Debug, Clone)]
pub struct NewMonitor {
    pub chain: Chain,
    pub target: Target,
    pub start_block: BlockNumber,
    pub end_block: Option<BlockNumber>,
    pub filter: Option<FilterDefinition>,
}

#[derive(Debug, Clone)]
pub struct MonitorRecord {
    pub id: MonitorId,
    pub chain: Chain,
    pub target: Target,
    pub start_block: BlockNumber,
    pub end_block: Option<BlockNumber>,
    pub cursor: Option<BlockNumber>,
    pub completed: bool,
    pub enabled: bool,
    pub filter: Option<FilterDefinition>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub enum ResultRecord {
    Call { tx_hash: TxHash, block_number: BlockNumber, params: serde_json::Value },
    Event { tx_hash: TxHash, log_index: u64, block_number: BlockNumber, params: serde_json::Value },
}

#[async_trait]
pub trait MonitorRepository: Send + Sync {
    async fn count_monitors(&self) -> anyhow::Result<usize>;
    async fn create_monitor(&self, monitor: NewMonitor) -> anyhow::Result<MonitorRecord>;
    async fn list_monitors(&self, chain: Option<Chain>) -> anyhow::Result<Vec<MonitorRecord>>;
    async fn get_monitor(&self, id: MonitorId) -> anyhow::Result<MonitorRecord>;
    async fn set_monitor_enabled(
        &self,
        id: MonitorId,
        enabled: bool,
    ) -> anyhow::Result<MonitorRecord>;
    async fn delete_monitor(&self, id: MonitorId) -> anyhow::Result<()>;
}

#[async_trait]
pub trait ResultRepository: Send + Sync {
    async fn query_results(
        &self,
        monitor: &MonitorRecord,
        query: crate::commands::ResultQuery,
    ) -> anyhow::Result<Vec<ResultRecord>>;
}

pub trait BlockSourceFactory: Send + Sync {
    fn connect(&self, rpc_url: &Url) -> anyhow::Result<Arc<dyn BlockSource>>;
}

#[async_trait]
pub trait BlockSource: Send + Sync {
    async fn chain_id(&self) -> anyhow::Result<u64>;
    async fn finalized_head(&self) -> anyhow::Result<BlockNumber>;
    async fn fetch_block(&self, block_number: BlockNumber) -> anyhow::Result<SourceBlock>;
    async fn fetch_executed_transactions(
        &self,
        block: &SourceBlock,
        transactions: &[BlockTransaction],
    ) -> anyhow::Result<Vec<ExecutedTransaction>>;
    async fn fetch_logs(
        &self,
        _block_number: BlockNumber,
        _addresses: &[Address],
        _topic0s: &[B256],
    ) -> anyhow::Result<Vec<SourceLog>> {
        anyhow::bail!("log fetching is not implemented")
    }
}

pub trait BlockCache: Send + Sync {
    fn get(&self, chain: Chain, block_number: BlockNumber) -> Option<SourceBlock>;
    fn put(&self, chain: Chain, block: SourceBlock);
    fn evict_before(&self, chain: Chain, block_number: BlockNumber);
}

pub trait BlockCacheFactory: Send + Sync {
    fn create(&self) -> Arc<dyn BlockCache>;
}

pub trait Telemetry: Send + Sync {
    fn record_rpc(
        &self,
        chain_id: ChainId,
        operation: &'static str,
        strategy: &'static str,
        outcome: &'static str,
        elapsed: Duration,
    );
    fn record_cache(&self, chain_id: ChainId, hit: bool);
    fn record_commit(
        &self,
        chain_id: ChainId,
        calls: u64,
        events: u64,
        outcome: &'static str,
        elapsed: Duration,
    );
    fn set_worker_lag(&self, chain_id: ChainId, lag: BlockNumber);
    fn adjust_in_flight(&self, chain_id: ChainId, stage: &'static str, delta: i64);
    fn render(&self) -> anyhow::Result<String>;
}

pub struct InFlightGuard<'a> {
    telemetry: &'a dyn Telemetry,
    chain_id: ChainId,
    stage: &'static str,
}

impl<'a> InFlightGuard<'a> {
    pub fn new(telemetry: &'a dyn Telemetry, chain_id: ChainId, stage: &'static str) -> Self {
        telemetry.adjust_in_flight(chain_id, stage, 1);
        Self { telemetry, chain_id, stage }
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.telemetry.adjust_in_flight(self.chain_id, self.stage, -1);
    }
}

#[derive(Default)]
pub struct NoopTelemetry;

impl Telemetry for NoopTelemetry {
    fn record_rpc(
        &self,
        _: ChainId,
        _: &'static str,
        _: &'static str,
        _: &'static str,
        _: Duration,
    ) {
    }
    fn record_cache(&self, _: ChainId, _: bool) {}
    fn record_commit(&self, _: ChainId, _: u64, _: u64, _: &'static str, _: Duration) {}
    fn set_worker_lag(&self, _: ChainId, _: BlockNumber) {}
    fn adjust_in_flight(&self, _: ChainId, _: &'static str, _: i64) {}
    fn render(&self) -> anyhow::Result<String> {
        Ok(String::new())
    }
}
