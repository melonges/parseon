use std::fmt;
use std::sync::Arc;

use alloy::primitives::Address;

use crate::abi::{AbiParam, TargetSpec, parse_target_signature};
use crate::commands::{CreateChain, CreateMonitor, ResultQuery, UpdateChain, UpdateMonitor};
use crate::ports::{
    BlockSourceFactory, ChainRepository, ChainUpdate, MonitorKind, MonitorRepository,
    MonitorUpdate, NewChain, NewMonitor, ParamSchema, ResultRepository,
};
use crate::views::{ChainView, MonitorResultView, MonitorView};
use crate::{Chain, worker};

#[derive(Debug)]
pub struct InvalidCommand(String);

impl fmt::Display for InvalidCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for InvalidCommand {}

fn invalid(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(InvalidCommand(message.into()))
}

pub fn is_invalid_command(error: &anyhow::Error) -> bool {
    error.downcast_ref::<InvalidCommand>().is_some()
}

#[derive(Clone)]
pub struct ChainService {
    repository: Arc<dyn ChainRepository>,
    sources: Arc<dyn BlockSourceFactory>,
}

impl ChainService {
    pub fn new(repository: Arc<dyn ChainRepository>, sources: Arc<dyn BlockSourceFactory>) -> Self {
        Self { repository, sources }
    }

    async fn validate_source(&self, rpc_url: &str, expected: Option<Chain>) -> anyhow::Result<Chain> {
        if rpc_url.trim().is_empty() {
            return Err(invalid("rpc_url must not be empty"));
        }
        let source = self.sources.connect(rpc_url).map_err(|_| invalid("RPC endpoint configuration is invalid"))?;
        let probe = worker::probe_source(source.as_ref()).await.map_err(|_| invalid("RPC endpoint validation failed"))?;
        let chain_id = i64::try_from(probe.chain_id).map_err(|_| invalid("RPC endpoint returned an unsupported chain ID"))?;
        let chain = Chain::new(chain_id).map_err(|_| invalid("RPC endpoint returned an invalid chain ID"))?;
        if expected.is_some_and(|expected| expected != chain) {
            return Err(invalid("RPC endpoint returned a different chain ID"));
        }
        Ok(chain)
    }

    pub async fn create(&self, command: CreateChain) -> anyhow::Result<ChainView> {
        let chain = self.validate_source(&command.rpc_url, None).await?;
        self.repository.create_chain(NewChain { chain, rpc_url: command.rpc_url, enabled: command.enabled }).await.map(Into::into)
    }

    pub async fn list(&self) -> anyhow::Result<Vec<ChainView>> {
        Ok(self.repository.list_chains().await?.into_iter().map(Into::into).collect())
    }

    pub async fn get(&self, chain_id: i64) -> anyhow::Result<ChainView> {
        self.repository.get_chain(Chain::new(chain_id)?).await.map(Into::into)
    }

    pub async fn update(&self, chain_id: i64, command: UpdateChain) -> anyhow::Result<ChainView> {
        if command.rpc_url.is_none() && command.enabled.is_none() {
            return Err(invalid("at least one of rpc_url or enabled is required"));
        }
        let chain = Chain::new(chain_id).map_err(|error| invalid(error.to_string()))?;
        self.repository.get_chain(chain).await?;
        if let Some(url) = command.rpc_url.as_deref() {
            self.validate_source(url, Some(chain)).await?;
        }
        self.repository.update_chain(chain, ChainUpdate { rpc_url: command.rpc_url, enabled: command.enabled }).await.map(Into::into)
    }

    pub async fn delete(&self, chain_id: i64) -> anyhow::Result<()> {
        self.repository.delete_chain(Chain::new(chain_id)?).await
    }
}

#[derive(Clone)]
pub struct MonitorService {
    chains: Arc<dyn ChainRepository>,
    monitors: Arc<dyn MonitorRepository>,
    results: Arc<dyn ResultRepository>,
}

impl MonitorService {
    pub fn new(chains: Arc<dyn ChainRepository>, monitors: Arc<dyn MonitorRepository>, results: Arc<dyn ResultRepository>) -> Self {
        Self { chains, monitors, results }
    }

    pub async fn count(&self) -> anyhow::Result<usize> {
        self.monitors.count_monitors().await
    }

    pub async fn create(&self, command: CreateMonitor) -> anyhow::Result<MonitorView> {
        validate_range(command.start_block, command.end_block)?;
        let chain = Chain::new(command.chain_id).map_err(|error| invalid(error.to_string()))?;
        self.chains.get_chain(chain).await?;
        let address: Address = command.address.parse().map_err(|error| invalid(format!("invalid address: {error}")))?;
        let spec = parse_target_signature(&command.signature).map_err(|error| invalid(error.to_string()))?;
        let (kind, signature_hash, params) = match spec {
            TargetSpec::Call(spec) => (MonitorKind::Call, format!("0x{}", alloy::hex::encode(spec.selector)), spec.params),
            TargetSpec::Event(spec) => (MonitorKind::Event, spec.topic0.to_string(), spec.params),
        };
        self.monitors.create_monitor(NewMonitor {
            chain,
            address: address.to_string().to_ascii_lowercase(),
            signature: command.signature,
            kind,
            signature_hash,
            param_schema: params.iter().map(param_schema).collect(),
            start_block: command.start_block,
            end_block: command.end_block,
        }).await.map(Into::into)
    }

    pub async fn list(&self, chain_id: Option<i64>) -> anyhow::Result<Vec<MonitorView>> {
        let chain = chain_id.map(Chain::new).transpose().map_err(|error| invalid(error.to_string()))?;
        Ok(self.monitors.list_monitors(chain).await?.into_iter().map(Into::into).collect())
    }

    pub async fn get(&self, id: i64) -> anyhow::Result<MonitorView> {
        self.monitors.get_monitor(id).await.map(Into::into)
    }

    pub async fn update(&self, id: i64, command: UpdateMonitor) -> anyhow::Result<MonitorView> {
        if command.start_block.is_none() && command.end_block.is_none() && command.enabled.is_none() {
            return Err(invalid("at least one monitor field is required"));
        }
        let current = self.monitors.get_monitor(id).await?;
        let start_block = command.start_block.unwrap_or(current.start_block);
        let end_block = command.end_block.unwrap_or(current.end_block);
        validate_range(start_block, end_block)?;
        let reindex = start_block != current.start_block || end_block.is_some_and(|end| current.cursor.is_some_and(|cursor| cursor > end));
        let cursor = reindex.then_some(start_block - 1).or(current.cursor);
        let completed = end_block.is_some_and(|end| cursor.is_some_and(|cursor| cursor >= end));
        self.monitors.update_monitor(id, MonitorUpdate {
            start_block,
            end_block,
            cursor,
            completed,
            enabled: command.enabled.unwrap_or(current.enabled),
            reindex,
        }).await.map(Into::into)
    }

    pub async fn delete(&self, id: i64) -> anyhow::Result<()> {
        self.monitors.delete_monitor(id).await
    }

    pub async fn results(&self, id: i64, query: ResultQuery) -> anyhow::Result<Vec<MonitorResultView>> {
        let monitor = self.monitors.get_monitor(id).await?;
        let query = ResultQuery { limit: query.limit.clamp(1, 200), offset: query.offset.max(0) };
        Ok(self.results.query_results(&monitor, query).await?.into_iter().map(Into::into).collect())
    }
}

fn validate_range(start_block: i64, end_block: Option<i64>) -> anyhow::Result<()> {
    if start_block < 0 {
        return Err(invalid("start_block must be non-negative"));
    }
    if end_block.is_some_and(|end| end < start_block) {
        return Err(invalid("end_block must be greater than or equal to start_block"));
    }
    Ok(())
}

fn param_schema(param: &AbiParam) -> ParamSchema {
    ParamSchema { name: param.name.clone(), sol_type: param.sol_type(), indexed: param.indexed }
}
