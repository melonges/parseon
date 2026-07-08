use std::fmt;
use std::sync::Arc;

use anyhow::Context;

use crate::abi::{TargetSpec, parse_target_signature};
use crate::commands::{CreateChain, CreateMonitor, ResultQuery, UpdateChain, UpdateMonitor};
use crate::ports::{
    BlockSourceFactory, ChainRepository, ChainUpdate, MonitorRepository, MonitorUpdate, NewChain,
    NewMonitor, ResultRepository,
};
use crate::views::{ChainView, MonitorResultView, MonitorView};
use crate::{CallTarget, Chain, ChainId, EventTarget, MonitorId, Target, Url, worker};

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

    async fn validate_source(&self, rpc_url: &Url, expected: Option<Chain>) -> anyhow::Result<Chain> {
        let source = self.sources.connect(rpc_url).map_err(|_| invalid("RPC endpoint configuration is invalid"))?;
        let probe = worker::probe_source(source.as_ref()).await.map_err(|_| invalid("RPC endpoint validation failed"))?;
        let chain = Chain::new(probe.chain_id);
        if expected.is_some_and(|expected| expected != chain) {
            return Err(invalid("RPC endpoint returned a different chain ID"));
        }
        Ok(chain)
    }

    pub async fn create(&self, command: CreateChain) -> anyhow::Result<ChainView> {
        let chain = self.validate_source(&command.rpc_url, None).await?;
        self.repository.create_chain(NewChain { chain, rpc_url: command.rpc_url, enabled: command.enabled }).await.context("create chain").map(Into::into)
    }

    pub async fn list(&self) -> anyhow::Result<Vec<ChainView>> {
        Ok(self.repository.list_chains().await.context("list chains")?.into_iter().map(Into::into).collect())
    }

    pub async fn get(&self, chain_id: ChainId) -> anyhow::Result<ChainView> {
        let chain = Chain::new(chain_id);
        self.repository.get_chain(chain).await.context("get chain").map(Into::into)
    }

    pub async fn update(&self, chain_id: ChainId, command: UpdateChain) -> anyhow::Result<ChainView> {
        if command.rpc_url.is_none() && command.enabled.is_none() {
            return Err(invalid("at least one of rpc_url or enabled is required"));
        }
        let chain = Chain::new(chain_id);
        self.repository.get_chain(chain).await.context("get chain before update")?;
        if let Some(url) = command.rpc_url.as_ref() {
            self.validate_source(url, Some(chain)).await?;
        }
        self.repository.update_chain(chain, ChainUpdate { rpc_url: command.rpc_url, enabled: command.enabled }).await.context("update chain").map(Into::into)
    }

    pub async fn delete(&self, chain_id: ChainId) -> anyhow::Result<()> {
        let chain = Chain::new(chain_id);
        self.repository.delete_chain(chain).await.context("delete chain")
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
        self.monitors.count_monitors().await.context("count monitors")
    }

    pub async fn create(&self, command: CreateMonitor) -> anyhow::Result<MonitorView> {
        validate_range(command.start_block, command.end_block)?;
        let chain = Chain::new(command.chain_id);
        self.chains.get_chain(chain).await.context("get monitor chain")?;
        let spec = parse_target_signature(&command.signature).map_err(|error| invalid(error.to_string()))?;
        let target = match spec {
            TargetSpec::Call(spec) => Target::Call(CallTarget {
                address: command.address,
                selector: spec.selector,
                inputs: spec.params,
            }),
            TargetSpec::Event(spec) => Target::Event(EventTarget {
                address: command.address,
                topic0: spec.topic0,
                params: spec.params,
            }),
        };
        self.monitors.create_monitor(NewMonitor {
            chain,
            target,
            start_block: command.start_block,
            end_block: command.end_block,
        }).await.context("create monitor").map(Into::into)
    }

    pub async fn list(&self, chain_id: Option<ChainId>) -> anyhow::Result<Vec<MonitorView>> {
        let chain = chain_id.map(Chain::new);
        Ok(self.monitors.list_monitors(chain).await.context("list monitors")?.into_iter().map(Into::into).collect())
    }

    pub async fn get(&self, id: MonitorId) -> anyhow::Result<MonitorView> {
        self.monitors.get_monitor(id).await.context("get monitor").map(Into::into)
    }

    pub async fn update(&self, id: MonitorId, command: UpdateMonitor) -> anyhow::Result<MonitorView> {
        if command.start_block.is_none() && command.end_block.is_none() && command.enabled.is_none() {
            return Err(invalid("at least one monitor field is required"));
        }
        let current = self.monitors.get_monitor(id).await.context("get monitor before update")?;
        let start_block = command.start_block.unwrap_or(current.start_block);
        let end_block = command.end_block.unwrap_or(current.end_block);
        validate_range(start_block, end_block)?;
        let reindex = start_block != current.start_block || end_block.is_some_and(|end| current.cursor.is_some_and(|cursor| cursor > end));
        let cursor = if reindex {
            start_block.checked_sub(1)
        } else {
            current.cursor
        };
        let completed = end_block.is_some_and(|end| cursor.is_some_and(|cursor| cursor >= end));
        self.monitors.update_monitor(id, MonitorUpdate {
            start_block,
            end_block,
            cursor,
            completed,
            enabled: command.enabled.unwrap_or(current.enabled),
            reindex,
        }).await.context("update monitor").map(Into::into)
    }

    pub async fn delete(&self, id: MonitorId) -> anyhow::Result<()> {
        self.monitors.delete_monitor(id).await.context("delete monitor")
    }

    pub async fn results(&self, id: MonitorId, query: ResultQuery) -> anyhow::Result<Vec<MonitorResultView>> {
        let monitor = self.monitors.get_monitor(id).await.context("get monitor for result query")?;
        Ok(self.results.query_results(&monitor, query).await.context("query monitor results")?.into_iter().map(Into::into).collect())
    }
}

fn validate_range(start_block: u64, end_block: Option<u64>) -> anyhow::Result<()> {
    if end_block.is_some_and(|end| end < start_block) {
        return Err(invalid("end_block must be greater than or equal to start_block"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_range;

    #[test]
    fn validates_monitor_ranges() {
        assert!(validate_range(0, None).is_ok());
        assert!(validate_range(10, Some(10)).is_ok());
        assert!(validate_range(10, Some(9)).is_err());
    }
}
