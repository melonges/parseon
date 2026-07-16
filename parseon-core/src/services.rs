use std::fmt;
use std::sync::Arc;

use anyhow::Context;

use crate::abi::{TargetSpec, parse_target_signature};
use crate::commands::{CreateChain, CreateMonitor, PreviewFilter, ResultQuery, UpdateChain};
use crate::filter::{self, FilterDefinition, FilterPreview};
use crate::ports::{BlockSourceFactory, ChainUpdate, NewChain, NewMonitor, Storage};
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
    storage: Arc<dyn Storage>,
    sources: Arc<dyn BlockSourceFactory>,
}

impl ChainService {
    pub fn new(storage: Arc<dyn Storage>, sources: Arc<dyn BlockSourceFactory>) -> Self {
        Self { storage, sources }
    }

    async fn validate_source(
        &self,
        rpc_url: &Url,
        expected: Option<Chain>,
    ) -> anyhow::Result<Chain> {
        let source = self
            .sources
            .connect(rpc_url)
            .map_err(|_| invalid("RPC endpoint configuration is invalid"))?;
        let probe = worker::probe_source(source.as_ref())
            .await
            .map_err(|_| invalid("RPC endpoint validation failed"))?;
        let chain = Chain::new(probe.chain_id);
        if expected.is_some_and(|expected| expected != chain) {
            return Err(invalid("RPC endpoint returned a different chain ID"));
        }
        Ok(chain)
    }

    pub async fn create(&self, command: CreateChain) -> anyhow::Result<ChainView> {
        let chain = self.validate_source(&command.rpc_url, None).await?;
        self.storage
            .create_chain(NewChain { chain, rpc_url: command.rpc_url, enabled: command.enabled })
            .await
            .context("create chain")
            .map(Into::into)
    }

    pub async fn list(&self) -> anyhow::Result<Vec<ChainView>> {
        Ok(self
            .storage
            .list_chains()
            .await
            .context("list chains")?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub async fn get(&self, chain_id: ChainId) -> anyhow::Result<ChainView> {
        let chain = Chain::new(chain_id);
        self.storage.get_chain(chain).await.context("get chain").map(Into::into)
    }

    pub async fn update(
        &self,
        chain_id: ChainId,
        command: UpdateChain,
    ) -> anyhow::Result<ChainView> {
        if command.rpc_url.is_none() && command.enabled.is_none() {
            return Err(invalid("at least one of rpc_url or enabled is required"));
        }
        let chain = Chain::new(chain_id);
        self.storage.get_chain(chain).await.context("get chain before update")?;
        if let Some(url) = command.rpc_url.as_ref() {
            self.validate_source(url, Some(chain)).await?;
        }
        self.storage
            .update_chain(chain, ChainUpdate { rpc_url: command.rpc_url, enabled: command.enabled })
            .await
            .context("update chain")
            .map(Into::into)
    }

    pub async fn delete(&self, chain_id: ChainId) -> anyhow::Result<()> {
        let chain = Chain::new(chain_id);
        self.storage.delete_chain(chain).await.context("delete chain")
    }
}

#[derive(Clone)]
pub struct MonitorService {
    storage: Arc<dyn Storage>,
    sources: Arc<dyn BlockSourceFactory>,
}

impl MonitorService {
    pub fn new(storage: Arc<dyn Storage>, sources: Arc<dyn BlockSourceFactory>) -> Self {
        Self { storage, sources }
    }

    pub async fn count(&self) -> anyhow::Result<usize> {
        self.storage.count_monitors().await.context("count monitors")
    }

    pub async fn create(&self, command: CreateMonitor) -> anyhow::Result<MonitorView> {
        let chain = Chain::new(command.chain_id);
        let registered = self.storage.get_chain(chain).await.context("get monitor chain")?;
        let start_block = match command.start_block {
            Some(block) => block,
            None => self
                .sources
                .connect(&registered.rpc_url)
                .context("connect monitor chain block source")?
                .finalized_head()
                .await
                .context("fetch monitor chain finalized head")?,
        };
        validate_range(start_block, command.end_block)?;
        let spec = parse_target_signature(&command.signature)
            .map_err(|error| invalid(error.to_string()))?;
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
        let filter = command
            .filter
            .map(|expression| FilterDefinition::prepare(expression, &target).map(|value| value.0))
            .transpose()
            .map_err(|error| invalid(error.to_string()))?;
        self.storage
            .create_monitor(NewMonitor {
                chain,
                target,
                start_block,
                end_block: command.end_block,
                filter,
            })
            .await
            .context("create monitor")
            .map(Into::into)
    }

    pub async fn list(&self, chain_id: Option<ChainId>) -> anyhow::Result<Vec<MonitorView>> {
        let chain = chain_id.map(Chain::new);
        Ok(self
            .storage
            .list_monitors(chain)
            .await
            .context("list monitors")?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub async fn get(&self, id: MonitorId) -> anyhow::Result<MonitorView> {
        self.storage.get_monitor(id).await.context("get monitor").map(Into::into)
    }

    pub async fn set_enabled(&self, id: MonitorId, enabled: bool) -> anyhow::Result<MonitorView> {
        self.storage
            .set_monitor_enabled(id, enabled)
            .await
            .context("set monitor enabled state")
            .map(Into::into)
    }

    pub async fn delete(&self, id: MonitorId) -> anyhow::Result<()> {
        self.storage.delete_monitor(id).await.context("delete monitor")
    }

    pub async fn results(
        &self,
        id: MonitorId,
        query: ResultQuery,
    ) -> anyhow::Result<Vec<MonitorResultView>> {
        let monitor = self.storage.get_monitor(id).await.context("get monitor for result query")?;
        Ok(self
            .storage
            .query_results(&monitor, query)
            .await
            .context("query monitor results")?
            .into_iter()
            .map(Into::into)
            .collect())
    }
}

pub fn preview_filter(command: PreviewFilter) -> anyhow::Result<FilterPreview> {
    let address = match &command.sample {
        filter::FilterSample::Call { to, .. } => *to,
        filter::FilterSample::Event { emitter, .. } => *emitter,
    };
    let target = match parse_target_signature(&command.signature)
        .map_err(|error| invalid(error.to_string()))?
    {
        TargetSpec::Call(spec) => {
            Target::Call(CallTarget { address, selector: spec.selector, inputs: spec.params })
        }
        TargetSpec::Event(spec) => {
            Target::Event(EventTarget { address, topic0: spec.topic0, params: spec.params })
        }
    };
    filter::preview(&target, command.filter, command.sample)
        .map_err(|error| invalid(error.to_string()))
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
