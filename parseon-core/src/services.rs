//! Application services invoked by HTTP handlers.
//!
//! [`ChainService`] and [`MonitorService`] wrap a [`Storage`] and
//! [`BlockSourceFactory`] and expose the use cases the API needs: chain CRUD
//! with endpoint validation, monitor CRUD with ABI parsing and optional
//! finalized-head defaulting, and result queries. [`preview_filter`] handles
//! the stateless filter-preview endpoint without creating a monitor.

use std::sync::Arc;

use anyhow::Context;

use crate::abi::{TargetSpec, parse_target_signature};
use crate::commands::{CreateChain, CreateMonitor, PreviewFilter, ResultQuery, UpdateChain};
use crate::filter::{self, FilterDefinition, FilterPreview};
use crate::ports::{
    BlockSourceFactory, ChainRecord, ChainUpdate, NewChain, NewMonitor, RegisteredChain, Storage,
};
use crate::supervisor::SupervisorHandle;
use crate::views::{ChainView, MonitorResultView, MonitorView};
use crate::{CallTarget, Chain, ChainId, EventTarget, MonitorId, Target, Url, worker};

/// Error returned when a command fails application-level validation (e.g.
/// an invalid ABI signature, an out-of-range block range, or an RPC endpoint
/// that returns the wrong chain ID).
///
/// The server maps this to `400 Bad Request` so handlers can downcast and
/// present the message to the caller.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct InvalidCommand(String);

fn invalid(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(InvalidCommand(message.into()))
}

/// Chain management service: create, list, get, update, and delete chains.
///
/// Each mutating method validates the RPC endpoint (if its URL is changing),
/// persists the change via [`crate::ports::ChainRepository`], and applies it
/// to the running supervisor: creations and enable/disable start and stop
/// workers, RPC URL changes rotate the live endpoint, and deletions retire
/// the worker before its data is removed — all without a restart. The
/// service is cheap to clone.
#[derive(Clone)]
pub struct ChainService {
    storage: Arc<dyn Storage>,
    sources: Arc<dyn BlockSourceFactory>,
    supervisor: SupervisorHandle,
}

impl ChainService {
    /// Creates a chain service over `storage` and `sources`, applying registry
    /// changes through `supervisor`.
    pub fn new(
        storage: Arc<dyn Storage>,
        sources: Arc<dyn BlockSourceFactory>,
        supervisor: SupervisorHandle,
    ) -> Self {
        Self { storage, sources, supervisor }
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
        let _mutation = self.supervisor.lock_mutations().await;
        let record = self
            .storage
            .create_chain(NewChain { chain, rpc_url: command.rpc_url, enabled: command.enabled })
            .await
            .context("create chain")?;
        self.supervisor.apply(registered(&record)).await;
        Ok(record.into())
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
        let _mutation = self.supervisor.lock_mutations().await;
        self.storage.get_chain(chain).await.context("get chain before update")?;
        let record = self
            .storage
            .update_chain(chain, ChainUpdate { rpc_url: command.rpc_url, enabled: command.enabled })
            .await
            .context("update chain")?;
        self.supervisor.apply(registered(&record)).await;
        Ok(record.into())
    }

    pub async fn delete(&self, chain_id: ChainId) -> anyhow::Result<()> {
        let chain = Chain::new(chain_id);
        let _mutation = self.supervisor.lock_mutations().await;
        // Stop the worker before deleting so no in-flight commit can race
        // the removal of the chain's monitors and result tables.
        self.supervisor.retire(chain.id).await;
        self.storage.delete_chain(chain).await.context("delete chain")
    }
}

/// Builds the supervisor-facing registration for a persisted chain record.
fn registered(record: &ChainRecord) -> RegisteredChain {
    RegisteredChain {
        chain: record.chain,
        rpc_url: record.rpc_url.clone(),
        enabled: record.enabled,
    }
}

/// Monitor management service: create, list, get, pause/resume, delete, and
/// query results.
///
/// Monitor creation parses the ABI signature, resolves an optional
/// `start_block` default to the chain's current finalized head, validates the
/// block range, compiles the optional filter, and forwards to
/// [`crate::ports::MonitorRepository`].
#[derive(Clone)]
pub struct MonitorService {
    storage: Arc<dyn Storage>,
    sources: Arc<dyn BlockSourceFactory>,
}

impl MonitorService {
    /// Creates a monitor service over `storage` and `sources`.
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

/// Evaluates a filter expression against a sample decoded result without
/// creating a monitor.
///
/// Parses `command.signature` into a target, compiles `command.filter`, decodes
/// `command.sample`'s parameters via the same ABI schema, and returns the
/// canonicalized expression together with the match result.
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
    use std::sync::Arc;
    use std::time::Duration;

    use super::{ChainService, validate_range};
    use crate::commands::{CreateChain, UpdateChain};
    use crate::status::WorkerState;
    use crate::testkit::{TestContext, url, wait_for};

    #[test]
    fn validates_monitor_ranges() {
        assert!(validate_range(0, None).is_ok());
        assert!(validate_range(10, Some(10)).is_ok());
        assert!(validate_range(10, Some(9)).is_err());
    }

    fn service(context: &TestContext) -> ChainService {
        ChainService::new(
            context.storage.clone(),
            Arc::new(context.sources.clone()),
            context.handle(),
        )
    }

    fn create_chain(enabled: bool) -> CreateChain {
        CreateChain { rpc_url: url("http://localhost:8545"), enabled }
    }

    #[tokio::test]
    async fn create_starts_a_worker_for_an_enabled_chain() {
        let context = TestContext::new();
        let service = service(&context);

        let view = service.create(create_chain(true)).await.unwrap();

        assert_eq!(view.chain_id, 1);
        wait_for(|| context.statuses.snapshot()[0].worker_state == WorkerState::Running).await;
        let polls = context.storage.load_monitor_calls();
        wait_for(|| context.storage.load_monitor_calls() > polls).await;
    }

    #[tokio::test]
    async fn create_registers_a_disabled_chain_without_a_worker() {
        let context = TestContext::new();
        let service = service(&context);

        let view = service.create(create_chain(false)).await.unwrap();

        assert_eq!(view.chain_id, 1);
        assert!(!view.enabled);
        assert_eq!(context.statuses.snapshot()[0].worker_state, WorkerState::Disabled);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(context.storage.load_monitor_calls(), 0, "a disabled chain must not poll");
    }

    #[tokio::test]
    async fn update_applies_enable_disable_and_url_rotation_without_restart() {
        let context = TestContext::new();
        let service = service(&context);
        service.create(create_chain(true)).await.unwrap();
        wait_for(|| context.statuses.snapshot()[0].worker_state == WorkerState::Running).await;

        service
            .update(1, UpdateChain { rpc_url: Some(url("http://localhost:9545")), enabled: None })
            .await
            .unwrap();
        assert_eq!(context.sources.rotations(), vec![url("http://localhost:9545")]);
        assert!(context.statuses.snapshot()[0].enabled);

        service.update(1, UpdateChain { rpc_url: None, enabled: Some(false) }).await.unwrap();
        assert_eq!(context.statuses.snapshot()[0].worker_state, WorkerState::Disabled);
        let polls = context.storage.load_monitor_calls();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(context.storage.load_monitor_calls(), polls, "disabled chain must not poll");

        service.update(1, UpdateChain { rpc_url: None, enabled: Some(true) }).await.unwrap();
        wait_for(|| context.statuses.snapshot()[0].worker_state == WorkerState::Running).await;
    }

    #[tokio::test]
    async fn delete_retires_the_worker_before_removing_the_chain() {
        let context = TestContext::new();
        let service = service(&context);
        service.create(create_chain(true)).await.unwrap();
        wait_for(|| context.statuses.snapshot()[0].worker_state == WorkerState::Running).await;

        service.delete(1).await.unwrap();

        assert_eq!(context.storage.deleted_chains(), vec![1]);
        assert!(context.statuses.snapshot().is_empty(), "delete removes the status");
        let polls = context.storage.load_monitor_calls();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            context.storage.load_monitor_calls(),
            polls,
            "the retired worker must not poll after deletion"
        );
    }

    #[tokio::test]
    async fn concurrent_update_and_delete_remain_one_ordered_mutation() {
        let context = TestContext::new();
        let service = service(&context);
        service.create(create_chain(true)).await.unwrap();
        wait_for(|| context.statuses.snapshot()[0].worker_state == WorkerState::Running).await;

        let pause = context.storage.pause_next_update();
        let updating = {
            let service = service.clone();
            tokio::spawn(async move {
                service.update(1, UpdateChain { rpc_url: None, enabled: Some(false) }).await
            })
        };
        pause.wait_until_reached().await;

        let deleting = {
            let service = service.clone();
            tokio::spawn(async move { service.delete(1).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert!(context.storage.deleted_chains().is_empty(), "delete must wait for update apply");
        assert_eq!(context.statuses.snapshot()[0].worker_state, WorkerState::Running);

        pause.resume();
        updating.await.unwrap().unwrap();
        deleting.await.unwrap().unwrap();

        assert_eq!(context.storage.deleted_chains(), vec![1]);
        assert!(context.statuses.snapshot().is_empty());
    }
}
