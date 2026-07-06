use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::Chain;
use super::ports::{BlockSource, BlockSourceFactory, ChainRegistry, RegisteredChain, Storage};
use super::status::{ChainStatus, RuntimeStatus};
use super::worker::{self, WorkerConfig};
use crate::metrics::Metrics;

#[derive(Debug, Clone, Copy)]
pub struct SupervisorConfig {
    pub batch_size: i64,
    pub poll_interval: Duration,
    pub block_cache_size: usize,
    pub block_concurrency: usize,
    pub db_write_concurrency: usize,
}

struct WorkerRuntime {
    rpc_url: String,
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

pub struct Supervisor {
    config: SupervisorConfig,
    registry: Arc<dyn ChainRegistry>,
    storage: Arc<dyn Storage>,
    source_factory: Arc<dyn BlockSourceFactory>,
    statuses: RuntimeStatus,
    metrics: Metrics,
    db_writes: Arc<Semaphore>,
    workers: HashMap<i64, WorkerRuntime>,
}

impl Supervisor {
    pub fn new(
        config: SupervisorConfig,
        registry: Arc<dyn ChainRegistry>,
        storage: Arc<dyn Storage>,
        source_factory: Arc<dyn BlockSourceFactory>,
        statuses: RuntimeStatus,
        metrics: Metrics,
    ) -> Self {
        let db_write_concurrency = config.db_write_concurrency.max(1);
        Self {
            config,
            registry,
            storage,
            source_factory,
            statuses,
            metrics,
            db_writes: Arc::new(Semaphore::new(db_write_concurrency)),
            workers: HashMap::new(),
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) {
        tracing::info!("chain supervisor started");
        loop {
            if cancel.is_cancelled() {
                break;
            }
            if let Err(error) = self.reconcile().await {
                tracing::warn!(error = %error, "chain registry reconciliation failed");
            }
            tokio::select! {
                _ = tokio::time::sleep(self.config.poll_interval) => {}
                _ = cancel.cancelled() => break,
            }
        }
        self.shutdown().await;
        tracing::info!("chain supervisor stopped");
    }

    pub async fn reconcile(&mut self) -> anyhow::Result<()> {
        let chains = self.registry.list_registered_chains().await?;
        let registered = chains
            .iter()
            .map(|registered| registered.chain.id)
            .collect::<HashSet<_>>();

        let removed = self
            .workers
            .keys()
            .filter(|chain_id| !registered.contains(chain_id))
            .copied()
            .collect::<Vec<_>>();
        for chain_id in removed {
            self.stop_worker(chain_id).await;
        }
        for status in self.statuses.snapshot() {
            if !registered.contains(&status.chain_id) {
                self.statuses.remove(status.chain_id);
            }
        }

        for registered in chains {
            if !registered.enabled {
                self.stop_worker(registered.chain.id).await;
                self.statuses
                    .replace(ChainStatus::disabled(registered.chain.id));
                continue;
            }

            let unchanged = self
                .workers
                .get(&registered.chain.id)
                .is_some_and(|runtime| {
                    runtime.rpc_url == registered.rpc_url && !runtime.handle.is_finished()
                });
            if unchanged {
                continue;
            }

            match self.prepare_source(&registered).await {
                Ok((source, finalized_head)) => {
                    self.stop_worker(registered.chain.id).await;
                    self.start_worker(registered, source, finalized_head);
                }
                Err(message) => {
                    self.statuses
                        .replace(ChainStatus::degraded(registered.chain.id, message));
                }
            }
        }
        Ok(())
    }

    async fn prepare_source(
        &self,
        registered: &RegisteredChain,
    ) -> Result<(Arc<dyn BlockSource>, i64), &'static str> {
        let source = self
            .source_factory
            .connect(&registered.rpc_url)
            .map_err(|_| "RPC endpoint configuration is invalid")?;
        let probe = worker::probe_source(source.as_ref())
            .await
            .map_err(|_| "RPC endpoint validation failed")?;
        let discovered = i64::try_from(probe.chain_id)
            .map_err(|_| "RPC endpoint returned an unsupported chain ID")?;
        if discovered != registered.chain.id {
            return Err("RPC endpoint returned a different chain ID");
        }
        Ok((source, probe.finalized_head))
    }

    fn start_worker(
        &mut self,
        registered: RegisteredChain,
        source: Arc<dyn BlockSource>,
        finalized_head: i64,
    ) {
        let chain = registered.chain;
        let status = ChainStatus::starting(chain.id, Some(finalized_head));
        self.statuses.replace(status.clone());
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(worker::run(
            WorkerConfig {
                chain,
                batch_size: self.config.batch_size,
                block_concurrency: self.config.block_concurrency,
                poll_interval: self.config.poll_interval,
            },
            self.storage.clone(),
            source,
            Arc::new(crate::cache::MemoryBlockCache::new(
                self.config.block_cache_size,
            )),
            self.db_writes.clone(),
            self.metrics.clone(),
            status,
            cancel.clone(),
        ));
        self.workers.insert(
            chain.id,
            WorkerRuntime {
                rpc_url: registered.rpc_url,
                cancel,
                handle,
            },
        );
    }

    async fn stop_worker(&mut self, chain_id: i64) {
        if let Some(runtime) = self.workers.remove(&chain_id) {
            runtime.cancel.cancel();
            runtime.handle.abort();
            let _ = runtime.handle.await;
        }
    }

    async fn shutdown(&mut self) {
        let chain_ids = self.workers.keys().copied().collect::<Vec<_>>();
        for chain_id in chain_ids {
            self.stop_worker(chain_id).await;
        }
    }

    #[cfg(test)]
    fn has_worker(&self, chain_id: i64) -> bool {
        self.workers.contains_key(&chain_id)
    }
}

pub async fn validate_source(
    factory: &dyn BlockSourceFactory,
    rpc_url: &str,
    expected_chain: Option<Chain>,
) -> Result<(Chain, i64), &'static str> {
    let source = factory
        .connect(rpc_url)
        .map_err(|_| "RPC endpoint configuration is invalid")?;
    let probe = worker::probe_source(source.as_ref())
        .await
        .map_err(|_| "RPC endpoint validation failed")?;
    let chain_id = i64::try_from(probe.chain_id)
        .map_err(|_| "RPC endpoint returned an unsupported chain ID")?;
    let chain = Chain::new(chain_id).map_err(|_| "RPC endpoint returned an invalid chain ID")?;
    if expected_chain.is_some_and(|expected| expected != chain) {
        return Err("RPC endpoint returned a different chain ID");
    }
    Ok((chain, probe.finalized_head))
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, RwLock};

    use async_trait::async_trait;

    use super::*;
    use crate::core::ports::{BlockCommit, BlockSource};
    use crate::core::{BlockTransaction, ExecutedTransaction, SourceBlock};

    #[derive(Default)]
    struct FakeRegistry(RwLock<Vec<RegisteredChain>>);

    #[async_trait]
    impl ChainRegistry for FakeRegistry {
        async fn list_registered_chains(&self) -> anyhow::Result<Vec<RegisteredChain>> {
            Ok(self.0.read().unwrap().clone())
        }
    }

    struct EmptyStorage;

    #[async_trait]
    impl Storage for EmptyStorage {
        async fn load_monitors(
            &self,
            _chain: Chain,
        ) -> anyhow::Result<Vec<super::super::monitor::Monitor>> {
            Ok(Vec::new())
        }

        async fn commit_block(&self, _commit: BlockCommit) -> anyhow::Result<usize> {
            Ok(0)
        }
    }

    struct FakeSource {
        chain_id: u64,
        fail: bool,
    }

    #[async_trait]
    impl BlockSource for FakeSource {
        async fn chain_id(&self) -> anyhow::Result<u64> {
            if self.fail {
                anyhow::bail!("unavailable")
            }
            Ok(self.chain_id)
        }

        async fn finalized_head(&self) -> anyhow::Result<i64> {
            if self.fail {
                anyhow::bail!("unavailable")
            }
            Ok(100)
        }

        async fn fetch_block(&self, block_number: i64) -> anyhow::Result<SourceBlock> {
            Ok(SourceBlock {
                number: block_number,
                transactions: Vec::new(),
            })
        }

        async fn fetch_executed_transactions(
            &self,
            _block: &SourceBlock,
            _transactions: &[BlockTransaction],
        ) -> anyhow::Result<Vec<ExecutedTransaction>> {
            Ok(Vec::new())
        }
    }

    #[derive(Default)]
    struct FakeFactory(Mutex<HashMap<String, (u64, bool)>>);

    impl FakeFactory {
        fn set(&self, url: &str, chain_id: u64, fail: bool) {
            self.0.lock().unwrap().insert(url.into(), (chain_id, fail));
        }
    }

    impl BlockSourceFactory for FakeFactory {
        fn connect(&self, rpc_url: &str) -> anyhow::Result<Arc<dyn BlockSource>> {
            let (chain_id, fail) = self
                .0
                .lock()
                .unwrap()
                .get(rpc_url)
                .copied()
                .ok_or_else(|| anyhow::anyhow!("unknown endpoint"))?;
            Ok(Arc::new(FakeSource { chain_id, fail }))
        }
    }

    fn registered(chain_id: i64, url: &str, enabled: bool) -> RegisteredChain {
        RegisteredChain {
            chain: Chain::new(chain_id).unwrap(),
            rpc_url: url.into(),
            enabled,
        }
    }

    fn supervisor(
        registry: Arc<FakeRegistry>,
        factory: Arc<FakeFactory>,
        statuses: RuntimeStatus,
    ) -> Supervisor {
        Supervisor::new(
            SupervisorConfig {
                batch_size: 1,
                poll_interval: Duration::from_secs(60),
                block_cache_size: 1,
                block_concurrency: 1,
                db_write_concurrency: 1,
            },
            registry,
            Arc::new(EmptyStorage),
            factory,
            statuses,
            Metrics::default(),
        )
    }

    #[tokio::test]
    async fn reconciles_start_disable_reenable_replace_and_delete() {
        let registry = Arc::new(FakeRegistry::default());
        let factory = Arc::new(FakeFactory::default());
        factory.set("first", 1, false);
        factory.set("replacement", 1, false);
        let statuses = RuntimeStatus::default();
        let mut supervisor = supervisor(registry.clone(), factory, statuses.clone());

        *registry.0.write().unwrap() = vec![registered(1, "first", true)];
        supervisor.reconcile().await.unwrap();
        assert!(supervisor.has_worker(1));

        *registry.0.write().unwrap() = vec![registered(1, "first", false)];
        supervisor.reconcile().await.unwrap();
        assert!(!supervisor.has_worker(1));
        assert_eq!(
            statuses.snapshot()[0].worker_state,
            super::super::status::WorkerState::Disabled
        );

        *registry.0.write().unwrap() = vec![registered(1, "replacement", true)];
        supervisor.reconcile().await.unwrap();
        assert!(supervisor.has_worker(1));

        registry.0.write().unwrap().clear();
        supervisor.reconcile().await.unwrap();
        assert!(!supervisor.has_worker(1));
        assert!(statuses.snapshot().is_empty());
    }

    #[tokio::test]
    async fn isolates_failed_chains_and_retries() {
        let registry = Arc::new(FakeRegistry::default());
        *registry.0.write().unwrap() = vec![
            registered(1, "healthy", true),
            registered(2, "failing", true),
        ];
        let factory = Arc::new(FakeFactory::default());
        factory.set("healthy", 1, false);
        factory.set("failing", 2, true);
        let statuses = RuntimeStatus::default();
        let mut supervisor = supervisor(registry, factory.clone(), statuses.clone());

        supervisor.reconcile().await.unwrap();
        assert!(supervisor.has_worker(1));
        assert!(!supervisor.has_worker(2));
        assert_eq!(
            statuses.snapshot()[1].worker_state,
            super::super::status::WorkerState::Degraded
        );

        factory.set("failing", 2, false);
        supervisor.reconcile().await.unwrap();
        assert!(supervisor.has_worker(2));
    }

    #[tokio::test]
    async fn rejects_replacement_for_another_chain_without_stopping_worker() {
        let registry = Arc::new(FakeRegistry::default());
        let factory = Arc::new(FakeFactory::default());
        factory.set("first", 1, false);
        factory.set("wrong", 2, false);
        let statuses = RuntimeStatus::default();
        let mut supervisor = supervisor(registry.clone(), factory, statuses);

        *registry.0.write().unwrap() = vec![registered(1, "first", true)];
        supervisor.reconcile().await.unwrap();
        *registry.0.write().unwrap() = vec![registered(1, "wrong", true)];
        supervisor.reconcile().await.unwrap();
        assert!(supervisor.has_worker(1));
        assert_eq!(supervisor.workers[&1].rpc_url, "first");
    }
}
