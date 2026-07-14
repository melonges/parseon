use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::ports::{
    BlockCacheFactory, BlockSource, BlockSourceFactory, IndexStorage, RegisteredChain, Telemetry,
};
use super::status::{ChainStatus, RuntimeStatus};
use super::worker::{self, WorkerConfig};
use super::{BlockNumber, ChainId};

#[derive(Debug, Clone, Copy)]
pub struct SupervisorConfig {
    pub batch_size: NonZeroU64,
    pub poll_interval: Duration,
    pub block_concurrency: NonZeroUsize,
    pub db_write_concurrency: NonZeroUsize,
}

struct WorkerRuntime {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

pub struct Supervisor {
    config: SupervisorConfig,
    chains: Vec<RegisteredChain>,
    storage: Arc<dyn IndexStorage>,
    source_factory: Arc<dyn BlockSourceFactory>,
    cache_factory: Arc<dyn BlockCacheFactory>,
    statuses: RuntimeStatus,
    telemetry: Arc<dyn Telemetry>,
    db_writes: Arc<Semaphore>,
    workers: HashMap<ChainId, WorkerRuntime>,
}

impl Supervisor {
    pub fn new(
        config: SupervisorConfig,
        chains: Vec<RegisteredChain>,
        storage: Arc<dyn IndexStorage>,
        source_factory: Arc<dyn BlockSourceFactory>,
        cache_factory: Arc<dyn BlockCacheFactory>,
        statuses: RuntimeStatus,
        telemetry: Arc<dyn Telemetry>,
    ) -> Self {
        let db_write_concurrency = config.db_write_concurrency.get();
        Self {
            config,
            chains,
            storage,
            source_factory,
            cache_factory,
            statuses,
            telemetry,
            db_writes: Arc::new(Semaphore::new(db_write_concurrency)),
            workers: HashMap::new(),
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) {
        tracing::info!("chain supervisor started");
        for registered in std::mem::take(&mut self.chains) {
            if !registered.enabled {
                self.statuses.replace(ChainStatus::disabled(registered.chain.id));
                continue;
            }
            match self.prepare_source(&registered).await {
                Ok((source, finalized_head)) => {
                    self.start_worker(registered, source, finalized_head)
                }
                Err(message) => {
                    self.statuses.replace(ChainStatus::degraded(registered.chain.id, message));
                }
            }
        }
        cancel.cancelled().await;
        self.shutdown().await;
        tracing::info!("chain supervisor stopped");
    }

    async fn prepare_source(
        &self,
        registered: &RegisteredChain,
    ) -> Result<(Arc<dyn BlockSource>, BlockNumber), &'static str> {
        let source = self
            .source_factory
            .connect(&registered.rpc_url)
            .map_err(|_| "RPC endpoint configuration is invalid")?;
        let probe = worker::probe_source(source.as_ref())
            .await
            .map_err(|_| "RPC endpoint validation failed")?;
        if probe.chain_id != registered.chain.id {
            return Err("RPC endpoint returned a different chain ID");
        }
        Ok((source, probe.finalized_head))
    }

    fn start_worker(
        &mut self,
        registered: RegisteredChain,
        source: Arc<dyn BlockSource>,
        finalized_head: BlockNumber,
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
            self.cache_factory.create(),
            self.db_writes.clone(),
            self.telemetry.clone(),
            status,
            cancel.clone(),
        ));
        self.workers.insert(chain.id, WorkerRuntime { cancel, handle });
    }

    async fn stop_worker(&mut self, chain_id: ChainId) {
        if let Some(runtime) = self.workers.remove(&chain_id) {
            runtime.cancel.cancel();
            runtime.handle.abort();
            drop(runtime.handle.await);
        }
    }

    async fn shutdown(&mut self) {
        let chain_ids = self.workers.keys().copied().collect::<Vec<_>>();
        for chain_id in chain_ids {
            self.stop_worker(chain_id).await;
        }
    }
}
