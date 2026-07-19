use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::ports::{
    BlockCacheFactory, BlockSource, BlockSourceFactory, RegisteredChain, Sink, Storage, Telemetry,
};
use super::status::{ChainStatus, RuntimeStatus};
use super::worker::{self, WorkerConfig, WorkerDependencies};
use super::{BlockNumber, ChainId};

/// Supervisor configuration: indexing batch and concurrency limits plus the
/// poll interval.
#[derive(Debug, Clone, Copy)]
pub struct SupervisorConfig {
    /// Maximum number of blocks processed per monitor per poll.
    pub batch_size: NonZeroU64,
    /// Delay between polls.
    pub poll_interval: Duration,
    /// Maximum number of blocks prepared concurrently within one poll, per
    /// worker.
    pub block_concurrency: NonZeroUsize,
    /// Process-wide limit on concurrent storage commits across all workers.
    pub storage_write_concurrency: NonZeroUsize,
}

struct WorkerRuntime {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

async fn shutdown_workers(workers: HashMap<ChainId, WorkerRuntime>) {
    for runtime in workers.values() {
        runtime.cancel.cancel();
    }

    for (chain_id, runtime) in workers {
        if let Err(error) = runtime.handle.await {
            tracing::error!(
                chain_id,
                error = %error,
                "worker task failed during shutdown"
            );
        }
    }
}

/// The supervisor: owns the chain registry, shared dependencies, and the
/// per-worker cancellation handles.
pub struct Supervisor {
    config: SupervisorConfig,
    chains: Vec<RegisteredChain>,
    storage: Arc<dyn Storage>,
    sink: Arc<dyn Sink>,
    source_factory: Arc<dyn BlockSourceFactory>,
    cache_factory: Arc<dyn BlockCacheFactory>,
    statuses: RuntimeStatus,
    telemetry: Arc<dyn Telemetry>,
    storage_writes: Arc<Semaphore>,
    workers: HashMap<ChainId, WorkerRuntime>,
}

/// Wiring bundle for [`Supervisor::new`]: all shared dependencies the
/// supervisor clones into each worker.
pub struct SupervisorDependencies {
    /// Composite storage adapter.
    pub storage: Arc<dyn Storage>,
    /// Optional post-commit sink.
    pub sink: Arc<dyn Sink>,
    /// Block source factory used to validate endpoints and connect workers.
    pub source_factory: Arc<dyn BlockSourceFactory>,
    /// Per-worker block cache factory.
    pub cache_factory: Arc<dyn BlockCacheFactory>,
    /// Runtime status registry, shared with the HTTP `/status` handler.
    pub statuses: RuntimeStatus,
    /// Telemetry collector.
    pub telemetry: Arc<dyn Telemetry>,
}

impl Supervisor {
    pub fn new(
        config: SupervisorConfig,
        chains: Vec<RegisteredChain>,
        dependencies: SupervisorDependencies,
    ) -> Self {
        let storage_write_concurrency = config.storage_write_concurrency.get();
        Self {
            config,
            chains,
            storage: dependencies.storage,
            sink: dependencies.sink,
            source_factory: dependencies.source_factory,
            cache_factory: dependencies.cache_factory,
            statuses: dependencies.statuses,
            telemetry: dependencies.telemetry,
            storage_writes: Arc::new(Semaphore::new(storage_write_concurrency)),
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
            let prepared = tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                prepared = self.prepare_source(&registered) => prepared,
            };
            match prepared {
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
            WorkerDependencies {
                storage: self.storage.clone(),
                source,
                cache: self.cache_factory.create(),
                storage_writes: self.storage_writes.clone(),
                sink: self.sink.clone(),
                telemetry: self.telemetry.clone(),
            },
            status,
            cancel.clone(),
        ));
        self.workers.insert(chain.id, WorkerRuntime { cancel, handle });
    }

    async fn shutdown(&mut self) {
        let workers = std::mem::take(&mut self.workers);
        shutdown_workers(workers).await;
    }
}

#[cfg(test)]
mod tests {
    //! The per-chain worker supervisor.
    //!
    //! At startup, the supervisor reads the chain registry, validates each
    //! enabled chain's RPC endpoint, and spawns one worker per chain. Disabled
    //! chains get a [`ChainStatus::disabled`] record but no worker. The supervisor
    //! runs until its cancellation token fires, then cancels every worker and
    //! awaits them all before returning.

    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio_util::sync::CancellationToken;

    use super::{WorkerRuntime, shutdown_workers};

    #[tokio::test]
    async fn shutdown_cancels_all_workers_before_awaiting_them() {
        let first_cancel = CancellationToken::new();
        let second_cancel = CancellationToken::new();
        let observed_collective_cancellation = Arc::new(AtomicUsize::new(0));

        let first_handle = {
            let first_cancel = first_cancel.clone();
            let second_cancel = second_cancel.clone();
            let observed = observed_collective_cancellation.clone();
            tokio::spawn(async move {
                first_cancel.cancelled().await;
                if second_cancel.is_cancelled() {
                    observed.fetch_add(1, Ordering::Relaxed);
                }
            })
        };
        let second_handle = {
            let first_cancel = first_cancel.clone();
            let second_cancel = second_cancel.clone();
            let observed = observed_collective_cancellation.clone();
            tokio::spawn(async move {
                second_cancel.cancelled().await;
                if first_cancel.is_cancelled() {
                    observed.fetch_add(1, Ordering::Relaxed);
                }
            })
        };

        let workers = HashMap::from([
            (1, WorkerRuntime { cancel: first_cancel, handle: first_handle }),
            (2, WorkerRuntime { cancel: second_cancel, handle: second_handle }),
        ]);

        shutdown_workers(workers).await;

        assert_eq!(observed_collective_cancellation.load(Ordering::Relaxed), 2);
    }
}
