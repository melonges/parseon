//! The per-chain worker supervisor.
//!
//! The supervisor owns the runtime worker set. At startup it applies the
//! persisted chain registry; afterwards the shared [`SupervisorHandle`]
//! applies registry changes without a restart: chain creation and
//! enable/disable start and stop workers, RPC URL changes rotate the worker's
//! endpoint in place (restarting the worker only when the source cannot
//! rotate), and deletion retires the worker and its status. Disabled chains
//! get a [`ChainStatus::disabled`] record but no worker. The supervisor runs
//! until its cancellation token fires, then cancels every worker and awaits
//! them all before returning.

use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
use tokio::sync::{Mutex, MutexGuard, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::ports::{
    BlockCacheFactory, BlockSource, BlockSourceFactory, RegisteredChain, Sink, Storage, Telemetry,
};
use super::status::{ChainStatus, RuntimeStatus, WorkerState};
use super::worker::{self, WorkerConfig, WorkerDependencies};
use super::{BlockNumber, ChainId, Url};

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
    /// Number of latest blocks required before promotion.
    pub confirmations: NonZeroU64,
    /// Maximum canonical history retained for reorg recovery.
    pub rollback_retention: NonZeroU64,
}

struct WorkerRuntime {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
    rpc_url: Url,
    source: Arc<dyn BlockSource>,
}

async fn stop_worker(runtime: WorkerRuntime) {
    runtime.cancel.cancel();
    if let Err(error) = runtime.handle.await {
        tracing::error!(error = %error, "worker task failed while stopping");
    }
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

#[derive(Default)]
struct ControlPlane {
    closed: bool,
    workers: HashMap<ChainId, WorkerRuntime>,
}

/// Shared supervisor state: dependencies cloned into each worker plus the
/// live worker set. The control mutex serializes every runtime operation, and
/// the mutation mutex lets chain services keep persistence and runtime changes
/// in one ordered critical section.
struct SupervisorShared {
    config: SupervisorConfig,
    storage: Arc<dyn Storage>,
    sink: Arc<dyn Sink>,
    source_factory: Arc<dyn BlockSourceFactory>,
    cache_factory: Arc<dyn BlockCacheFactory>,
    statuses: RuntimeStatus,
    telemetry: Arc<dyn Telemetry>,
    storage_writes: Arc<Semaphore>,
    mutations: Mutex<()>,
    control: Mutex<ControlPlane>,
}

/// Cloneable control handle over the live worker set.
///
/// Application services use this to apply chain registry changes while the
/// process runs. All methods are idempotent: applying an already-matching
/// registration and retiring an absent chain are no-ops.
#[derive(Clone)]
pub struct SupervisorHandle {
    shared: Arc<SupervisorShared>,
}

impl SupervisorHandle {
    fn new(config: SupervisorConfig, dependencies: SupervisorDependencies) -> Self {
        let storage_write_concurrency = config.storage_write_concurrency.get();
        Self {
            shared: Arc::new(SupervisorShared {
                config,
                storage: dependencies.storage,
                sink: dependencies.sink,
                source_factory: dependencies.source_factory,
                cache_factory: dependencies.cache_factory,
                statuses: dependencies.statuses,
                telemetry: dependencies.telemetry,
                storage_writes: Arc::new(Semaphore::new(storage_write_concurrency)),
                mutations: Mutex::new(()),
                control: Mutex::new(ControlPlane::default()),
            }),
        }
    }

    /// Acquires the registry-mutation guard shared by startup reconciliation
    /// and every chain service instance.
    pub(crate) async fn lock_mutations(&self) -> MutexGuard<'_, ()> {
        self.shared.mutations.lock().await
    }

    /// Reconciles the runtime with `registered`: starts the worker for an
    /// enabled chain that has none, rotates the RPC URL of a running worker
    /// whose URL changed (restarting it when the source cannot rotate in
    /// place), and stops the worker of a disabled chain.
    pub async fn apply(&self, registered: RegisteredChain) {
        let mut control = self.shared.control.lock().await;
        if control.closed {
            return;
        }
        let workers = &mut control.workers;
        let chain_id = registered.chain.id;
        match (registered.enabled, workers.contains_key(&chain_id)) {
            (true, true) => {
                let runtime = workers.get_mut(&chain_id).expect("worker is running");
                if runtime.rpc_url == registered.rpc_url {
                    return;
                }
                match runtime.source.set_rpc_url(&registered.rpc_url) {
                    Ok(()) => {
                        runtime.rpc_url = registered.rpc_url;
                        tracing::info!(chain_id, "worker RPC endpoint rotated");
                    }
                    Err(error) => {
                        tracing::warn!(
                            chain_id,
                            error = %error,
                            "in-place RPC rotation failed; restarting worker"
                        );
                        let runtime = workers.remove(&chain_id).expect("worker is running");
                        stop_worker(runtime).await;
                        self.start_worker(workers, registered).await;
                    }
                }
            }
            (true, false) => self.start_worker(workers, registered).await,
            (false, true) => {
                let runtime = workers.remove(&chain_id).expect("worker is running");
                stop_worker(runtime).await;
                self.shared.telemetry.set_worker_state(chain_id, "disabled");
                self.shared.statuses.replace(ChainStatus::disabled(chain_id));
            }
            (false, false) => {
                self.shared.telemetry.set_worker_state(chain_id, "disabled");
                self.shared.statuses.replace(ChainStatus::disabled(chain_id));
            }
        }
    }

    /// Stops the chain's worker (if any) and removes its status. Chain
    /// deletion calls this before removing persisted data so no commit can
    /// race the delete.
    pub async fn retire(&self, chain_id: ChainId) {
        let mut control = self.shared.control.lock().await;
        if let Some(runtime) = control.workers.remove(&chain_id) {
            stop_worker(runtime).await;
        }
        self.shared.statuses.remove(chain_id);
    }

    /// Closes the control plane, then cancels and awaits every worker.
    /// Registrations applied after closure cannot spawn replacement tasks.
    pub async fn shutdown(&self) {
        let workers = {
            let mut control = self.shared.control.lock().await;
            control.closed = true;
            std::mem::take(&mut control.workers)
        };
        shutdown_workers(workers).await;
    }

    async fn prepare_source(
        &self,
        registered: &RegisteredChain,
    ) -> Result<(Arc<dyn BlockSource>, BlockNumber), &'static str> {
        let source = self
            .shared
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

    async fn start_worker(
        &self,
        workers: &mut HashMap<ChainId, WorkerRuntime>,
        registered: RegisteredChain,
    ) {
        let chain = registered.chain;
        let (source, finalized_head) = match self.prepare_source(&registered).await {
            Ok(prepared) => prepared,
            Err(message) => {
                tracing::warn!(chain_id = chain.id, error = message, "worker start failed");
                self.shared.telemetry.set_worker_state(chain.id, "degraded");
                self.shared.statuses.replace(ChainStatus::degraded(chain.id, message));
                return;
            }
        };
        let status = ChainStatus::starting(chain.id, Some(finalized_head));
        self.shared.statuses.replace(status.clone());
        let cancel = CancellationToken::new();
        let worker_config = WorkerConfig {
            chain,
            batch_size: self.shared.config.batch_size,
            block_concurrency: self.shared.config.block_concurrency,
            poll_interval: self.shared.config.poll_interval,
            confirmations: self.shared.config.confirmations,
            rollback_retention: self.shared.config.rollback_retention,
        };
        let worker_dependencies = WorkerDependencies {
            storage: self.shared.storage.clone(),
            source: source.clone(),
            cache: self.shared.cache_factory.create(),
            storage_writes: self.shared.storage_writes.clone(),
            sink: self.shared.sink.clone(),
            telemetry: self.shared.telemetry.clone(),
        };
        let worker_cancel = cancel.clone();
        let task_status = status.clone();
        let task_cancel = cancel.clone();
        let task_telemetry = self.shared.telemetry.clone();
        let chain_id = chain.id;
        let handle = tokio::spawn(async move {
            let result = AssertUnwindSafe(worker::run(
                worker_config,
                worker_dependencies,
                status,
                worker_cancel,
            ))
            .catch_unwind()
            .await;
            if task_cancel.is_cancelled() {
                return;
            }
            if task_status.snapshot().worker_state == WorkerState::Blocked {
                return;
            }
            if let Err(panic) = result {
                tracing::error!(chain_id, ?panic, "worker task panicked");
            }
            task_status.record_task_exit();
            task_telemetry.set_worker_state(chain_id, "exited");
        });
        workers.insert(
            chain.id,
            WorkerRuntime { cancel, handle, rpc_url: registered.rpc_url, source },
        );
        tracing::info!(chain_id = chain.id, "worker spawned");
    }
}

/// The supervisor: applies the startup chain registry snapshot, then parks
/// until cancellation and shuts every worker down. Clone its
/// [`SupervisorHandle`] before running to apply registry changes live.
pub struct Supervisor {
    chains: Vec<RegisteredChain>,
    handle: SupervisorHandle,
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
        Self { chains, handle: SupervisorHandle::new(config, dependencies) }
    }

    /// Returns the control handle that applies chain registry changes to the
    /// live worker set.
    pub fn handle(&self) -> SupervisorHandle {
        self.handle.clone()
    }

    /// Applies the captured startup registry before the control handle is
    /// exposed to request-serving code. Chain mutations share the same guard,
    /// so a caller that already owns a handle waits for this snapshot to finish.
    pub async fn reconcile(&mut self, cancel: &CancellationToken) {
        let _mutation = self.handle.lock_mutations().await;
        for registered in std::mem::take(&mut self.chains) {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = self.handle.apply(registered) => {}
            }
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) {
        tracing::info!("chain supervisor started");
        self.reconcile(&cancel).await;
        cancel.cancelled().await;
        self.handle.shutdown().await;
        tracing::info!("chain supervisor stopped");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio_util::sync::CancellationToken;

    use super::{Supervisor, SupervisorHandle, WorkerRuntime, shutdown_workers, stop_worker};
    use crate::ports::{BlockSourceFactory, RegisteredChain};
    use crate::status::WorkerState;
    use crate::testkit::{TestContext, url, wait_for};
    use crate::{Chain, ChainId};

    fn registered(chain_id: ChainId, rpc_url: &str, enabled: bool) -> RegisteredChain {
        RegisteredChain { chain: Chain::new(chain_id), rpc_url: url(rpc_url), enabled }
    }

    async fn running_worker_count(handle: &SupervisorHandle) -> usize {
        handle.shared.control.lock().await.workers.len()
    }

    fn test_runtime(context: &TestContext, cancel: CancellationToken) -> WorkerRuntime {
        WorkerRuntime {
            cancel: cancel.clone(),
            handle: tokio::spawn(async move { cancel.cancelled().await }),
            rpc_url: url("http://localhost:8545"),
            source: context
                .sources
                .connect(&url("http://localhost:8545"))
                .expect("test source connects"),
        }
    }

    #[tokio::test]
    async fn shutdown_cancels_all_workers_before_awaiting_them() {
        let first_cancel = CancellationToken::new();
        let second_cancel = CancellationToken::new();
        let observed_collective_cancellation = Arc::new(AtomicUsize::new(0));

        let spawn_probe = |cancel: &CancellationToken, other: &CancellationToken| {
            let cancel = cancel.clone();
            let other = other.clone();
            let observed = observed_collective_cancellation.clone();
            tokio::spawn(async move {
                cancel.cancelled().await;
                if other.is_cancelled() {
                    observed.fetch_add(1, Ordering::Relaxed);
                }
            })
        };

        let context = TestContext::new();
        let dummy_source = || {
            context.sources.connect(&url("http://localhost:8545")).expect("test source connects")
        };
        let workers = HashMap::from([
            (
                1,
                WorkerRuntime {
                    cancel: first_cancel.clone(),
                    handle: spawn_probe(&first_cancel, &second_cancel),
                    rpc_url: url("http://localhost:8545"),
                    source: dummy_source(),
                },
            ),
            (
                2,
                WorkerRuntime {
                    cancel: second_cancel.clone(),
                    handle: spawn_probe(&second_cancel, &first_cancel),
                    rpc_url: url("http://localhost:8545"),
                    source: dummy_source(),
                },
            ),
        ]);

        shutdown_workers(workers).await;

        assert_eq!(observed_collective_cancellation.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn apply_starts_disables_and_reenables_a_chain_without_restart() {
        let context = TestContext::new();
        let handle = context.handle();

        handle.apply(registered(1, "http://localhost:8545", true)).await;
        assert_eq!(running_worker_count(&handle).await, 1);
        wait_for(|| context.statuses.snapshot()[0].worker_state == WorkerState::Running).await;

        handle.apply(registered(1, "http://localhost:8545", false)).await;
        assert_eq!(running_worker_count(&handle).await, 0);
        assert_eq!(context.statuses.snapshot()[0].worker_state, WorkerState::Disabled);

        handle.apply(registered(1, "http://localhost:8545", true)).await;
        assert_eq!(running_worker_count(&handle).await, 1);
        assert!(context.statuses.snapshot()[0].enabled);

        handle.shutdown().await;
        assert_eq!(running_worker_count(&handle).await, 0);
    }

    #[tokio::test]
    async fn apply_rotates_the_rpc_url_without_stopping_the_worker() {
        let context = TestContext::new();
        let handle = context.handle();

        handle.apply(registered(1, "http://localhost:8545", true)).await;
        let cancel = {
            let control = handle.shared.control.lock().await;
            control.workers.get(&1).expect("worker is running").cancel.clone()
        };

        handle.apply(registered(1, "http://localhost:9545", true)).await;

        assert!(!cancel.is_cancelled(), "rotation must not stop the worker");
        assert_eq!(running_worker_count(&handle).await, 1);
        assert_eq!(
            context.sources.rotations(),
            vec![url("http://localhost:9545")],
            "the running source must rotate in place"
        );
        let control = handle.shared.control.lock().await;
        let runtime = control.workers.get(&1).expect("worker is running");
        assert_eq!(runtime.rpc_url, url("http://localhost:9545"));
    }

    #[tokio::test]
    async fn apply_restarts_the_worker_when_rotation_is_unsupported() {
        let context = TestContext::new().without_rotation();
        let handle = context.handle();

        handle.apply(registered(1, "http://localhost:8545", true)).await;
        let cancel = {
            let control = handle.shared.control.lock().await;
            control.workers.get(&1).expect("worker is running").cancel.clone()
        };

        handle.apply(registered(1, "http://localhost:9545", true)).await;

        assert!(cancel.is_cancelled(), "the old worker must stop when rotation is unsupported");
        assert_eq!(running_worker_count(&handle).await, 1);
        assert!(context.sources.rotations().is_empty());
        let control = handle.shared.control.lock().await;
        let runtime = control.workers.get(&1).expect("worker is running");
        assert_eq!(runtime.rpc_url, url("http://localhost:9545"));
    }

    #[tokio::test]
    async fn apply_marks_a_chain_degraded_when_its_source_fails_validation() {
        let context = TestContext::new().failing_probe();
        let handle = context.handle();

        handle.apply(registered(1, "http://localhost:8545", true)).await;

        assert_eq!(running_worker_count(&handle).await, 0);
        let snapshot = context.statuses.snapshot();
        assert_eq!(snapshot[0].worker_state, WorkerState::Degraded);
        assert_eq!(snapshot[0].last_error.as_deref(), Some("RPC endpoint validation failed"));
    }

    #[tokio::test]
    async fn retire_stops_the_worker_and_removes_its_status() {
        let context = TestContext::new();
        let handle = context.handle();

        handle.apply(registered(1, "http://localhost:8545", true)).await;
        assert_eq!(context.statuses.snapshot().len(), 1);

        handle.retire(1).await;

        assert_eq!(running_worker_count(&handle).await, 0);
        assert!(context.statuses.snapshot().is_empty());

        // Retiring an absent chain is a no-op.
        handle.retire(1).await;
        assert!(context.statuses.snapshot().is_empty());
    }

    #[tokio::test]
    async fn shutdown_closes_the_control_plane_before_draining_workers() {
        let context = TestContext::new();
        let handle = context.handle();
        handle.apply(registered(1, "http://localhost:8545", true)).await;

        handle.shutdown().await;
        handle.apply(registered(2, "http://localhost:8545", true)).await;

        let control = handle.shared.control.lock().await;
        assert!(control.closed);
        assert!(control.workers.is_empty());
        assert!(context.statuses.snapshot().iter().all(|status| status.chain_id != 2));
    }

    #[tokio::test]
    async fn stop_worker_cancels_and_awaits_the_task() {
        let context = TestContext::new();
        let cancel = CancellationToken::new();
        let runtime = test_runtime(&context, cancel.clone());

        stop_worker(runtime).await;

        assert!(cancel.is_cancelled());
    }

    #[tokio::test]
    async fn run_applies_startup_chains_and_shuts_down_workers() {
        let context = TestContext::new();
        let supervisor = Supervisor::new(
            context.config(),
            vec![
                registered(1, "http://localhost:8545", true),
                registered(2, "http://localhost:8545", false),
            ],
            context.dependencies(),
        );
        let statuses = context.statuses.clone();
        let handle = supervisor.handle();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(supervisor.run(cancel.clone()));

        wait_for(|| statuses.snapshot().len() == 2).await;
        assert_eq!(statuses.snapshot()[1].worker_state, WorkerState::Disabled);

        cancel.cancel();
        task.await.expect("supervisor task finishes");

        assert_eq!(running_worker_count(&handle).await, 0);
        assert_eq!(statuses.snapshot().len(), 2, "shutdown keeps status records");
    }
}
