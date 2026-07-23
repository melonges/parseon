//! Shared fakes for supervisor and application-service tests.
//!
//! The fakes are behavioral, not mocks: [`TestSource`] answers source probes
//! and records RPC URL rotations, [`TestStorage`] keeps the chain registry in
//! memory and counts monitor loads so tests can observe whether a worker is
//! polling, and [`TestContext`] wires them into supervisor dependencies.

use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::Context as _;
use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::Semaphore;

use crate::commands::ResultQuery;
use crate::monitor::Monitor;
use crate::ports::{
    BlockCache, BlockCacheFactory, BlockCommit, BlockSource, BlockSourceFactory, ChainRecord,
    ChainRepository, ChainUpdate, IndexStorage, MonitorRecord, MonitorRepository, NewChain,
    NewMonitor, NoopSink, NoopTelemetry, RegisteredChain, ResultRecord, ResultRepository,
};
use crate::status::RuntimeStatus;
use crate::supervisor::{Supervisor, SupervisorConfig, SupervisorDependencies, SupervisorHandle};
use crate::{BlockNumber, Chain, ChainId, ExecutionOutcome, MonitorId, SourceBlock, TxHash, Url};

/// Parses a test RPC URL.
pub(crate) fn url(value: &str) -> Url {
    Url::parse(value).expect("test URL parses")
}

/// Waits until `condition` holds, failing after a generous timeout.
pub(crate) async fn wait_for(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("condition met within timeout");
}

/// A fake block source that answers probes and records URL rotations.
pub(crate) struct TestSource {
    chain_id: u64,
    finalized_head: BlockNumber,
    fail_probe: bool,
    rotation_supported: bool,
    rotations: Arc<Mutex<Vec<Url>>>,
}

#[async_trait]
impl BlockSource for TestSource {
    async fn chain_id(&self) -> anyhow::Result<u64> {
        anyhow::ensure!(!self.fail_probe, "probe failure");
        Ok(self.chain_id)
    }

    async fn finalized_head(&self) -> anyhow::Result<BlockNumber> {
        anyhow::ensure!(!self.fail_probe, "probe failure");
        Ok(self.finalized_head)
    }

    async fn fetch_block(&self, _block_number: BlockNumber) -> anyhow::Result<SourceBlock> {
        anyhow::bail!("unused in tests")
    }

    async fn fetch_execution_outcomes(
        &self,
        _block_number: BlockNumber,
        _transaction_hashes: &[TxHash],
    ) -> anyhow::Result<Vec<ExecutionOutcome>> {
        anyhow::bail!("unused in tests")
    }

    fn set_rpc_url(&self, rpc_url: &Url) -> anyhow::Result<()> {
        anyhow::ensure!(self.rotation_supported, "rotation unsupported");
        self.rotations.lock().push(rpc_url.clone());
        Ok(())
    }
}

/// A fake block source factory handing out [`TestSource`]s that share one
/// rotation log.
#[derive(Clone)]
pub(crate) struct TestSourceFactory {
    chain_id: u64,
    finalized_head: BlockNumber,
    fail_probe: bool,
    rotation_supported: bool,
    rotations: Arc<Mutex<Vec<Url>>>,
}

impl TestSourceFactory {
    /// Every URL a connected source was asked to rotate to, in order.
    pub(crate) fn rotations(&self) -> Vec<Url> {
        self.rotations.lock().clone()
    }
}

impl BlockSourceFactory for TestSourceFactory {
    fn connect(&self, _rpc_url: &Url) -> anyhow::Result<Arc<dyn BlockSource>> {
        Ok(Arc::new(TestSource {
            chain_id: self.chain_id,
            finalized_head: self.finalized_head,
            fail_probe: self.fail_probe,
            rotation_supported: self.rotation_supported,
            rotations: self.rotations.clone(),
        }))
    }
}

/// In-memory storage fake: a working chain registry plus a monitor-load
/// counter that reveals whether any worker is polling.
#[derive(Default)]
pub(crate) struct TestStorage {
    chains: Mutex<HashMap<ChainId, ChainRecord>>,
    deleted: Mutex<Vec<ChainId>>,
    next_update_pause: Mutex<Option<Arc<OperationPause>>>,
    load_monitor_calls: AtomicUsize,
}

/// One-shot barrier used to hold a fake storage operation after persistence.
pub(crate) struct OperationPause {
    reached: Semaphore,
    resume: Semaphore,
}

impl OperationPause {
    async fn pause(&self) {
        self.reached.add_permits(1);
        self.resume.acquire().await.expect("pause remains open").forget();
    }

    /// Waits until the paused operation has persisted its change.
    pub(crate) async fn wait_until_reached(&self) {
        self.reached.acquire().await.expect("pause remains open").forget();
    }

    /// Allows the paused operation to return.
    pub(crate) fn resume(&self) {
        self.resume.add_permits(1);
    }
}

impl TestStorage {
    /// Chain IDs passed to [`ChainRepository::delete_chain`], in order.
    pub(crate) fn deleted_chains(&self) -> Vec<ChainId> {
        self.deleted.lock().clone()
    }

    /// How many times workers loaded monitors so far.
    pub(crate) fn load_monitor_calls(&self) -> usize {
        self.load_monitor_calls.load(Ordering::Relaxed)
    }

    /// Pauses the next chain update after its record is changed but before the
    /// repository call returns to the service.
    pub(crate) fn pause_next_update(&self) -> Arc<OperationPause> {
        let pause =
            Arc::new(OperationPause { reached: Semaphore::new(0), resume: Semaphore::new(0) });
        *self.next_update_pause.lock() = Some(pause.clone());
        pause
    }
}

#[async_trait]
impl IndexStorage for TestStorage {
    async fn load_monitors(&self, _chain: Chain) -> anyhow::Result<Vec<Monitor>> {
        self.load_monitor_calls.fetch_add(1, Ordering::Relaxed);
        Ok(Vec::new())
    }

    async fn commit_block(&self, _commit: &BlockCommit) -> anyhow::Result<()> {
        anyhow::bail!("unused in tests")
    }
}

#[async_trait]
impl ChainRepository for TestStorage {
    async fn list_registered_chains(&self) -> anyhow::Result<Vec<RegisteredChain>> {
        Ok(self
            .chains
            .lock()
            .values()
            .map(|record| RegisteredChain {
                chain: record.chain,
                rpc_url: record.rpc_url.clone(),
                enabled: record.enabled,
            })
            .collect())
    }

    async fn create_chain(&self, chain: NewChain) -> anyhow::Result<ChainRecord> {
        let record = ChainRecord {
            chain: chain.chain,
            rpc_url: chain.rpc_url,
            enabled: chain.enabled,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        self.chains.lock().insert(chain.chain.id, record.clone());
        Ok(record)
    }

    async fn list_chains(&self) -> anyhow::Result<Vec<ChainRecord>> {
        Ok(self.chains.lock().values().cloned().collect())
    }

    async fn get_chain(&self, chain: Chain) -> anyhow::Result<ChainRecord> {
        self.chains.lock().get(&chain.id).cloned().context("unknown chain")
    }

    async fn update_chain(&self, chain: Chain, update: ChainUpdate) -> anyhow::Result<ChainRecord> {
        let record = {
            let mut chains = self.chains.lock();
            let record = chains.get_mut(&chain.id).context("unknown chain")?;
            if let Some(rpc_url) = update.rpc_url {
                record.rpc_url = rpc_url;
            }
            if let Some(enabled) = update.enabled {
                record.enabled = enabled;
            }
            record.updated_at = chrono::Utc::now();
            record.clone()
        };
        let pause = self.next_update_pause.lock().take();
        if let Some(pause) = pause {
            pause.pause().await;
        }
        Ok(record)
    }

    async fn delete_chain(&self, chain: Chain) -> anyhow::Result<()> {
        self.chains.lock().remove(&chain.id);
        self.deleted.lock().push(chain.id);
        Ok(())
    }
}

#[async_trait]
impl MonitorRepository for TestStorage {
    async fn count_monitors(&self) -> anyhow::Result<usize> {
        anyhow::bail!("unused in tests")
    }

    async fn create_monitor(&self, _monitor: NewMonitor) -> anyhow::Result<MonitorRecord> {
        anyhow::bail!("unused in tests")
    }

    async fn list_monitors(&self, _chain: Option<Chain>) -> anyhow::Result<Vec<MonitorRecord>> {
        anyhow::bail!("unused in tests")
    }

    async fn get_monitor(&self, _id: MonitorId) -> anyhow::Result<MonitorRecord> {
        anyhow::bail!("unused in tests")
    }

    async fn set_monitor_enabled(
        &self,
        _id: MonitorId,
        _enabled: bool,
    ) -> anyhow::Result<MonitorRecord> {
        anyhow::bail!("unused in tests")
    }

    async fn delete_monitor(&self, _id: MonitorId) -> anyhow::Result<()> {
        anyhow::bail!("unused in tests")
    }
}

#[async_trait]
impl ResultRepository for TestStorage {
    async fn query_results(
        &self,
        _monitor: &MonitorRecord,
        _query: ResultQuery,
    ) -> anyhow::Result<Vec<ResultRecord>> {
        anyhow::bail!("unused in tests")
    }
}

struct TestCache;

impl BlockCache for TestCache {
    fn get(&self, _chain: Chain, _block_number: BlockNumber) -> Option<Arc<SourceBlock>> {
        None
    }

    fn put(&self, _chain: Chain, _block: Arc<SourceBlock>) {}

    fn evict_before(&self, _chain: Chain, _block_number: BlockNumber) {}
}

struct TestCacheFactory;

impl BlockCacheFactory for TestCacheFactory {
    fn create(&self) -> Arc<dyn BlockCache> {
        Arc::new(TestCache)
    }
}

/// Wiring helper: one storage fake, one source factory, one status registry,
/// and constructors for supervisor configs, dependencies, and handles.
pub(crate) struct TestContext {
    /// Storage fake shared with every worker the supervisor starts.
    pub(crate) storage: Arc<TestStorage>,
    /// Source factory fake; inspect `rotations()` after applying URL changes.
    pub(crate) sources: TestSourceFactory,
    /// Status registry shared with the supervisor.
    pub(crate) statuses: RuntimeStatus,
}

impl TestContext {
    pub(crate) fn new() -> Self {
        Self {
            storage: Arc::new(TestStorage::default()),
            sources: TestSourceFactory {
                chain_id: 1,
                finalized_head: 100,
                fail_probe: false,
                rotation_supported: true,
                rotations: Arc::new(Mutex::new(Vec::new())),
            },
            statuses: RuntimeStatus::default(),
        }
    }

    /// Sources handed out by this context refuse in-place URL rotation.
    pub(crate) fn without_rotation(mut self) -> Self {
        self.sources.rotation_supported = false;
        self
    }

    /// Sources handed out by this context fail validation probes.
    pub(crate) fn failing_probe(mut self) -> Self {
        self.sources.fail_probe = true;
        self
    }

    pub(crate) fn config(&self) -> SupervisorConfig {
        SupervisorConfig {
            batch_size: NonZeroU64::new(10).expect("non-zero"),
            // A short interval keeps worker-driven assertions responsive.
            poll_interval: Duration::from_millis(5),
            block_concurrency: NonZeroUsize::new(2).expect("non-zero"),
            storage_write_concurrency: NonZeroUsize::new(2).expect("non-zero"),
        }
    }

    pub(crate) fn dependencies(&self) -> SupervisorDependencies {
        SupervisorDependencies {
            storage: self.storage.clone(),
            sink: Arc::new(NoopSink),
            source_factory: Arc::new(self.sources.clone()),
            cache_factory: Arc::new(TestCacheFactory),
            statuses: self.statuses.clone(),
            telemetry: Arc::new(NoopTelemetry),
        }
    }

    /// A fresh supervisor handle over this context's fakes, with no startup
    /// chains.
    pub(crate) fn handle(&self) -> SupervisorHandle {
        Supervisor::new(self.config(), Vec::new(), self.dependencies()).handle()
    }
}
