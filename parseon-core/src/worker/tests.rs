//! Worker integration tests.
//!
//! These tests exercise [`super::run_once`] and [`super::probe_source`] against
//! in-process fakes for storage, block source, cache, and sink. They cover
//! cancellation, cursor progress, event-window log fetching, sink submission
//! ordering, cross-chain isolation, status transitions, concurrent
//! preparation with ordered commits, and failure isolation.

use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use alloy::primitives::{Address, B256};
use async_trait::async_trait;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::{
    BlockNumber, Chain, PollContext, PollOutcome, PollResult, SourceStatus, WorkerConfig,
    probe_source, run_once,
};
use crate::filter::Filter;
use crate::monitor::Monitor;
use crate::ports::{
    BlockCache, BlockCommit, BlockRange, BlockSource, IndexStorage, LogQuery, NoopSink,
    NoopTelemetry, Sink, SinkBatch,
};
use crate::status::ChainStatus;
use crate::{
    BlockTransaction, CallTarget, Cursor, EventTarget, ExecutionOutcome, MonitorId, SourceBlock,
    Target,
};

struct FakeStorage {
    monitor: Monitor,
    commits: Mutex<Vec<BlockCommit>>,
}

struct RejectingStorage(Monitor);

#[derive(Default)]
struct RecordingSink(Mutex<Vec<SinkBatch>>);

impl Sink for RecordingSink {
    fn submit(&self, batch: SinkBatch) {
        self.0.lock().unwrap().push(batch);
    }
}

fn completed(outcome: PollOutcome) -> PollResult {
    let PollOutcome::Completed(poll) = outcome else { panic!("poll unexpectedly cancelled") };
    poll
}

#[async_trait]
impl IndexStorage for FakeStorage {
    async fn load_monitors(&self, chain: Chain) -> anyhow::Result<Vec<Monitor>> {
        Ok((self.monitor.chain == chain).then(|| self.monitor.clone()).into_iter().collect())
    }

    async fn commit_block(&self, commit: &BlockCommit) -> anyhow::Result<()> {
        self.commits.lock().unwrap().push(commit.clone());
        Ok(())
    }
}

#[async_trait]
impl IndexStorage for RejectingStorage {
    async fn load_monitors(&self, chain: Chain) -> anyhow::Result<Vec<Monitor>> {
        Ok((self.0.chain == chain).then(|| self.0.clone()).into_iter().collect())
    }

    async fn commit_block(&self, _: &BlockCommit) -> anyhow::Result<()> {
        anyhow::bail!("storage commit failed")
    }
}

struct FakeSource;

#[derive(Default)]
struct CandidateSource {
    candidates: Mutex<Vec<B256>>,
}

struct UnsupportedFinalizedSource;

struct PendingSource;

#[derive(Default)]
struct RecordingLogSource {
    ranges: Mutex<Vec<BlockRange>>,
}

struct ParallelSource {
    current: AtomicUsize,
    maximum: AtomicUsize,
    fail_at: Option<BlockNumber>,
}

impl ParallelSource {
    fn new(fail_at: Option<BlockNumber>) -> Self {
        Self { current: AtomicUsize::new(0), maximum: AtomicUsize::new(0), fail_at }
    }
}

#[derive(Default)]
struct FakeCache {
    blocks: Mutex<HashMap<(u64, BlockNumber), Arc<SourceBlock>>>,
}

impl BlockCache for FakeCache {
    fn get(&self, chain: Chain, block_number: BlockNumber) -> Option<Arc<SourceBlock>> {
        self.blocks.lock().unwrap().get(&(chain.id, block_number)).cloned()
    }

    fn put(&self, chain: Chain, block: Arc<SourceBlock>) {
        self.blocks.lock().unwrap().insert((chain.id, block.number), block);
    }

    fn evict_before(&self, chain: Chain, block_number: BlockNumber) {
        self.blocks
            .lock()
            .unwrap()
            .retain(|(chain_id, number), _| *chain_id != chain.id || *number >= block_number);
    }
}

#[async_trait]
impl BlockSource for FakeSource {
    async fn chain_id(&self) -> anyhow::Result<u64> {
        Ok(1)
    }

    async fn finalized_head(&self) -> anyhow::Result<BlockNumber> {
        Ok(10)
    }

    async fn fetch_block(&self, block_number: BlockNumber) -> anyhow::Result<SourceBlock> {
        Ok(SourceBlock { number: block_number, transactions: Vec::new() })
    }

    async fn fetch_execution_outcomes(
        &self,
        _block_number: BlockNumber,
        _transaction_hashes: &[B256],
    ) -> anyhow::Result<Vec<ExecutionOutcome>> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl BlockSource for CandidateSource {
    async fn chain_id(&self) -> anyhow::Result<u64> {
        Ok(1)
    }

    async fn finalized_head(&self) -> anyhow::Result<BlockNumber> {
        Ok(10)
    }

    async fn fetch_block(&self, block_number: BlockNumber) -> anyhow::Result<SourceBlock> {
        Ok(SourceBlock {
            number: block_number,
            transactions: vec![
                BlockTransaction {
                    hash: B256::repeat_byte(1),
                    from: Address::ZERO,
                    to: Address::ZERO,
                    input: vec![1, 2, 3, 4].into(),
                },
                BlockTransaction {
                    hash: B256::repeat_byte(2),
                    from: Address::ZERO,
                    to: Address::ZERO,
                    input: vec![4, 3, 2, 1].into(),
                },
                BlockTransaction {
                    hash: B256::repeat_byte(3),
                    from: Address::ZERO,
                    to: Address::repeat_byte(1),
                    input: vec![1, 2, 3, 4].into(),
                },
                BlockTransaction {
                    hash: B256::repeat_byte(4),
                    from: Address::ZERO,
                    to: Address::ZERO,
                    input: vec![1, 2, 3].into(),
                },
            ],
        })
    }

    async fn fetch_execution_outcomes(
        &self,
        _block_number: BlockNumber,
        transaction_hashes: &[B256],
    ) -> anyhow::Result<Vec<ExecutionOutcome>> {
        self.candidates.lock().unwrap().extend_from_slice(transaction_hashes);
        Ok(transaction_hashes
            .iter()
            .copied()
            .map(|transaction_hash| ExecutionOutcome { transaction_hash, succeeded: true })
            .collect())
    }
}

#[async_trait]
impl BlockSource for UnsupportedFinalizedSource {
    async fn chain_id(&self) -> anyhow::Result<u64> {
        Ok(1)
    }

    async fn finalized_head(&self) -> anyhow::Result<BlockNumber> {
        anyhow::bail!("invalid argument: finalized")
    }

    async fn fetch_block(&self, _block_number: BlockNumber) -> anyhow::Result<SourceBlock> {
        unreachable!()
    }

    async fn fetch_execution_outcomes(
        &self,
        _block_number: BlockNumber,
        _transaction_hashes: &[B256],
    ) -> anyhow::Result<Vec<ExecutionOutcome>> {
        unreachable!()
    }
}

#[async_trait]
impl BlockSource for PendingSource {
    async fn chain_id(&self) -> anyhow::Result<u64> {
        Ok(1)
    }

    async fn finalized_head(&self) -> anyhow::Result<BlockNumber> {
        std::future::pending().await
    }

    async fn fetch_block(&self, _block_number: BlockNumber) -> anyhow::Result<SourceBlock> {
        unreachable!()
    }

    async fn fetch_execution_outcomes(
        &self,
        _block_number: BlockNumber,
        _transaction_hashes: &[B256],
    ) -> anyhow::Result<Vec<ExecutionOutcome>> {
        unreachable!()
    }
}

#[async_trait]
impl BlockSource for RecordingLogSource {
    async fn chain_id(&self) -> anyhow::Result<u64> {
        Ok(1)
    }

    async fn finalized_head(&self) -> anyhow::Result<BlockNumber> {
        Ok(12)
    }

    async fn fetch_block(&self, _block_number: BlockNumber) -> anyhow::Result<SourceBlock> {
        unreachable!()
    }

    async fn fetch_execution_outcomes(
        &self,
        _block_number: BlockNumber,
        _transaction_hashes: &[B256],
    ) -> anyhow::Result<Vec<ExecutionOutcome>> {
        unreachable!()
    }

    async fn fetch_logs(&self, query: LogQuery) -> anyhow::Result<Vec<crate::SourceLog>> {
        self.ranges.lock().unwrap().push(query.range());
        Ok(Vec::new())
    }
}

#[async_trait]
impl BlockSource for ParallelSource {
    async fn chain_id(&self) -> anyhow::Result<u64> {
        Ok(1)
    }

    async fn finalized_head(&self) -> anyhow::Result<BlockNumber> {
        Ok(12)
    }

    async fn fetch_block(&self, block_number: BlockNumber) -> anyhow::Result<SourceBlock> {
        let in_flight = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(in_flight, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis((13 - block_number) * 5)).await;
        self.current.fetch_sub(1, Ordering::SeqCst);
        if self.fail_at == Some(block_number) {
            anyhow::bail!("block {block_number} failed")
        }
        Ok(SourceBlock { number: block_number, transactions: Vec::new() })
    }

    async fn fetch_execution_outcomes(
        &self,
        _block_number: BlockNumber,
        _transaction_hashes: &[B256],
    ) -> anyhow::Result<Vec<ExecutionOutcome>> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn startup_probe_requires_finalized_tag_support() {
    assert_eq!(
        probe_source(&FakeSource).await.unwrap(),
        SourceStatus { chain_id: 1, finalized_head: 10 }
    );

    let error = probe_source(&UnsupportedFinalizedSource).await.unwrap_err();
    assert!(error.to_string().contains("does not support the required finalized block tag"));
}

#[tokio::test]
async fn cancellation_interrupts_initial_source_io() {
    let storage = FakeStorage {
        monitor: Monitor {
            id: MonitorId::new(7).unwrap(),
            chain: Chain::new(1),
            target: Target::Call(CallTarget {
                address: Address::ZERO,
                selector: [1, 2, 3, 4].into(),
                inputs: Vec::new(),
            }),
            start_block: 10,
            end_block: None,
            cursor: Cursor(None),
            completed: false,
            enabled: true,
            filter: Filter::All,
        },
        commits: Mutex::new(Vec::new()),
    };
    let config = WorkerConfig {
        chain: Chain::new(1),
        batch_size: NonZeroU64::new(1).unwrap(),
        block_concurrency: NonZeroUsize::new(1).unwrap(),
        poll_interval: Duration::from_millis(100),
    };
    let cache = FakeCache::default();
    let storage_writes = Semaphore::new(1);
    let telemetry = NoopTelemetry;
    let cancel = CancellationToken::new();
    let cancel_after_poll = async {
        tokio::task::yield_now().await;
        cancel.cancel();
    };

    let (outcome, ()) = tokio::join!(
        run_once(
            &config,
            PollContext {
                storage: &storage,
                source: &PendingSource,
                cache: &cache,
                storage_writes: &storage_writes,
                sink: &NoopSink,
                telemetry: &telemetry,
                cancel: &cancel,
            },
        ),
        cancel_after_poll,
    );

    assert_eq!(outcome.unwrap(), PollOutcome::Cancelled);
    assert!(storage.commits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn commits_cursor_progress_when_block_has_no_matches() {
    let storage = FakeStorage {
        monitor: Monitor {
            id: MonitorId::new(7).unwrap(),
            chain: Chain::new(1),
            target: Target::Call(CallTarget {
                address: Address::ZERO,
                selector: [1, 2, 3, 4].into(),
                inputs: Vec::new(),
            }),
            start_block: 10,
            end_block: None,
            cursor: Cursor(None),
            completed: false,
            enabled: true,
            filter: Filter::All,
        },
        commits: Mutex::new(Vec::new()),
    };
    let config = WorkerConfig {
        chain: Chain::new(1),
        batch_size: NonZeroU64::new(1).unwrap(),
        block_concurrency: NonZeroUsize::new(1).unwrap(),
        poll_interval: Duration::from_millis(100),
    };
    let storage_writes = Semaphore::new(1);
    let telemetry = NoopTelemetry;
    let poll = completed(
        run_once(
            &config,
            PollContext {
                storage: &storage,
                source: &FakeSource,
                cache: &FakeCache::default(),
                storage_writes: &storage_writes,
                sink: &NoopSink,
                telemetry: &telemetry,
                cancel: &CancellationToken::new(),
            },
        )
        .await
        .unwrap(),
    );

    assert_eq!(poll.finalized_head, 10);
    assert_eq!(poll.decoded, 0);
    let commits = storage.commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].block_number, 10);
    assert_eq!(commits[0].monitors[0].id.get(), 7);
}

#[tokio::test]
async fn fetches_execution_only_for_indexed_call_targets() {
    let storage = FakeStorage {
        monitor: Monitor {
            id: MonitorId::new(7).unwrap(),
            chain: Chain::new(1),
            target: Target::Call(CallTarget {
                address: Address::ZERO,
                selector: [1, 2, 3, 4].into(),
                inputs: Vec::new(),
            }),
            start_block: 10,
            end_block: None,
            cursor: Cursor(None),
            completed: false,
            enabled: true,
            filter: Filter::All,
        },
        commits: Mutex::new(Vec::new()),
    };
    let source = CandidateSource::default();
    let config = WorkerConfig {
        chain: Chain::new(1),
        batch_size: NonZeroU64::new(1).unwrap(),
        block_concurrency: NonZeroUsize::new(1).unwrap(),
        poll_interval: Duration::from_millis(100),
    };

    completed(
        run_once(
            &config,
            PollContext {
                storage: &storage,
                source: &source,
                cache: &FakeCache::default(),
                storage_writes: &Semaphore::new(1),
                sink: &NoopSink,
                telemetry: &NoopTelemetry,
                cancel: &CancellationToken::new(),
            },
        )
        .await
        .unwrap(),
    );

    assert_eq!(*source.candidates.lock().unwrap(), [B256::repeat_byte(1)]);
}

#[tokio::test]
async fn fetches_contiguous_event_blocks_with_one_ranged_log_query() {
    let storage = FakeStorage {
        monitor: Monitor {
            id: MonitorId::new(7).unwrap(),
            chain: Chain::new(1),
            target: Target::Event(EventTarget {
                address: Address::repeat_byte(1),
                topic0: B256::repeat_byte(2),
                params: Vec::new(),
            }),
            start_block: 10,
            end_block: Some(12),
            cursor: Cursor(None),
            completed: false,
            enabled: true,
            filter: Filter::All,
        },
        commits: Mutex::new(Vec::new()),
    };
    let source = RecordingLogSource::default();
    let config = WorkerConfig {
        chain: Chain::new(1),
        batch_size: NonZeroU64::new(3).unwrap(),
        block_concurrency: NonZeroUsize::new(3).unwrap(),
        poll_interval: Duration::from_millis(100),
    };

    let poll = completed(
        run_once(
            &config,
            PollContext {
                storage: &storage,
                source: &source,
                cache: &FakeCache::default(),
                storage_writes: &Semaphore::new(1),
                sink: &NoopSink,
                telemetry: &NoopTelemetry,
                cancel: &CancellationToken::new(),
            },
        )
        .await
        .unwrap(),
    );

    assert_eq!(poll.decoded, 0);
    assert_eq!(*source.ranges.lock().unwrap(), [BlockRange::new(10, 12).unwrap()]);
}

#[tokio::test]
async fn submits_non_empty_batches_only_after_successful_commit() {
    let monitor = Monitor {
        id: MonitorId::new(7).unwrap(),
        chain: Chain::new(1),
        target: Target::Call(CallTarget {
            address: Address::ZERO,
            selector: [1, 2, 3, 4].into(),
            inputs: Vec::new(),
        }),
        start_block: 10,
        end_block: None,
        cursor: Cursor(None),
        completed: false,
        enabled: true,
        filter: Filter::All,
    };
    let config = WorkerConfig {
        chain: Chain::new(1),
        batch_size: NonZeroU64::new(1).unwrap(),
        block_concurrency: NonZeroUsize::new(1).unwrap(),
        poll_interval: Duration::from_millis(100),
    };
    let sink = RecordingSink::default();
    let storage = FakeStorage { monitor: monitor.clone(), commits: Mutex::new(Vec::new()) };
    completed(
        run_once(
            &config,
            PollContext {
                storage: &storage,
                source: &CandidateSource::default(),
                cache: &FakeCache::default(),
                storage_writes: &Semaphore::new(1),
                sink: &sink,
                telemetry: &NoopTelemetry,
                cancel: &CancellationToken::new(),
            },
        )
        .await
        .unwrap(),
    );
    assert_eq!(storage.commits.lock().unwrap().len(), 1);
    assert_eq!(sink.0.lock().unwrap().len(), 1);
    assert_eq!(sink.0.lock().unwrap()[0].results.len(), 1);

    let sink = RecordingSink::default();
    assert!(
        run_once(
            &config,
            PollContext {
                storage: &RejectingStorage(monitor),
                source: &CandidateSource::default(),
                cache: &FakeCache::default(),
                storage_writes: &Semaphore::new(1),
                sink: &sink,
                telemetry: &NoopTelemetry,
                cancel: &CancellationToken::new(),
            },
        )
        .await
        .is_err()
    );
    assert!(sink.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn ignores_monitors_owned_by_another_chain() {
    let storage = FakeStorage {
        monitor: Monitor {
            id: MonitorId::new(7).unwrap(),
            chain: Chain::new(2),
            target: Target::Call(CallTarget {
                address: Address::ZERO,
                selector: [1, 2, 3, 4].into(),
                inputs: Vec::new(),
            }),
            start_block: 10,
            end_block: None,
            cursor: Cursor(None),
            completed: false,
            enabled: true,
            filter: Filter::All,
        },
        commits: Mutex::new(Vec::new()),
    };
    let config = WorkerConfig {
        chain: Chain::new(1),
        batch_size: NonZeroU64::new(1).unwrap(),
        block_concurrency: NonZeroUsize::new(1).unwrap(),
        poll_interval: Duration::from_millis(100),
    };
    let storage_writes = Semaphore::new(1);
    let telemetry = NoopTelemetry;

    let poll = completed(
        run_once(
            &config,
            PollContext {
                storage: &storage,
                source: &FakeSource,
                cache: &FakeCache::default(),
                storage_writes: &storage_writes,
                sink: &NoopSink,
                telemetry: &telemetry,
                cancel: &CancellationToken::new(),
            },
        )
        .await
        .unwrap(),
    );

    assert_eq!(poll.finalized_head, 10);
    assert!(storage.commits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn status_keeps_last_head_and_degrades_after_rpc_failure() {
    let status = ChainStatus::running(1, 9);
    let storage = FakeStorage {
        monitor: Monitor {
            id: MonitorId::new(7).unwrap(),
            chain: Chain::new(1),
            target: Target::Call(CallTarget {
                address: Address::ZERO,
                selector: [1, 2, 3, 4].into(),
                inputs: Vec::new(),
            }),
            start_block: 11,
            end_block: None,
            cursor: Cursor(None),
            completed: false,
            enabled: true,
            filter: Filter::All,
        },
        commits: Mutex::new(Vec::new()),
    };
    let config = WorkerConfig {
        chain: Chain::new(1),
        batch_size: NonZeroU64::new(1).unwrap(),
        block_concurrency: NonZeroUsize::new(1).unwrap(),
        poll_interval: Duration::from_millis(100),
    };
    let storage_writes = Semaphore::new(1);
    let telemetry = NoopTelemetry;

    let poll = completed(
        run_once(
            &config,
            PollContext {
                storage: &storage,
                source: &FakeSource,
                cache: &FakeCache::default(),
                storage_writes: &storage_writes,
                sink: &NoopSink,
                telemetry: &telemetry,
                cancel: &CancellationToken::new(),
            },
        )
        .await
        .unwrap(),
    );
    status.record_success(poll.finalized_head);

    let error = run_once(
        &config,
        PollContext {
            storage: &storage,
            source: &UnsupportedFinalizedSource,
            cache: &FakeCache::default(),
            storage_writes: &storage_writes,
            sink: &NoopSink,
            telemetry: &telemetry,
            cancel: &CancellationToken::new(),
        },
    )
    .await
    .unwrap_err();
    status.record_error(&error);

    let snapshot = status.snapshot();
    assert_eq!(snapshot.finalized_head, Some(10));
    assert_eq!(snapshot.worker_state, crate::status::WorkerState::Degraded);
    assert!(snapshot.last_error.unwrap().contains("invalid argument"));
}

#[tokio::test]
async fn prepares_concurrently_but_commits_in_block_order() {
    let storage = FakeStorage {
        monitor: Monitor {
            id: MonitorId::new(7).unwrap(),
            chain: Chain::new(1),
            target: Target::Call(CallTarget {
                address: Address::ZERO,
                selector: [1, 2, 3, 4].into(),
                inputs: Vec::new(),
            }),
            start_block: 10,
            end_block: Some(12),
            cursor: Cursor(None),
            completed: false,
            enabled: true,
            filter: Filter::All,
        },
        commits: Mutex::new(Vec::new()),
    };
    let source = ParallelSource::new(None);
    let config = WorkerConfig {
        chain: Chain::new(1),
        batch_size: NonZeroU64::new(3).unwrap(),
        block_concurrency: NonZeroUsize::new(3).unwrap(),
        poll_interval: Duration::from_millis(100),
    };

    completed(
        run_once(
            &config,
            PollContext {
                storage: &storage,
                source: &source,
                cache: &FakeCache::default(),
                storage_writes: &Semaphore::new(1),
                sink: &NoopSink,
                telemetry: &NoopTelemetry,
                cancel: &CancellationToken::new(),
            },
        )
        .await
        .unwrap(),
    );

    assert_eq!(source.maximum.load(Ordering::SeqCst), 3);
    let committed = storage
        .commits
        .lock()
        .unwrap()
        .iter()
        .map(|commit| commit.block_number)
        .collect::<Vec<_>>();
    assert_eq!(committed, [10, 11, 12]);
}

#[tokio::test]
async fn never_commits_past_a_failed_block() {
    let storage = FakeStorage {
        monitor: Monitor {
            id: MonitorId::new(7).unwrap(),
            chain: Chain::new(1),
            target: Target::Call(CallTarget {
                address: Address::ZERO,
                selector: [1, 2, 3, 4].into(),
                inputs: Vec::new(),
            }),
            start_block: 10,
            end_block: Some(12),
            cursor: Cursor(None),
            completed: false,
            enabled: true,
            filter: Filter::All,
        },
        commits: Mutex::new(Vec::new()),
    };
    let source = ParallelSource::new(Some(11));
    let config = WorkerConfig {
        chain: Chain::new(1),
        batch_size: NonZeroU64::new(3).unwrap(),
        block_concurrency: NonZeroUsize::new(3).unwrap(),
        poll_interval: Duration::from_millis(100),
    };

    let error = run_once(
        &config,
        PollContext {
            storage: &storage,
            source: &source,
            cache: &FakeCache::default(),
            storage_writes: &Semaphore::new(1),
            sink: &NoopSink,
            telemetry: &NoopTelemetry,
            cancel: &CancellationToken::new(),
        },
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("block 11 failed"));
    let committed = storage
        .commits
        .lock()
        .unwrap()
        .iter()
        .map(|commit| commit.block_number)
        .collect::<Vec<_>>();
    assert_eq!(committed, [10]);
}
