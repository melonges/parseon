use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::indexer;
use super::ports::{BlockCache, BlockCommit, BlockSource, InFlightGuard, IndexStorage, Telemetry};
use super::status::ChainStatus;
use super::{BlockNumber, Chain, Cursor, DecodedResult, MonitorId, Target, scheduler};

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub chain: Chain,
    pub batch_size: NonZeroU64,
    pub block_concurrency: NonZeroUsize,
    pub poll_interval: Duration,
}

pub async fn run(
    config: WorkerConfig,
    storage: Arc<dyn IndexStorage>,
    source: Arc<dyn BlockSource>,
    cache: Arc<dyn BlockCache>,
    db_writes: Arc<Semaphore>,
    telemetry: Arc<dyn Telemetry>,
    status: ChainStatus,
    cancel: CancellationToken,
) {
    tracing::info!(chain_id = config.chain.id, "worker started");
    loop {
        if cancel.is_cancelled() {
            break;
        }
        match run_once(
            &config,
            storage.as_ref(),
            source.as_ref(),
            cache.as_ref(),
            db_writes.as_ref(),
            telemetry.as_ref(),
            &cancel,
        )
        .await
        {
            Ok(poll) => status.record_success(poll.finalized_head),
            Err(error) => {
                let message = status.record_error(&error);
                tracing::warn!(chain_id = config.chain.id, error = %message, "worker tick failed");
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(config.poll_interval) => {}
            _ = cancel.cancelled() => break,
        }
    }
    tracing::info!(chain_id = config.chain.id, "worker stopped");
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollResult {
    pub finalized_head: BlockNumber,
    pub decoded: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceStatus {
    pub chain_id: u64,
    pub finalized_head: BlockNumber,
}

pub async fn probe_source(source: &dyn BlockSource) -> anyhow::Result<SourceStatus> {
    let chain_id = source.chain_id().await?;
    let finalized_head = source.finalized_head().await.map_err(|error| {
        anyhow::anyhow!("RPC does not support the required finalized block tag: {error:#}")
    })?;
    Ok(SourceStatus {
        chain_id,
        finalized_head,
    })
}

pub async fn run_once(
    config: &WorkerConfig,
    storage: &dyn IndexStorage,
    source: &dyn BlockSource,
    cache: &dyn BlockCache,
    db_writes: &Semaphore,
    telemetry: &dyn Telemetry,
    cancel: &CancellationToken,
) -> anyhow::Result<PollResult> {
    let finalized_head = source.finalized_head().await?;
    let monitor_index = Arc::new(indexer::MonitorIndex::new(
        storage.load_monitors(config.chain).await?,
    )?);
    let active = monitor_index.monitors().iter().collect::<Vec<_>>();
    if active.is_empty() {
        telemetry.set_worker_lag(config.chain.id, 0);
        return Ok(PollResult {
            finalized_head,
            decoded: 0,
        });
    }

    let wanted = scheduler::plan_blocks(&active, finalized_head, config.batch_size);
    let planned = wanted
        .into_iter()
        .filter_map(|block_number| {
            let covering = active
                .iter()
                .filter(|monitor| monitor.needs_block(block_number))
                .map(|monitor| (*monitor).clone())
                .collect::<Vec<_>>();
            (!covering.is_empty()).then_some((block_number, covering))
        })
        .collect::<Vec<_>>();

    let mut prepared = super::pipeline::ordered(
        planned.into_iter().map(|(block_number, covering)| {
            let monitor_index = monitor_index.clone();
            async move {
                prepare_block(
                    config.chain,
                    block_number,
                    covering,
                    monitor_index,
                    source,
                    cache,
                    telemetry,
                )
                .await
            }
        }),
        config.block_concurrency.get(),
    );
    let mut decoded = 0;
    let mut progress = active
        .iter()
        .map(|monitor| (monitor.id, monitor.cursor.0))
        .collect::<std::collections::HashMap<_, _>>();

    loop {
        let next = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            next = prepared.next() => next,
        };
        let Some(next) = next else { break };
        let prepared = next?;
        let calls = prepared
            .results
            .iter()
            .filter(|result| matches!(result, DecodedResult::Call(_)))
            .count() as u64;
        let events = prepared.results.len() as u64 - calls;
        let permit = db_writes.acquire().await?;
        let _in_flight = InFlightGuard::new(telemetry, config.chain.id, "db");
        let started = std::time::Instant::now();
        let result = storage
            .commit_block(BlockCommit {
                chain: config.chain,
                block_number: prepared.block_number,
                monitors: prepared.monitors,
                results: prepared.results,
            })
            .await;
        drop(permit);
        match result {
            Ok(count) => {
                telemetry.record_commit(
                    config.chain.id,
                    calls,
                    events,
                    "success",
                    started.elapsed(),
                );
                decoded += count;
                for monitor_id in prepared.monitor_ids {
                    progress.insert(monitor_id, Some(prepared.block_number));
                }
            }
            Err(error) => {
                telemetry.record_commit(config.chain.id, calls, events, "error", started.elapsed());
                return Err(error);
            }
        }
    }

    if let Some(min_next) = active
        .iter()
        .filter_map(|monitor| {
            Cursor(*progress.get(&monitor.id).unwrap_or(&monitor.cursor.0))
                .next(monitor.start_block)
        })
        .min()
    {
        cache.evict_before(config.chain, min_next);
    }
    let lag = active
        .iter()
        .map(|monitor| {
            let target = monitor
                .end_block
                .unwrap_or(finalized_head)
                .min(finalized_head);
            Cursor(*progress.get(&monitor.id).unwrap_or(&monitor.cursor.0))
                .next(monitor.start_block)
                .map_or(0, |next| {
                    if next > target {
                        0
                    } else {
                        target.saturating_sub(next).saturating_add(1)
                    }
                })
        })
        .max()
        .unwrap_or(0);
    telemetry.set_worker_lag(config.chain.id, lag);
    Ok(PollResult {
        finalized_head,
        decoded,
    })
}

struct PreparedBlock {
    block_number: BlockNumber,
    monitor_ids: Vec<MonitorId>,
    monitors: Vec<super::monitor::Monitor>,
    results: Vec<DecodedResult>,
}

async fn prepare_block(
    chain: Chain,
    block_number: BlockNumber,
    covering: Vec<super::monitor::Monitor>,
    monitor_index: Arc<indexer::MonitorIndex>,
    source: &dyn BlockSource,
    cache: &dyn BlockCache,
    telemetry: &dyn Telemetry,
) -> anyhow::Result<PreparedBlock> {
    let _in_flight = InFlightGuard::new(telemetry, chain.id, "block");

    let has_calls = covering
        .iter()
        .any(|monitor| matches!(&monitor.target, Target::Call(_)));
    let has_events = covering
        .iter()
        .any(|monitor| matches!(&monitor.target, Target::Event(_)));
    let calls = async {
        let mut results = Vec::new();
        if has_calls {
            let block = match cache.get(chain, block_number) {
                Some(block) => {
                    telemetry.record_cache(chain.id, true);
                    block
                }
                None => {
                    telemetry.record_cache(chain.id, false);
                    let block = source.fetch_block(block_number).await?;
                    cache.put(chain, block.clone());
                    block
                }
            };
            let candidates = block
                .transactions
                .iter()
                .filter(|transaction| {
                    let selector = transaction
                        .input
                        .get(..4)
                        .and_then(|bytes| super::Selector::try_from(bytes).ok());
                    selector.is_some_and(|selector| {
                        monitor_index
                            .call(block_number, transaction.to, selector)
                            .is_some()
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            let executed = source
                .fetch_executed_transactions(&block, &candidates)
                .await?;
            results.extend(
                indexer::decode_calls(&block, monitor_index.as_ref(), executed)
                    .into_iter()
                    .map(DecodedResult::Call),
            );
        }
        Ok::<_, anyhow::Error>(results)
    };
    let events = async {
        let mut results = Vec::new();
        if has_events {
            let mut addresses = Vec::new();
            let mut topic0s = Vec::new();
            for monitor in &covering {
                if let Target::Event(target) = &monitor.target {
                    addresses.push(target.address);
                    topic0s.push(target.topic0);
                }
            }
            addresses.sort_unstable();
            addresses.dedup();
            topic0s.sort_unstable();
            topic0s.dedup();
            let logs = source
                .fetch_logs(block_number, &addresses, &topic0s)
                .await?;
            results.extend(
                indexer::decode_events(block_number, monitor_index.as_ref(), logs)?
                    .into_iter()
                    .map(DecodedResult::Event),
            );
        }
        Ok::<_, anyhow::Error>(results)
    };
    let (mut calls, events) = tokio::try_join!(calls, events)?;
    calls.extend(events);
    Ok(PreparedBlock {
        block_number,
        monitor_ids: covering.iter().map(|monitor| monitor.id).collect(),
        monitors: covering,
        results: calls,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use alloy::primitives::{Address, B256};
    use async_trait::async_trait;

    use super::*;
    use crate::filter::Filter;
    use crate::monitor::Monitor;
    use crate::ports::{BlockCache, BlockCommit, BlockSource, IndexStorage, NoopTelemetry};
    use crate::{BlockTransaction, CallTarget, Cursor, ExecutedTransaction, SourceBlock, Target};

    struct FakeStorage {
        monitor: Monitor,
        commits: Mutex<Vec<BlockCommit>>,
    }

    #[async_trait]
    impl IndexStorage for FakeStorage {
        async fn load_monitors(&self, chain: Chain) -> anyhow::Result<Vec<Monitor>> {
            Ok((self.monitor.chain == chain)
                .then(|| self.monitor.clone())
                .into_iter()
                .collect())
        }

        async fn commit_block(&self, commit: BlockCommit) -> anyhow::Result<usize> {
            let count = commit.results.len();
            self.commits.lock().unwrap().push(commit);
            Ok(count)
        }
    }

    struct FakeSource;

    #[derive(Default)]
    struct CandidateSource {
        candidates: Mutex<Vec<B256>>,
    }

    struct UnsupportedFinalizedSource;

    struct ParallelSource {
        current: AtomicUsize,
        maximum: AtomicUsize,
        fail_at: Option<BlockNumber>,
    }

    impl ParallelSource {
        fn new(fail_at: Option<BlockNumber>) -> Self {
            Self {
                current: AtomicUsize::new(0),
                maximum: AtomicUsize::new(0),
                fail_at,
            }
        }
    }

    #[derive(Default)]
    struct FakeCache {
        blocks: Mutex<HashMap<(u64, BlockNumber), SourceBlock>>,
    }

    impl BlockCache for FakeCache {
        fn get(&self, chain: Chain, block_number: BlockNumber) -> Option<SourceBlock> {
            self.blocks
                .lock()
                .unwrap()
                .get(&(chain.id, block_number))
                .cloned()
        }

        fn put(&self, chain: Chain, block: SourceBlock) {
            self.blocks
                .lock()
                .unwrap()
                .insert((chain.id, block.number), block);
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
                        to: Address::ZERO,
                        input: vec![1, 2, 3, 4],
                    },
                    BlockTransaction {
                        hash: B256::repeat_byte(2),
                        to: Address::ZERO,
                        input: vec![4, 3, 2, 1],
                    },
                    BlockTransaction {
                        hash: B256::repeat_byte(3),
                        to: Address::repeat_byte(1),
                        input: vec![1, 2, 3, 4],
                    },
                    BlockTransaction {
                        hash: B256::repeat_byte(4),
                        to: Address::ZERO,
                        input: vec![1, 2, 3],
                    },
                ],
            })
        }

        async fn fetch_executed_transactions(
            &self,
            _block: &SourceBlock,
            transactions: &[BlockTransaction],
        ) -> anyhow::Result<Vec<ExecutedTransaction>> {
            self.candidates
                .lock()
                .unwrap()
                .extend(transactions.iter().map(|transaction| transaction.hash));
            Ok(Vec::new())
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

        async fn fetch_executed_transactions(
            &self,
            _block: &SourceBlock,
            _transactions: &[BlockTransaction],
        ) -> anyhow::Result<Vec<ExecutedTransaction>> {
            unreachable!()
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

    #[tokio::test]
    async fn startup_probe_requires_finalized_tag_support() {
        assert_eq!(
            probe_source(&FakeSource).await.unwrap(),
            SourceStatus {
                chain_id: 1,
                finalized_head: 10,
            }
        );

        let error = probe_source(&UnsupportedFinalizedSource).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not support the required finalized block tag")
        );
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
        let db_writes = Semaphore::new(1);
        let telemetry = NoopTelemetry;
        let poll = run_once(
            &config,
            &storage,
            &FakeSource,
            &FakeCache::default(),
            &db_writes,
            &telemetry,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

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

        run_once(
            &config,
            &storage,
            &source,
            &FakeCache::default(),
            &Semaphore::new(1),
            &NoopTelemetry,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(*source.candidates.lock().unwrap(), [B256::repeat_byte(1)]);
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
        let db_writes = Semaphore::new(1);
        let telemetry = NoopTelemetry;

        let poll = run_once(
            &config,
            &storage,
            &FakeSource,
            &FakeCache::default(),
            &db_writes,
            &telemetry,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

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
        let db_writes = Semaphore::new(1);
        let telemetry = NoopTelemetry;

        let poll = run_once(
            &config,
            &storage,
            &FakeSource,
            &FakeCache::default(),
            &db_writes,
            &telemetry,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        status.record_success(poll.finalized_head);

        let error = run_once(
            &config,
            &storage,
            &UnsupportedFinalizedSource,
            &FakeCache::default(),
            &db_writes,
            &telemetry,
            &CancellationToken::new(),
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

        run_once(
            &config,
            &storage,
            &source,
            &FakeCache::default(),
            &Semaphore::new(1),
            &NoopTelemetry,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

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
            &storage,
            &source,
            &FakeCache::default(),
            &Semaphore::new(1),
            &NoopTelemetry,
            &CancellationToken::new(),
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
}
