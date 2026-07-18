use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::indexer;
use super::ports::{
    BlockCache, BlockCommit, BlockRange, BlockSource, InFlightGuard, IndexStorage, LogQuery,
    LogTarget, Sink, SinkBatch, Storage, Telemetry,
};
use super::status::ChainStatus;
use super::{BlockNumber, Chain, Cursor, DecodedResult, Target, scheduler};

#[derive(Debug, Clone)]
pub(crate) struct WorkerConfig {
    pub(crate) chain: Chain,
    pub(crate) batch_size: NonZeroU64,
    pub(crate) block_concurrency: NonZeroUsize,
    pub(crate) poll_interval: Duration,
}

#[derive(Clone)]
pub(crate) struct WorkerDependencies {
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) source: Arc<dyn BlockSource>,
    pub(crate) cache: Arc<dyn BlockCache>,
    pub(crate) storage_writes: Arc<Semaphore>,
    pub(crate) sink: Arc<dyn Sink>,
    pub(crate) telemetry: Arc<dyn Telemetry>,
}

#[derive(Clone, Copy)]
pub(crate) struct PollContext<'a> {
    pub(crate) storage: &'a dyn IndexStorage,
    pub(crate) source: &'a dyn BlockSource,
    pub(crate) cache: &'a dyn BlockCache,
    pub(crate) storage_writes: &'a Semaphore,
    pub(crate) sink: &'a dyn Sink,
    pub(crate) telemetry: &'a dyn Telemetry,
    pub(crate) cancel: &'a CancellationToken,
}

pub(crate) async fn run(
    config: WorkerConfig,
    dependencies: WorkerDependencies,
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
            PollContext {
                storage: dependencies.storage.as_ref(),
                source: dependencies.source.as_ref(),
                cache: dependencies.cache.as_ref(),
                storage_writes: dependencies.storage_writes.as_ref(),
                sink: dependencies.sink.as_ref(),
                telemetry: dependencies.telemetry.as_ref(),
                cancel: &cancel,
            },
        )
        .await
        {
            Ok(PollOutcome::Completed(poll)) => status.record_success(poll.finalized_head),
            Ok(PollOutcome::Cancelled) => break,
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
pub(crate) struct PollResult {
    pub(crate) finalized_head: BlockNumber,
    pub(crate) decoded: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PollOutcome {
    Completed(PollResult),
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SourceStatus {
    pub(crate) chain_id: u64,
    pub(crate) finalized_head: BlockNumber,
}

pub(crate) async fn probe_source(source: &dyn BlockSource) -> anyhow::Result<SourceStatus> {
    let chain_id = source.chain_id().await?;
    let finalized_head = source.finalized_head().await.map_err(|error| {
        anyhow::anyhow!("RPC does not support the required finalized block tag: {error:#}")
    })?;
    Ok(SourceStatus { chain_id, finalized_head })
}

pub(crate) async fn run_once(
    config: &WorkerConfig,
    context: PollContext<'_>,
) -> anyhow::Result<PollOutcome> {
    let PollContext { storage, source, cache, telemetry, cancel, .. } = context;
    let finalized_head = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Ok(PollOutcome::Cancelled),
        finalized_head = source.finalized_head() => finalized_head?,
    };
    let monitors = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Ok(PollOutcome::Cancelled),
        monitors = storage.load_monitors(config.chain) => monitors?,
    };
    let monitor_index = Arc::new(indexer::MonitorIndex::new(monitors)?);
    let active = monitor_index.monitors();
    if active.is_empty() {
        if cancel.is_cancelled() {
            return Ok(PollOutcome::Cancelled);
        }
        telemetry.set_worker_lag(config.chain.id, 0);
        return Ok(PollOutcome::Completed(PollResult { finalized_head, decoded: 0 }));
    }

    let plans = scheduler::plan_blocks(active, finalized_head, config.batch_size);
    let mut decoded = 0;
    let mut progress = active.iter().map(|monitor| monitor.cursor.0).collect::<Vec<_>>();

    for window in plan_windows(plans, config.batch_size) {
        if let Some(query) = log_query(&window, monitor_index.as_ref())? {
            let prepared = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(PollOutcome::Cancelled),
                prepared = prepare_event_window(
                    config,
                    context,
                    window,
                    query,
                    monitor_index.clone(),
                ) => prepared?,
            };
            for prepared in prepared {
                match commit_prepared(config.chain, context, prepared?, &mut progress).await? {
                    CommitOutcome::Committed(count) => decoded += count,
                    CommitOutcome::Cancelled => return Ok(PollOutcome::Cancelled),
                }
            }
        } else {
            let mut prepared = super::pipeline::ordered(
                window.into_iter().map(|plan| {
                    let monitor_index = monitor_index.clone();
                    async move {
                        prepare_calls(config.chain, plan, monitor_index, source, cache, telemetry)
                            .await
                    }
                }),
                config.block_concurrency.get(),
            );
            loop {
                let next = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Ok(PollOutcome::Cancelled),
                    next = prepared.next() => next,
                };
                let Some(prepared) = next else { break };
                let prepared = finish_prepared(prepared?, monitor_index.as_ref(), Vec::new())?;
                match commit_prepared(config.chain, context, prepared, &mut progress).await? {
                    CommitOutcome::Committed(count) => decoded += count,
                    CommitOutcome::Cancelled => return Ok(PollOutcome::Cancelled),
                }
            }
        }
    }

    if let Some(min_next) = active
        .iter()
        .zip(&progress)
        .map(|(monitor, cursor)| Cursor(*cursor).next(monitor.start_block))
        .min()
    {
        cache.evict_before(config.chain, min_next);
    }
    let lag = active
        .iter()
        .zip(&progress)
        .map(|(monitor, cursor)| {
            let target = monitor.end_block.unwrap_or(finalized_head).min(finalized_head);
            match *cursor {
                Some(cursor) => target.saturating_sub(cursor),
                None if monitor.start_block <= target => {
                    target.saturating_sub(monitor.start_block).saturating_add(1)
                }
                None => 0,
            }
        })
        .max()
        .unwrap_or(0);
    telemetry.set_worker_lag(config.chain.id, lag);
    Ok(PollOutcome::Completed(PollResult { finalized_head, decoded }))
}

struct PreparedBlock {
    block_number: BlockNumber,
    monitor_indices: Vec<usize>,
    monitors: Vec<Arc<super::monitor::Monitor>>,
    results: Vec<DecodedResult>,
}

struct PreparedCalls {
    plan: scheduler::BlockPlan,
    results: Vec<DecodedResult>,
}

enum CommitOutcome {
    Committed(usize),
    Cancelled,
}

async fn commit_prepared(
    chain: Chain,
    context: PollContext<'_>,
    prepared: PreparedBlock,
    progress: &mut [Option<BlockNumber>],
) -> anyhow::Result<CommitOutcome> {
    let calls =
        prepared.results.iter().filter(|result| matches!(result, DecodedResult::Call(_))).count()
            as u64;
    let events = prepared.results.len() as u64 - calls;
    let commit = BlockCommit {
        chain,
        block_number: prepared.block_number,
        monitors: prepared.monitors,
        results: prepared.results,
    };
    let permit = tokio::select! {
        biased;
        _ = context.cancel.cancelled() => return Ok(CommitOutcome::Cancelled),
        permit = context.storage_writes.acquire() => permit?,
    };
    let in_flight = InFlightGuard::new(context.telemetry, chain.id, "storage");
    let started = std::time::Instant::now();
    let result = context.storage.commit_block(&commit).await;
    drop(in_flight);
    drop(permit);
    match result {
        Ok(()) => {
            context.telemetry.record_commit(chain.id, calls, events, "success", started.elapsed());
            if context.sink.enabled() {
                match SinkBatch::new(chain, commit.block_number, &commit.monitors, &commit.results)
                {
                    Ok(Some(batch)) => context.sink.submit(batch),
                    Ok(None) => {}
                    Err(error) => tracing::warn!(
                        chain_id = chain.id,
                        block_number = commit.block_number,
                        %error,
                        "failed to encode committed sink batch"
                    ),
                }
            }
            for monitor_index in prepared.monitor_indices {
                let cursor = progress
                    .get_mut(monitor_index)
                    .ok_or_else(|| anyhow::anyhow!("block plan references unknown monitor"))?;
                *cursor = Some(prepared.block_number);
            }
            Ok(CommitOutcome::Committed(commit.results.len()))
        }
        Err(error) => {
            context.telemetry.record_commit(chain.id, calls, events, "error", started.elapsed());
            Err(error)
        }
    }
}

fn plan_windows(
    plans: Vec<scheduler::BlockPlan>,
    max_blocks: NonZeroU64,
) -> Vec<Vec<scheduler::BlockPlan>> {
    let max_blocks = usize::try_from(max_blocks.get()).unwrap_or(usize::MAX);
    let mut windows = Vec::<Vec<scheduler::BlockPlan>>::new();
    for plan in plans {
        let append = windows.last().is_some_and(|window| {
            window.len() < max_blocks
                && window.last().and_then(|last| last.block_number.checked_add(1))
                    == Some(plan.block_number)
        });
        if append {
            windows.last_mut().expect("window exists").push(plan);
        } else {
            windows.push(vec![plan]);
        }
    }
    windows
}

async fn prepare_event_window(
    config: &WorkerConfig,
    context: PollContext<'_>,
    plans: Vec<scheduler::BlockPlan>,
    query: LogQuery,
    monitor_index: Arc<indexer::MonitorIndex>,
) -> anyhow::Result<Vec<anyhow::Result<PreparedBlock>>> {
    let calls = async {
        let mut prepared = super::pipeline::ordered(
            plans.iter().cloned().map(|plan| {
                let monitor_index = monitor_index.clone();
                async move {
                    prepare_calls(
                        config.chain,
                        plan,
                        monitor_index,
                        context.source,
                        context.cache,
                        context.telemetry,
                    )
                    .await
                }
            }),
            config.block_concurrency.get(),
        );
        let mut calls = Vec::with_capacity(plans.len());
        while let Some(prepared) = prepared.next().await {
            calls.push(prepared);
        }
        Ok::<_, anyhow::Error>(calls)
    };
    let events = async move {
        let range = query.range();
        let mut by_block = BTreeMap::<_, Vec<_>>::new();
        for log in context.source.fetch_logs(query).await? {
            anyhow::ensure!(!log.removed, "removed log returned for finalized block range");
            let block_number =
                log.block_number.ok_or_else(|| anyhow::anyhow!("log is missing block number"))?;
            anyhow::ensure!(
                (range.start()..=range.end()).contains(&block_number),
                "log block number is outside the requested range"
            );
            by_block.entry(block_number).or_default().push(log);
        }
        Ok::<_, anyhow::Error>(by_block)
    };
    let (calls, mut logs) = tokio::try_join!(calls, events)?;
    Ok(calls
        .into_iter()
        .map(|prepared| {
            let prepared = prepared?;
            let block_number = prepared.plan.block_number;
            finish_prepared(
                prepared,
                monitor_index.as_ref(),
                logs.remove(&block_number).unwrap_or_default(),
            )
        })
        .collect::<Vec<anyhow::Result<_>>>())
}

fn finish_prepared(
    mut prepared: PreparedCalls,
    monitor_index: &indexer::MonitorIndex,
    logs: Vec<super::SourceLog>,
) -> anyhow::Result<PreparedBlock> {
    prepared.results.extend(
        indexer::decode_events(
            prepared.plan.block_number,
            monitor_index,
            &prepared.plan.monitor_indices,
            logs,
        )?
        .into_iter()
        .map(DecodedResult::Event),
    );
    let monitors = prepared
        .plan
        .monitor_indices
        .iter()
        .map(|index| {
            monitor_index
                .monitor(*index)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("block plan references unknown monitor"))
        })
        .collect::<anyhow::Result<_>>()?;
    Ok(PreparedBlock {
        block_number: prepared.plan.block_number,
        monitor_indices: prepared.plan.monitor_indices,
        monitors,
        results: prepared.results,
    })
}

fn log_query(
    plans: &[scheduler::BlockPlan],
    monitors: &indexer::MonitorIndex,
) -> anyhow::Result<Option<LogQuery>> {
    let Some((first, last)) = plans.first().zip(plans.last()) else { return Ok(None) };
    let mut targets = Vec::new();
    for plan in plans {
        for monitor_index in &plan.monitor_indices {
            let monitor = monitors
                .monitor(*monitor_index)
                .ok_or_else(|| anyhow::anyhow!("block plan references unknown monitor"))?;
            if let Target::Event(target) = &monitor.target {
                targets.push(LogTarget::new(target.address, target.topic0));
            }
        }
    }
    if targets.is_empty() {
        return Ok(None);
    }
    let range = BlockRange::new(first.block_number, last.block_number)
        .ok_or_else(|| anyhow::anyhow!("block plans are not ordered"))?;
    Ok(Some(LogQuery::new(range, targets)))
}

async fn prepare_calls(
    chain: Chain,
    plan: scheduler::BlockPlan,
    monitor_index: Arc<indexer::MonitorIndex>,
    source: &dyn BlockSource,
    cache: &dyn BlockCache,
    telemetry: &dyn Telemetry,
) -> anyhow::Result<PreparedCalls> {
    let _in_flight = InFlightGuard::new(telemetry, chain.id, "block");
    let has_calls = plan.monitor_indices.iter().any(|index| {
        monitor_index
            .monitor(*index)
            .is_some_and(|monitor| matches!(&monitor.target, Target::Call(_)))
    });
    if !has_calls {
        return Ok(PreparedCalls { plan, results: Vec::new() });
    }
    let block = match cache.get(chain, plan.block_number) {
        Some(block) => {
            telemetry.record_cache(chain.id, true);
            block
        }
        None => {
            telemetry.record_cache(chain.id, false);
            let block = Arc::new(source.fetch_block(plan.block_number).await?);
            cache.put(chain, block.clone());
            block
        }
    };
    let candidates = block
        .transactions
        .iter()
        .enumerate()
        .filter_map(|(index, transaction)| {
            let selector = transaction
                .input
                .get(..4)
                .and_then(|bytes| super::Selector::try_from(bytes).ok())?;
            monitor_index.call(plan.block_number, transaction.to, selector).and_then(
                |(monitor_index, _, _)| {
                    plan.monitor_indices.binary_search(&monitor_index).is_ok().then_some(index)
                },
            )
        })
        .collect::<Vec<_>>();
    let hashes = candidates.iter().map(|index| block.transactions[*index].hash).collect::<Vec<_>>();
    let outcomes = if hashes.is_empty() {
        Vec::new()
    } else {
        source.fetch_execution_outcomes(plan.block_number, &hashes).await?
    };
    let results = indexer::decode_calls(
        &block,
        monitor_index.as_ref(),
        &plan.monitor_indices,
        &candidates,
        outcomes,
    )?
    .into_iter()
    .map(DecodedResult::Call)
    .collect();
    Ok(PreparedCalls { plan, results })
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
    use crate::ports::{
        BlockCache, BlockCommit, BlockSource, IndexStorage, NoopSink, NoopTelemetry,
    };
    use crate::{
        BlockTransaction, CallTarget, Cursor, EventTarget, ExecutionOutcome, MonitorId,
        SourceBlock, Target,
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
}
