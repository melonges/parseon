//! Per-chain worker runtime: polls canonical blocks, decodes calls and events,
//! and commits results atomically with provisional/finalized lifecycle.
//!
//! The worker is the heart of the indexing pipeline. One worker runs per
//! enabled chain, polling the block source at `poll_interval` and processing
//! up to `batch_size` blocks per poll. Canonical tips are checked before new
//! work so bounded reorgs can rollback and replay safely. Within a poll, blocks are prepared
//! concurrently (capped by `block_concurrency`) and committed in block-number
//! order so cursors never advance past a gap.
//!
//! ## Module layout
//!
//! - [`prepare`]: block fetching, call/event decoding, and window planning.
//! - [`commit`]: atomic block commits with telemetry, sink submission, and
//!   cursor advancement.
//! - `tests`: the worker's integration test suite (compiled only under
//!   `#[cfg(test)]`).

mod commit;
mod prepare;
#[cfg(test)]
mod tests;

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures_util::StreamExt;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::indexer;
use super::scheduler;
use super::status::ChainStatus;
use super::{BlockNumber, Chain, Cursor, Finality};
use crate::ports::{BlockCache, BlockSource, IndexStorage, Sink, Storage, Telemetry};

/// Per-worker static configuration, set at supervisor startup.
#[derive(Debug, Clone)]
pub(crate) struct WorkerConfig {
    /// Chain this worker indexes.
    pub(crate) chain: Chain,
    /// Maximum number of blocks processed per monitor per poll.
    pub(crate) batch_size: NonZeroU64,
    /// Maximum number of blocks prepared concurrently within one poll.
    pub(crate) block_concurrency: NonZeroUsize,
    /// Delay between successful polls.
    pub(crate) poll_interval: Duration,
    /// Number of latest blocks required before promotion.
    pub(crate) confirmations: NonZeroU64,
    /// Maximum number of canonical blocks retained for ancestor search.
    pub(crate) rollback_retention: NonZeroU64,
}

/// Per-worker dependencies cloned from the supervisor.
#[derive(Clone)]
pub(crate) struct WorkerDependencies {
    /// Composite storage adapter (chains, monitors, results, commits).
    pub(crate) storage: Arc<dyn Storage>,
    /// Finalized EVM block source.
    pub(crate) source: Arc<dyn BlockSource>,
    /// Per-worker block cache.
    pub(crate) cache: Arc<dyn BlockCache>,
    /// Process-wide storage write concurrency limiter.
    pub(crate) storage_writes: Arc<Semaphore>,
    /// Optional post-commit sink.
    pub(crate) sink: Arc<dyn Sink>,
    /// Telemetry collector.
    pub(crate) telemetry: Arc<dyn Telemetry>,
}

/// Borrowed view of [`WorkerDependencies`] for one poll.
///
/// Constructed fresh in each iteration of [`run`]; lets the poll functions
/// borrow the dependencies without propagating the `Arc` clones.
#[derive(Clone, Copy)]
pub(crate) struct PollContext<'a> {
    /// Storage (only the index-storage role is needed during a poll).
    pub(crate) storage: &'a dyn IndexStorage,
    /// Block source.
    pub(crate) source: &'a dyn BlockSource,
    /// Block cache.
    pub(crate) cache: &'a dyn BlockCache,
    /// Storage write semaphore.
    pub(crate) storage_writes: &'a Semaphore,
    /// Post-commit sink.
    pub(crate) sink: &'a dyn Sink,
    /// Telemetry.
    pub(crate) telemetry: &'a dyn Telemetry,
    /// Cancellation token for the worker.
    pub(crate) cancel: &'a CancellationToken,
}

/// Aggregate result of one successful poll.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PollResult {
    /// Latest head observed at the start of the poll.
    pub(crate) latest_head: BlockNumber,
    /// Finalized head observed at the start of the poll.
    pub(crate) finalized_head: BlockNumber,
    /// Highest block safe to expose as finalized.
    pub(crate) promotion_height: BlockNumber,
    /// Highest canonical block retained after this poll.
    pub(crate) canonical_head: Option<BlockNumber>,
    /// Number of decoded results committed during this poll.
    pub(crate) decoded: usize,
}

/// Outcome of one poll iteration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PollOutcome {
    /// The poll completed successfully.
    Completed(PollResult),
    /// The poll was interrupted by cancellation. No partial state was
    /// committed past the cancellation point.
    Cancelled,
}

/// Source probe result: the endpoint's chain ID and finalized head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SourceStatus {
    /// EIP-155 chain ID reported by the endpoint.
    pub(crate) chain_id: u64,
    /// Current latest head block number.
    pub(crate) latest_head: BlockNumber,
    /// Current finalized head block number.
    pub(crate) finalized_head: BlockNumber,
}

/// Probes a block source for its chain ID and finalized-head support.
///
/// Used by the supervisor at startup and by the chain service when a chain is
/// registered or updated. Returns an error if the endpoint does not support
/// the `finalized` block tag.
pub(crate) async fn probe_source(source: &dyn BlockSource) -> anyhow::Result<SourceStatus> {
    let chain_id = source.chain_id().await?;
    let finalized_head = source.finalized_head().await.map_err(|error| {
        anyhow::anyhow!("RPC does not support the required finalized block tag: {error:#}")
    })?;
    let latest_head = source.latest_head().await?;
    Ok(SourceStatus { chain_id, latest_head, finalized_head })
}

/// Runs the worker until `cancel` is cancelled.
///
/// Each iteration polls once via [`run_once`], records the outcome on `status`,
/// and sleeps for `config.poll_interval` (interruptible by cancellation) before
/// the next poll.
pub(crate) async fn run(
    config: WorkerConfig,
    dependencies: WorkerDependencies,
    status: ChainStatus,
    cancel: CancellationToken,
) {
    dependencies.telemetry.set_worker_state(config.chain.id, "starting");
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
            Ok(PollOutcome::Completed(poll)) => {
                status.record_success_heads(
                    poll.latest_head,
                    poll.finalized_head,
                    poll.promotion_height,
                    poll.canonical_head,
                );
                dependencies.telemetry.set_worker_state(config.chain.id, "running");
                dependencies
                    .telemetry
                    .set_worker_last_successful_poll(config.chain.id, Utc::now().timestamp());
            }
            Ok(PollOutcome::Cancelled) => break,
            Err(error) => {
                let blocked = is_blocking_reorg_error(&error);
                let message = if blocked {
                    dependencies.telemetry.set_worker_state(config.chain.id, "blocked");
                    status.record_blocked(&error)
                } else {
                    dependencies.telemetry.set_worker_state(config.chain.id, "degraded");
                    status.record_error(&error)
                };
                tracing::warn!(chain_id = config.chain.id, error = %message, "worker tick failed");
                if blocked {
                    break;
                }
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(config.poll_interval) => {}
            _ = cancel.cancelled() => break,
        }
    }
    tracing::info!(chain_id = config.chain.id, "worker stopped");
}

/// Runs one poll: loads monitors, plans blocks, prepares them concurrently,
/// commits them in order, and updates the cache and telemetry.
///
/// Returns [`PollOutcome::Cancelled`] if cancellation interrupted any stage;
/// partial state committed before the cancellation point is preserved.
pub(crate) async fn run_once(
    config: &WorkerConfig,
    context: PollContext<'_>,
) -> anyhow::Result<PollOutcome> {
    let PollContext { storage, source, cache, telemetry, cancel, .. } = context;
    let latest_head = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Ok(PollOutcome::Cancelled),
        latest_head = source.latest_head() => latest_head?,
    };
    let finalized_head = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Ok(PollOutcome::Cancelled),
        finalized_head = source.finalized_head() => finalized_head?,
    };
    anyhow::ensure!(
        finalized_head <= latest_head,
        "source finalized head {finalized_head} exceeds latest head {latest_head}"
    );
    let promotion_height = promotion_height(latest_head, finalized_head, config.confirmations);
    if reconcile_canonical_tip(config, context, latest_head).await? {
        return Ok(PollOutcome::Cancelled);
    }
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
        let batches = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(PollOutcome::Cancelled),
            batches = storage.promote_finalized(config.chain, promotion_height) => batches?,
        };
        for batch in batches {
            if cancel.is_cancelled() {
                return Ok(PollOutcome::Cancelled);
            }
            if !context.sink.enabled() {
                break;
            }
            context.sink.submit(batch);
        }
        return Ok(PollOutcome::Completed(PollResult {
            latest_head,
            finalized_head,
            promotion_height,
            canonical_head: tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(PollOutcome::Cancelled),
                tip = storage.canonical_tip(config.chain) => tip?.map(|block| block.metadata.number),
            },
            decoded: 0,
        }));
    }

    let plans = scheduler::plan_blocks(active, latest_head, config.batch_size);
    let mut decoded = 0;
    let mut progress = active.iter().map(|monitor| monitor.cursor.0).collect::<Vec<_>>();

    for window in prepare::plan_windows(plans, config.batch_size) {
        if let Some(query) = prepare::log_query(&window, monitor_index.as_ref())? {
            let prepared = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(PollOutcome::Cancelled),
                prepared = prepare::prepare_event_window(
                    config,
                    context,
                    window,
                    query,
                    monitor_index.clone(),
                ) => prepared?,
            };
            for prepared in prepared {
                let prepared = prepared?;
                let finality = Finality::Provisional;
                match commit::commit_prepared(
                    config.chain,
                    context,
                    prepared,
                    finality,
                    &mut progress,
                )
                .await?
                {
                    commit::CommitOutcome::Committed(count) => decoded += count,
                    commit::CommitOutcome::Cancelled => return Ok(PollOutcome::Cancelled),
                }
            }
        } else {
            let mut prepared = super::pipeline::ordered(
                window.into_iter().map(|plan| {
                    let monitor_index = monitor_index.clone();
                    async move {
                        prepare::prepare_calls(
                            config.chain,
                            plan,
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
            loop {
                let next = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Ok(PollOutcome::Cancelled),
                    next = prepared.next() => next,
                };
                let Some(prepared) = next else { break };
                let prepared =
                    prepare::finish_prepared(prepared?, monitor_index.as_ref(), Vec::new())?;
                let finality = Finality::Provisional;
                match commit::commit_prepared(
                    config.chain,
                    context,
                    prepared,
                    finality,
                    &mut progress,
                )
                .await?
                {
                    commit::CommitOutcome::Committed(count) => decoded += count,
                    commit::CommitOutcome::Cancelled => return Ok(PollOutcome::Cancelled),
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
            let target = monitor.end_block.unwrap_or(latest_head).min(latest_head);
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
    let batches = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Ok(PollOutcome::Cancelled),
        batches = storage.promote_finalized(config.chain, promotion_height) => batches?,
    };
    for batch in batches {
        if cancel.is_cancelled() {
            return Ok(PollOutcome::Cancelled);
        }
        if context.sink.enabled() {
            context.sink.submit(batch);
        }
    }
    telemetry.set_worker_lag(config.chain.id, lag);
    let canonical_head = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Ok(PollOutcome::Cancelled),
        tip = storage.canonical_tip(config.chain) => tip?.map(|block| block.metadata.number),
    };
    Ok(PollOutcome::Completed(PollResult {
        latest_head,
        finalized_head,
        promotion_height,
        canonical_head,
        decoded,
    }))
}

fn is_blocking_reorg_error(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("promoted finalized boundary")
        || message.contains("canonical ancestor not found within rollback retention")
}

fn promotion_height(
    latest_head: BlockNumber,
    finalized_head: BlockNumber,
    confirmations: NonZeroU64,
) -> BlockNumber {
    finalized_head.min(latest_head.saturating_sub(confirmations.get()))
}

async fn reconcile_canonical_tip(
    config: &WorkerConfig,
    context: PollContext<'_>,
    latest_head: BlockNumber,
) -> anyhow::Result<bool> {
    if context.cancel.is_cancelled() {
        return Ok(true);
    }
    let tip = tokio::select! {
        biased;
        _ = context.cancel.cancelled() => return Ok(true),
        tip = context.storage.canonical_tip(config.chain) => tip?,
    };
    let Some(tip) = tip else {
        return Ok(false);
    };
    if latest_head < tip.metadata.number {
        return Ok(false);
    }
    let mut number = tip.metadata.number;
    let retention = config.rollback_retention.get();
    for distance in 0..=retention {
        let source = tokio::select! {
            biased;
            _ = context.cancel.cancelled() => return Ok(true),
            source = context.source.fetch_block_header(number) => source?,
        };
        let stored = tokio::select! {
            biased;
            _ = context.cancel.cancelled() => return Ok(true),
            stored = context.storage.canonical_block(config.chain, number) => stored?,
        };
        if stored.is_some_and(|stored| {
            stored.metadata.hash == source.hash && stored.metadata.parent_hash == source.parent_hash
        }) {
            if number < tip.metadata.number {
                let rollback = tokio::select! {
                    biased;
                    _ = context.cancel.cancelled() => return Ok(true),
                    rollback = context.storage.rollback_to(config.chain, number) => rollback,
                };
                if let Err(error) = rollback {
                    if error.to_string().contains("promoted finalized boundary") {
                        anyhow::bail!(
                            "reorg recovery crossed promoted finalized boundary: {error}"
                        );
                    }
                    return Err(error);
                }
                if context.cancel.is_cancelled() {
                    return Ok(true);
                }
                context.cache.evict_after(config.chain, number);
            }
            return Ok(false);
        }
        if distance == retention || number == 0 {
            break;
        }
        number -= 1;
    }
    anyhow::bail!("canonical ancestor not found within rollback retention")
}
