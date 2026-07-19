//! Block preparation: fetching, decoding, and window planning.
//!
//! This module owns the read-and-decode side of the worker pipeline:
//! - [`plan_windows`] groups contiguous block plans into windows of at most
//!   `batch_size` blocks so the block source can fetch logs for an entire
//!   window in one request.
//! - [`log_query`] builds an exact-target [`LogQuery`] for one window.
//! - [`prepare_calls`] fetches one block, finds matching call targets, fetches
//!   their execution outcomes, and decodes the calldata.
//! - [`prepare_event_window`] concurrently prepares calls for every block in a
//!   window while fetching the window's logs in one request.
//! - [`finish_prepared`] attaches decoded events to a prepared-calls block and
//!   resolves the monitor Arcs needed for the commit.

use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::sync::Arc;

use futures_util::StreamExt;

use super::indexer::{self, MonitorIndex};
use super::scheduler::{self, BlockPlan};
use super::{PollContext, WorkerConfig};
use crate::monitor::Monitor;
use crate::pipeline;
use crate::ports::{
    BlockCache, BlockRange, BlockSource, InFlightGuard, LogQuery, LogTarget, Telemetry,
};
use crate::{BlockNumber, Chain, DecodedResult, Selector, SourceLog, Target};

/// One block's prepared results together with the monitor indices that cover
/// it and the resolved monitor Arcs needed for the commit.
pub(super) struct PreparedBlock {
    /// Block number being committed.
    pub(super) block_number: BlockNumber,
    /// Indices of monitors in [`MonitorIndex`] whose cursors must advance to
    /// `block_number`.
    pub(super) monitor_indices: Vec<usize>,
    /// Resolved monitor Arcs for [`crate::ports::BlockCommit`].
    pub(super) monitors: Vec<Arc<Monitor>>,
    /// Decoded calls and events for this block.
    pub(super) results: Vec<DecodedResult>,
}

/// One block's prepared call results (events are attached later by
/// [`finish_prepared`]).
pub(super) struct PreparedCalls {
    plan: scheduler::BlockPlan,
    results: Vec<DecodedResult>,
}

/// Groups contiguous block plans into windows of at most `max_blocks` blocks.
///
/// A window is a maximal run of plans whose block numbers are consecutive and
/// whose length does not exceed `max_blocks`. Event-log fetching uses one
/// ranged `eth_getLogs` request per window.
pub(super) fn plan_windows(
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

/// Builds an exact-target [`LogQuery`] for one window, or `None` if no monitor
/// in the window targets events.
pub(super) fn log_query(
    plans: &[scheduler::BlockPlan],
    monitors: &MonitorIndex,
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

/// Fetches one block, identifies call candidates, fetches their execution
/// outcomes, and decodes the matching calls.
///
/// Returns an empty `results` vector if no monitor in `plan.monitor_indices`
/// targets a call (the block fetch is skipped in that case).
pub(super) async fn prepare_calls(
    chain: Chain,
    plan: scheduler::BlockPlan,
    monitor_index: Arc<MonitorIndex>,
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
            let selector =
                transaction.input.get(..4).and_then(|bytes| Selector::try_from(bytes).ok())?;
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

/// Concurrently prepares calls for every block in a window while fetching the
/// window's logs in one ranged request.
///
/// Returns one [`PreparedBlock`] per plan, in input order. Each prepared block
/// has its decoded events attached via [`finish_prepared`].
pub(super) async fn prepare_event_window(
    config: &WorkerConfig,
    context: PollContext<'_>,
    plans: Vec<BlockPlan>,
    query: LogQuery,
    monitor_index: Arc<MonitorIndex>,
) -> anyhow::Result<Vec<anyhow::Result<PreparedBlock>>> {
    let calls = async {
        let mut prepared = pipeline::ordered(
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

/// Attaches decoded events to a prepared-calls block and resolves the monitor
/// Arcs needed for the commit.
pub(super) fn finish_prepared(
    mut prepared: PreparedCalls,
    monitor_index: &MonitorIndex,
    logs: Vec<SourceLog>,
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
