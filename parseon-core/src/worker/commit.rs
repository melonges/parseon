//! Atomic block commit: storage write, sink submission, and cursor
//! advancement.
//!
//! [`commit_prepared`] is the single entry point. It acquires a storage-write
//! permit, calls [`IndexStorage::commit_block`], records telemetry, submits a
//! sink batch if the sink is enabled, and advances the in-memory progress
//! vector for every covering monitor. Cancellation is checked before the
//! permit is acquired; an in-progress commit is always allowed to finish so
//! storage never observes a half-committed block.

use super::prepare::PreparedBlock;
use super::{BlockNumber, Chain, PollContext};
use crate::DecodedResult;
use crate::ports::{BlockCommit, InFlightGuard, SinkBatch};

/// Outcome of a commit attempt.
pub(super) enum CommitOutcome {
    /// The block was committed successfully. Carries the number of decoded
    /// results in the committed batch.
    Committed(usize),
    /// The commit was skipped because cancellation was observed before the
    /// storage-write permit was acquired.
    Cancelled,
}

/// Commits `prepared` atomically and advances `progress` for every covering
/// monitor.
///
/// `progress[i]` is the in-memory cursor for monitor `i` in the
/// [`super::indexer::MonitorIndex`]. On success, every index in
/// `prepared.monitor_indices` is set to `Some(prepared.block_number)`.
pub(super) async fn commit_prepared(
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
