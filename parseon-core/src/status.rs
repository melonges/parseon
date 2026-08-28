//! In-process worker status snapshots.
//!
//! Each chain worker maintains a [`ChainStatus`] that the HTTP API reads via
//! [`RuntimeStatus::snapshot`]. Status updates are frequent (every poll) and
//! reads are frequent (every `/status` request), so [`ChainStatus`] uses
//! lock-free [`ArcSwap`] for its snapshot and [`RuntimeStatus`] uses a
//! `parking_lot` RwLock for the chain-keyed map.

use std::collections::BTreeMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use parking_lot::RwLock;

use crate::{BlockNumber, ChainId};
use chrono::{DateTime, Utc};

/// Lifecycle state of one chain worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    /// The worker has started but has not completed its first poll.
    Starting,
    /// The worker's latest poll succeeded.
    Running,
    /// Source validation or the worker's latest poll failed.
    Degraded,
    /// Reorg recovery crossed an already promoted finalized boundary and the
    /// worker is halted until operator recovery.
    Blocked,
    /// The chain is disabled and has no worker.
    Disabled,
}

/// A point-in-time view of one chain worker's state.
#[derive(Debug, Clone)]
pub struct ChainStatusSnapshot {
    /// EIP-155 chain ID.
    pub chain_id: ChainId,
    /// Whether the chain runs a worker.
    pub enabled: bool,
    /// Latest head observed by the worker, if any.
    pub latest_head: Option<BlockNumber>,
    /// Highest canonical block persisted by the worker, if any.
    pub canonical_head: Option<BlockNumber>,
    /// Latest finalized head observed by the worker, if any.
    pub finalized_head: Option<BlockNumber>,
    /// Highest block eligible for finalized promotion.
    pub promotion_height: Option<BlockNumber>,
    /// Current worker lifecycle state.
    pub worker_state: WorkerState,
    /// When the worker's last poll succeeded, if ever.
    pub last_successful_poll_at: Option<DateTime<Utc>>,
    /// Last error message, if the worker is degraded. Private endpoint URLs
    /// are scrubbed before this is set.
    pub last_error: Option<String>,
}

/// Lock-free, chain-scoped worker status.
///
/// `record_success` and `record_error` replace the snapshot atomically;
/// `snapshot` loads the current snapshot without taking a lock.
#[derive(Debug, Clone)]
pub struct ChainStatus {
    inner: Arc<ArcSwap<ChainStatusSnapshot>>,
}

impl ChainStatus {
    /// Creates a status in the [`WorkerState::Starting`] state with an optional
    /// `finalized_head` observed during source validation.
    pub fn starting(chain_id: ChainId, finalized_head: Option<BlockNumber>) -> Self {
        Self::new(ChainStatusSnapshot {
            chain_id,
            enabled: true,
            latest_head: None,
            canonical_head: None,
            finalized_head,
            promotion_height: None,
            worker_state: WorkerState::Starting,
            last_successful_poll_at: None,
            last_error: None,
        })
    }

    /// Creates a status in the [`WorkerState::Running`] state with the given
    /// `finalized_head`. Used in tests.
    #[cfg(test)]
    pub fn running(chain_id: ChainId, finalized_head: BlockNumber) -> Self {
        Self::new(ChainStatusSnapshot {
            chain_id,
            enabled: true,
            latest_head: Some(finalized_head),
            canonical_head: Some(finalized_head),
            finalized_head: Some(finalized_head),
            promotion_height: Some(finalized_head),
            worker_state: WorkerState::Running,
            last_successful_poll_at: Some(Utc::now()),
            last_error: None,
        })
    }

    /// Creates a status in the [`WorkerState::Disabled`] state. The supervisor
    /// uses this for chains registered with `enabled = false`.
    pub fn disabled(chain_id: ChainId) -> Self {
        Self::new(ChainStatusSnapshot {
            chain_id,
            enabled: false,
            latest_head: None,
            canonical_head: None,
            finalized_head: None,
            promotion_height: None,
            worker_state: WorkerState::Disabled,
            last_successful_poll_at: None,
            last_error: None,
        })
    }

    /// Creates a status in the [`WorkerState::Degraded`] state with `message`
    /// as the last error. The supervisor uses this when source validation
    /// fails before a worker starts.
    pub fn degraded(chain_id: ChainId, message: impl Into<String>) -> Self {
        Self::new(ChainStatusSnapshot {
            chain_id,
            enabled: true,
            latest_head: None,
            canonical_head: None,
            finalized_head: None,
            promotion_height: None,
            worker_state: WorkerState::Degraded,
            last_successful_poll_at: None,
            last_error: Some(message.into()),
        })
    }

    fn new(snapshot: ChainStatusSnapshot) -> Self {
        Self { inner: Arc::new(ArcSwap::from_pointee(snapshot)) }
    }

    /// Atomically records a successful poll at `finalized_head`, clearing any
    /// prior error and transitioning to [`WorkerState::Running`].
    pub fn record_success(&self, finalized_head: BlockNumber) {
        self.record_success_heads(
            finalized_head,
            finalized_head,
            finalized_head,
            Some(finalized_head),
        );
    }

    /// Records latest, canonical and finality heads after a successful poll.
    pub fn record_success_heads(
        &self,
        latest_head: BlockNumber,
        finalized_head: BlockNumber,
        promotion_height: BlockNumber,
        canonical_head: Option<BlockNumber>,
    ) {
        let prev = self.inner.load();
        let next = ChainStatusSnapshot {
            chain_id: prev.chain_id,
            enabled: prev.enabled,
            latest_head: Some(latest_head),
            canonical_head,
            finalized_head: Some(finalized_head),
            promotion_height: Some(promotion_height),
            worker_state: WorkerState::Running,
            last_successful_poll_at: Some(Utc::now()),
            last_error: None,
        };
        self.inner.store(Arc::new(next));
    }

    /// Atomically records a worker error, transitioning to
    /// [`WorkerState::Degraded`] and storing a scrubbed `message`. Returns the
    /// scrubbed message so the caller can log it without re-running the
    /// scrubbing logic.
    pub fn record_error(&self, error: &anyhow::Error) -> String {
        let message = safe_error_message(error);
        let prev = self.inner.load();
        let next = ChainStatusSnapshot {
            chain_id: prev.chain_id,
            enabled: prev.enabled,
            latest_head: prev.latest_head,
            canonical_head: prev.canonical_head,
            finalized_head: prev.finalized_head,
            promotion_height: prev.promotion_height,
            worker_state: WorkerState::Degraded,
            last_successful_poll_at: prev.last_successful_poll_at,
            last_error: Some(message.clone()),
        };
        self.inner.store(Arc::new(next));
        message
    }

    /// Records that the worker task exited without an intentional cancellation.
    /// This keeps readiness failed instead of leaving a stale `running` snapshot.
    pub fn record_task_exit(&self) {
        let prev = self.inner.load();
        self.inner.store(Arc::new(ChainStatusSnapshot {
            chain_id: prev.chain_id,
            enabled: prev.enabled,
            latest_head: prev.latest_head,
            canonical_head: prev.canonical_head,
            finalized_head: prev.finalized_head,
            promotion_height: prev.promotion_height,
            worker_state: WorkerState::Degraded,
            last_successful_poll_at: prev.last_successful_poll_at,
            last_error: Some("worker task exited unexpectedly".to_string()),
        }));
    }

    /// Records a fail-closed recovery state that requires operator action.
    pub fn record_blocked(&self, error: &anyhow::Error) -> String {
        let message = safe_error_message(error);
        let prev = self.inner.load();
        self.inner.store(Arc::new(ChainStatusSnapshot {
            chain_id: prev.chain_id,
            enabled: prev.enabled,
            latest_head: prev.latest_head,
            canonical_head: prev.canonical_head,
            finalized_head: prev.finalized_head,
            promotion_height: prev.promotion_height,
            worker_state: WorkerState::Blocked,
            last_successful_poll_at: prev.last_successful_poll_at,
            last_error: Some(message.clone()),
        }));
        message
    }

    /// Loads the current snapshot. Lock-free.
    pub fn snapshot(&self) -> ChainStatusSnapshot {
        (**self.inner.load()).clone()
    }
}

/// The runtime registry of all chain worker statuses, keyed by chain ID.
///
/// Reads via [`RuntimeStatus::snapshot`] take a read lock on the underlying
/// `BTreeMap`; writes via [`RuntimeStatus::replace`] take a write lock. The
/// map is sorted by chain ID so [`RuntimeStatus::snapshot`] returns snapshots
/// in chain-ID order.
#[derive(Debug, Clone, Default)]
pub struct RuntimeStatus {
    inner: Arc<RwLock<BTreeMap<ChainId, ChainStatus>>>,
}

impl RuntimeStatus {
    /// Inserts or replaces the status for `status.snapshot().chain_id`.
    pub fn replace(&self, status: ChainStatus) {
        self.inner.write().insert(status.snapshot().chain_id, status);
    }

    /// Removes the status for `chain_id`, if present.
    pub fn remove(&self, chain_id: ChainId) {
        self.inner.write().remove(&chain_id);
    }

    /// Returns one snapshot per registered chain, sorted by chain ID.
    pub fn snapshot(&self) -> Vec<ChainStatusSnapshot> {
        self.inner.read().values().map(ChainStatus::snapshot).collect()
    }
}

/// Scrubs private endpoint URLs out of `error` before storing it as a public
/// status message.
///
/// `BlockSourceRequestError` is the marker type the RPC adapter wraps
/// transport failures in; any error whose chain includes one is replaced with
/// the generic `"block source request failed"` so that endpoint URLs never
/// leak through `/status`. Other errors are formatted with `anyhow`'s
/// `{error:#}` (which preserves the causal chain).
fn safe_error_message(error: &anyhow::Error) -> String {
    if error
        .chain()
        .any(|cause| cause.downcast_ref::<crate::ports::BlockSourceRequestError>().is_some())
    {
        "block source request failed".to_string()
    } else {
        format!("{error:#}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_are_sorted_and_isolated_by_chain() {
        let statuses = RuntimeStatus::default();
        statuses.replace(ChainStatus::disabled(10));
        statuses.replace(ChainStatus::running(1, 100));

        let snapshot = statuses.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].chain_id, 1);
        assert_eq!(snapshot[1].chain_id, 10);
        assert_eq!(snapshot[1].worker_state, WorkerState::Disabled);
    }

    #[test]
    fn records_worker_transitions() {
        let status = ChainStatus::starting(1, Some(9));
        status.record_success(10);
        assert_eq!(status.snapshot().worker_state, WorkerState::Running);

        status.record_error(&anyhow::anyhow!("boom"));
        let snapshot = status.snapshot();
        assert_eq!(snapshot.finalized_head, Some(10));
        assert_eq!(snapshot.worker_state, WorkerState::Degraded);
        assert_eq!(snapshot.last_error.as_deref(), Some("boom"));
    }

    #[test]
    fn hides_block_source_error_details() {
        let status = ChainStatus::starting(1, None);
        let error = anyhow::Error::new(crate::ports::BlockSourceRequestError::new(
            anyhow::anyhow!("secret endpoint failed"),
        ));

        assert_eq!(status.record_error(&error), "block source request failed");
        assert_eq!(status.snapshot().last_error.as_deref(), Some("block source request failed"));
    }

    #[test]
    fn records_unexpected_task_exit() {
        let status = ChainStatus::starting(1, None);
        status.record_task_exit();
        let snapshot = status.snapshot();
        assert_eq!(snapshot.worker_state, WorkerState::Degraded);
        assert_eq!(snapshot.last_error.as_deref(), Some("worker task exited unexpectedly"));
    }
}
