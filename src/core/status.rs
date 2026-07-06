use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    Starting,
    Running,
    Degraded,
    Disabled,
}

#[derive(Debug, Clone)]
pub struct ChainStatusSnapshot {
    pub chain_id: i64,
    pub enabled: bool,
    pub finalized_head: Option<i64>,
    pub worker_state: WorkerState,
    pub last_successful_poll_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ChainStatus {
    inner: Arc<RwLock<ChainStatusSnapshot>>,
}

impl ChainStatus {
    pub fn starting(chain_id: i64, finalized_head: Option<i64>) -> Self {
        Self::new(ChainStatusSnapshot {
            chain_id,
            enabled: true,
            finalized_head,
            worker_state: WorkerState::Starting,
            last_successful_poll_at: None,
            last_error: None,
        })
    }

    #[cfg(test)]
    pub fn running(chain_id: i64, finalized_head: i64) -> Self {
        Self::new(ChainStatusSnapshot {
            chain_id,
            enabled: true,
            finalized_head: Some(finalized_head),
            worker_state: WorkerState::Running,
            last_successful_poll_at: Some(Utc::now()),
            last_error: None,
        })
    }

    pub fn disabled(chain_id: i64) -> Self {
        Self::new(ChainStatusSnapshot {
            chain_id,
            enabled: false,
            finalized_head: None,
            worker_state: WorkerState::Disabled,
            last_successful_poll_at: None,
            last_error: None,
        })
    }

    pub fn degraded(chain_id: i64, message: impl Into<String>) -> Self {
        Self::new(ChainStatusSnapshot {
            chain_id,
            enabled: true,
            finalized_head: None,
            worker_state: WorkerState::Degraded,
            last_successful_poll_at: None,
            last_error: Some(message.into()),
        })
    }

    fn new(snapshot: ChainStatusSnapshot) -> Self {
        Self {
            inner: Arc::new(RwLock::new(snapshot)),
        }
    }

    pub fn record_success(&self, finalized_head: i64) {
        let mut status = self.inner.write().expect("runtime status lock poisoned");
        status.finalized_head = Some(finalized_head);
        status.worker_state = WorkerState::Running;
        status.last_successful_poll_at = Some(Utc::now());
        status.last_error = None;
    }

    pub fn record_error(&self, error: &anyhow::Error) -> String {
        let message = safe_error_message(error);
        let mut status = self.inner.write().expect("runtime status lock poisoned");
        status.worker_state = WorkerState::Degraded;
        status.last_error = Some(message.clone());
        message
    }

    pub fn snapshot(&self) -> ChainStatusSnapshot {
        self.inner
            .read()
            .expect("runtime status lock poisoned")
            .clone()
    }
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeStatus {
    inner: Arc<RwLock<BTreeMap<i64, ChainStatus>>>,
}

impl RuntimeStatus {
    pub fn replace(&self, status: ChainStatus) {
        self.inner
            .write()
            .expect("runtime status registry lock poisoned")
            .insert(status.snapshot().chain_id, status);
    }

    pub fn remove(&self, chain_id: i64) {
        self.inner
            .write()
            .expect("runtime status registry lock poisoned")
            .remove(&chain_id);
    }

    pub fn snapshot(&self) -> Vec<ChainStatusSnapshot> {
        self.inner
            .read()
            .expect("runtime status registry lock poisoned")
            .values()
            .map(ChainStatus::snapshot)
            .collect()
    }
}

fn safe_error_message(error: &anyhow::Error) -> String {
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<crate::error::AppError>()
            .is_some_and(|error| matches!(error, crate::error::AppError::Rpc(_)))
            || cause
                .downcast_ref::<alloy::transports::TransportError>()
                .is_some()
    }) {
        "RPC request failed".to_string()
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
}
