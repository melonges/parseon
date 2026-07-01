use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    Running,
    Degraded,
}

#[derive(Debug, Clone)]
pub struct StatusSnapshot {
    pub chain_id: i64,
    pub finalized_head: i64,
    pub worker_state: WorkerState,
    pub last_successful_poll_at: DateTime<Utc>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RuntimeStatus {
    inner: Arc<RwLock<StatusSnapshot>>,
}

impl RuntimeStatus {
    pub fn new(chain_id: i64, finalized_head: i64) -> Self {
        Self {
            inner: Arc::new(RwLock::new(StatusSnapshot {
                chain_id,
                finalized_head,
                worker_state: WorkerState::Running,
                last_successful_poll_at: Utc::now(),
                last_error: None,
            })),
        }
    }

    pub fn record_success(&self, finalized_head: i64) {
        let mut status = self.inner.write().expect("runtime status lock poisoned");
        status.finalized_head = finalized_head;
        status.worker_state = WorkerState::Running;
        status.last_successful_poll_at = Utc::now();
        status.last_error = None;
    }

    pub fn record_error(&self, error: &anyhow::Error) {
        let mut status = self.inner.write().expect("runtime status lock poisoned");
        status.worker_state = WorkerState::Degraded;
        status.last_error = Some(format!("{error:#}"));
    }

    pub fn snapshot(&self) -> StatusSnapshot {
        self.inner
            .read()
            .expect("runtime status lock poisoned")
            .clone()
    }
}
