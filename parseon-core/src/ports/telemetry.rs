//! Telemetry port: in-process observability for the worker and adapters.
//!
//! `parseon-server` implements [`Telemetry`] with a Prometheus exposition
//! collector served at `/metrics`. The worker records RPC, cache, and commit
//! timings through this port; the RPC adapter records per-request timings and
//! in-flight counts via [`InFlightGuard`](crate::ports::InFlightGuard).

use std::time::Duration;

use crate::{BlockNumber, ChainId};

/// Observability sink used by the worker and the RPC adapter.
///
/// Every method is infallible from the caller's perspective: telemetry must
/// never block the indexing pipeline. Implementations should buffer or drop
/// silently under pressure rather than panicking.
pub trait Telemetry: Send + Sync {
    /// Records one RPC operation of `operation`/`strategy`/`outcome` on
    /// `chain_id`, with its `elapsed` duration.
    fn record_rpc(
        &self,
        chain_id: ChainId,
        operation: &'static str,
        strategy: &'static str,
        outcome: &'static str,
        elapsed: Duration,
    );
    /// Records a cache hit (`hit = true`) or miss (`hit = false`) on `chain_id`.
    fn record_cache(&self, chain_id: ChainId, hit: bool);
    /// Records one atomic block commit on `chain_id` with `calls` and `events`
    /// counts, the commit `outcome` (`"success"` or `"error"`), and `elapsed`.
    fn record_commit(
        &self,
        chain_id: ChainId,
        calls: u64,
        events: u64,
        outcome: &'static str,
        elapsed: Duration,
    );
    /// Sets the current lag (finalized head minus committed head) for `chain_id`.
    fn set_worker_lag(&self, chain_id: ChainId, lag: BlockNumber);
    /// Marks the current lifecycle state of `chain_id` for dashboards and alerts.
    fn set_worker_state(&self, chain_id: ChainId, state: &'static str);
    /// Records the Unix timestamp of the last successful poll for `chain_id`.
    fn set_worker_last_successful_poll(&self, chain_id: ChainId, timestamp: i64);
    /// Adjusts the in-flight count for `stage` on `chain_id` by `delta`
    /// (positive on entry, negative on exit).
    fn adjust_in_flight(&self, chain_id: ChainId, stage: &'static str, delta: i64);
    /// Renders the collected metrics in the implementation's exposition format
    /// (e.g. Prometheus text format).
    fn render(&self) -> anyhow::Result<String>;
}

/// No-op telemetry used in tests and when metrics are disabled.
#[derive(Default)]
pub struct NoopTelemetry;

impl Telemetry for NoopTelemetry {
    fn record_rpc(
        &self,
        _: ChainId,
        _: &'static str,
        _: &'static str,
        _: &'static str,
        _: Duration,
    ) {
    }
    fn record_cache(&self, _: ChainId, _: bool) {}
    fn record_commit(&self, _: ChainId, _: u64, _: u64, _: &'static str, _: Duration) {}
    fn set_worker_lag(&self, _: ChainId, _: BlockNumber) {}
    fn set_worker_state(&self, _: ChainId, _: &'static str) {}
    fn set_worker_last_successful_poll(&self, _: ChainId, _: i64) {}
    fn adjust_in_flight(&self, _: ChainId, _: &'static str, _: i64) {}
    fn render(&self) -> anyhow::Result<String> {
        Ok(String::new())
    }
}
