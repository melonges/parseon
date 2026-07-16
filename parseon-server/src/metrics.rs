use std::sync::Arc;
use std::time::Duration;

use prometheus_client::encoding::{EncodeLabelSet, text::encode};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;

use parseon_core::ports::Telemetry;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ChainLabels {
    chain_id: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ResultLabels {
    chain_id: String,
    kind: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct OutcomeLabels {
    chain_id: String,
    outcome: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StageLabels {
    chain_id: String,
    stage: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct CacheLabels {
    chain_id: String,
    result: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RpcLabels {
    chain_id: String,
    operation: &'static str,
    outcome: &'static str,
    strategy: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RpcDurationLabels {
    chain_id: String,
    operation: &'static str,
    strategy: &'static str,
}

struct Inner {
    registry: Registry,
    rpc_operations: Family<RpcLabels, Counter>,
    rpc_duration: Family<RpcDurationLabels, Histogram>,
    blocks_committed: Family<ChainLabels, Counter>,
    results_committed: Family<ResultLabels, Counter>,
    storage_commit_duration: Family<OutcomeLabels, Histogram>,
    worker_lag: Family<ChainLabels, Gauge>,
    in_flight: Family<StageLabels, Gauge>,
    cache_access: Family<CacheLabels, Counter>,
}

fn histogram() -> Histogram {
    Histogram::new(exponential_buckets(0.001, 2.0, 16))
}

#[derive(Clone)]
pub(crate) struct Metrics {
    inner: Arc<Inner>,
}

impl Default for Metrics {
    fn default() -> Self {
        let rpc_operations = Family::default();
        let rpc_duration = Family::new_with_constructor(histogram as fn() -> Histogram);
        let blocks_committed = Family::default();
        let results_committed = Family::default();
        let storage_commit_duration = Family::new_with_constructor(histogram as fn() -> Histogram);
        let worker_lag = Family::default();
        let in_flight = Family::default();
        let cache_access = Family::default();

        let mut registry = Registry::default();
        registry.register(
            "parseon_rpc_operations",
            "Logical block source operations.",
            rpc_operations.clone(),
        );
        registry.register(
            "parseon_rpc_operation_duration_seconds",
            "Block source operation duration in seconds.",
            rpc_duration.clone(),
        );
        registry.register(
            "parseon_blocks_committed",
            "Blocks committed atomically with monitor cursor progress.",
            blocks_committed.clone(),
        );
        registry.register(
            "parseon_results_committed",
            "Decoded results committed to storage.",
            results_committed.clone(),
        );
        registry.register(
            "parseon_storage_commit_duration_seconds",
            "Atomic block commit duration in seconds.",
            storage_commit_duration.clone(),
        );
        registry.register(
            "parseon_worker_lag_blocks",
            "Finalized blocks between the slowest active monitor and the finalized head.",
            worker_lag.clone(),
        );
        registry.register(
            "parseon_in_flight",
            "Work currently in flight by bounded pipeline stage.",
            in_flight.clone(),
        );
        registry.register(
            "parseon_block_cache_access",
            "Block cache accesses by result.",
            cache_access.clone(),
        );

        Self {
            inner: Arc::new(Inner {
                registry,
                rpc_operations,
                rpc_duration,
                blocks_committed,
                results_committed,
                storage_commit_duration,
                worker_lag,
                in_flight,
                cache_access,
            }),
        }
    }
}

impl Metrics {
    fn chain_labels(chain_id: u64) -> ChainLabels {
        ChainLabels { chain_id: chain_id.to_string() }
    }
}

impl Telemetry for Metrics {
    fn record_rpc(
        &self,
        chain_id: u64,
        operation: &'static str,
        strategy: &'static str,
        outcome: &'static str,
        elapsed: Duration,
    ) {
        self.inner
            .rpc_operations
            .get_or_create(&RpcLabels {
                chain_id: chain_id.to_string(),
                operation,
                outcome,
                strategy,
            })
            .inc();
        self.inner
            .rpc_duration
            .get_or_create(&RpcDurationLabels {
                chain_id: chain_id.to_string(),
                operation,
                strategy,
            })
            .observe(elapsed.as_secs_f64());
    }

    fn record_cache(&self, chain_id: u64, hit: bool) {
        self.inner
            .cache_access
            .get_or_create(&CacheLabels {
                chain_id: chain_id.to_string(),
                result: if hit { "hit" } else { "miss" },
            })
            .inc();
    }

    fn record_commit(
        &self,
        chain_id: u64,
        calls: u64,
        events: u64,
        outcome: &'static str,
        elapsed: Duration,
    ) {
        self.inner
            .storage_commit_duration
            .get_or_create(&OutcomeLabels { chain_id: chain_id.to_string(), outcome })
            .observe(elapsed.as_secs_f64());
        if outcome != "success" {
            return;
        }
        self.inner.blocks_committed.get_or_create(&Self::chain_labels(chain_id)).inc();
        for (kind, count) in [("call", calls), ("event", events)] {
            if count > 0 {
                self.inner
                    .results_committed
                    .get_or_create(&ResultLabels { chain_id: chain_id.to_string(), kind })
                    .inc_by(count);
            }
        }
    }

    fn set_worker_lag(&self, chain_id: u64, lag: u64) {
        self.inner
            .worker_lag
            .get_or_create(&Self::chain_labels(chain_id))
            .set(i64::try_from(lag).unwrap_or(i64::MAX));
    }

    fn adjust_in_flight(&self, chain_id: u64, stage: &'static str, delta: i64) {
        let gauge = self
            .inner
            .in_flight
            .get_or_create(&StageLabels { chain_id: chain_id.to_string(), stage });
        if delta > 0 {
            gauge.inc_by(delta);
        } else {
            gauge.dec_by(-delta);
        }
    }

    fn render(&self) -> anyhow::Result<String> {
        let mut output = String::new();
        encode(&mut output, &self.inner.registry)?;
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_bounded_labels_without_rpc_urls() {
        let metrics = Metrics::default();
        metrics.record_rpc(8453, "receipts", "batch", "success", Duration::from_millis(2));
        metrics.record_cache(8453, true);
        metrics.record_commit(8453, 2, 1, "success", Duration::from_millis(1));
        metrics.set_worker_lag(8453, 7);
        metrics.adjust_in_flight(8453, "storage", 1);

        let output = metrics.render().unwrap();
        assert!(output.contains("parseon_rpc_operations_total"));
        assert!(output.contains("chain_id=\"8453\""));
        assert!(output.contains("strategy=\"batch\""));
        assert!(output.contains("parseon_storage_commit_duration_seconds"));
        assert!(output.contains("stage=\"storage\""));
        assert!(!output.contains("rpc_url"));
    }
}
