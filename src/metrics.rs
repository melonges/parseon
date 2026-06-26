use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

pub fn init() -> PrometheusHandle {
    PrometheusBuilder::new()
        .install_recorder()
        .expect("install prometheus recorder")
}

pub fn describe() {
    describe_counter!(
        "evm_indexer_blocks_fetched_total",
        "Blocks fetched from RPC"
    );
    describe_counter!(
        "evm_indexer_txs_decoded_total",
        "Transactions decoded and persisted"
    );
    describe_counter!("evm_indexer_decode_errors_total", "Decode/persist errors");
    describe_gauge!("evm_indexer_head_lag", "Chain head - max monitor cursor");
    describe_gauge!("evm_indexer_monitor_cursor", "Current cursor of a monitor");
    describe_gauge!(
        "evm_indexer_monitor_completed",
        "1 if monitor completed its range"
    );
    describe_gauge!(
        "evm_indexer_monitors_active",
        "Active (enabled, !completed) monitors per chain"
    );
    describe_histogram!(
        "evm_indexer_block_fetch_seconds",
        "Time spent fetching a block"
    );
    describe_histogram!(
        "evm_indexer_db_insert_seconds",
        "Time spent inserting tx rows"
    );
}

pub fn blocks_fetched(chain: &str) {
    counter!("evm_indexer_blocks_fetched_total", "chain" => chain.to_string()).increment(1);
}

pub fn txs_decoded(chain: &str, monitor: i64) {
    counter!("evm_indexer_txs_decoded_total", "chain" => chain.to_string(), "monitor" => monitor.to_string()).increment(1);
}

pub fn decode_error(chain: &str, reason: &str) {
    counter!("evm_indexer_decode_errors_total", "chain" => chain.to_string(), "reason" => reason.to_string()).increment(1);
}

pub fn head_lag(chain: &str, lag: i64) {
    gauge!("evm_indexer_head_lag", "chain" => chain.to_string()).set(lag as f64);
}

pub fn monitor_cursor(chain: &str, monitor: i64, cursor: i64) {
    gauge!("evm_indexer_monitor_cursor", "chain" => chain.to_string(), "monitor" => monitor.to_string()).set(cursor as f64);
}

pub fn monitor_completed(chain: &str, monitor: i64, completed: bool) {
    gauge!("evm_indexer_monitor_completed", "chain" => chain.to_string(), "monitor" => monitor.to_string()).set(if completed { 1.0 } else { 0.0 });
}

pub fn monitors_active(chain: &str, n: i64) {
    gauge!("evm_indexer_monitors_active", "chain" => chain.to_string()).set(n as f64);
}

pub fn block_fetch_seconds(chain: &str) -> metrics::Histogram {
    histogram!("evm_indexer_block_fetch_seconds", "chain" => chain.to_string())
}

pub fn db_insert_seconds(chain: &str) -> metrics::Histogram {
    histogram!("evm_indexer_db_insert_seconds", "chain" => chain.to_string())
}
