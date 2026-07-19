//! Optional post-commit sink port.
//!
//! `parseon-webhook-sink` implements [`Sink`] to deliver non-empty decoded
//! batches to a webhook URL after the worker has committed them to storage.
//! Sink failure is best-effort: it cannot rewind or fail committed indexing
//! work.

use std::sync::Arc;

use crate::monitor::Monitor;
use crate::{BlockNumber, Chain, ChainId, DecodedResult, Target};
use alloy::primitives::{Address, TxHash};

/// Post-commit delivery sink.
///
/// The worker calls [`Sink::submit`] only after a batch has been committed to
/// storage. [`Sink::submit`] must not block the indexing pipeline: it should
/// spawn or queue delivery internally. [`Sink::shutdown`] is called once during
/// process shutdown.
pub trait Sink: Send + Sync {
    /// Whether the sink is enabled. The worker skips encoding and submission
    /// entirely when this returns `false`. Defaults to `true`.
    fn enabled(&self) -> bool {
        true
    }

    /// Submits a non-empty batch of decoded results. Must not panic; failures
    /// should be logged and dropped.
    fn submit(&self, batch: SinkBatch);

    /// Flushes pending deliveries and releases resources. Called once during
    /// process shutdown after every worker has stopped. Defaults to no-op.
    fn shutdown(&self) {}
}

/// No-op sink used when no sink feature is enabled.
#[derive(Default)]
pub struct NoopSink;

impl Sink for NoopSink {
    fn enabled(&self) -> bool {
        false
    }

    fn submit(&self, _: SinkBatch) {}
}

/// One decoded-result batch delivered to a sink.
///
/// `version` is `1` for the current shape. `results` is non-empty by
/// construction (see [`SinkBatch::new`]).
#[derive(Debug, Clone, serde::Serialize)]
pub struct SinkBatch {
    /// Sink payload schema version.
    pub version: u8,
    /// EIP-155 chain ID the batch belongs to.
    pub chain_id: ChainId,
    /// Block the batch covers.
    pub block_number: BlockNumber,
    /// Decoded results in this batch.
    pub results: Vec<SinkResult>,
}

impl SinkBatch {
    /// Builds a sink batch for one committed block. Returns `Ok(None)` when
    /// `results` is empty so the worker can skip submission without an extra
    /// branch.
    ///
    /// Returns an error if a result references a monitor id not present in
    /// `monitors`, or if a call result references an event monitor (or vice
    /// versa).
    pub fn new(
        chain: Chain,
        block_number: BlockNumber,
        monitors: &[Arc<Monitor>],
        results: &[DecodedResult],
    ) -> anyhow::Result<Option<Self>> {
        if results.is_empty() {
            return Ok(None);
        }
        let results = results
            .iter()
            .map(|result| match result {
                DecodedResult::Call(call) => {
                    let monitor = monitors
                        .iter()
                        .find(|monitor| monitor.id == call.monitor_id)
                        .ok_or_else(|| {
                            anyhow::anyhow!("result references unknown monitor {}", call.monitor_id)
                        })?;
                    let Target::Call(target) = &monitor.target else {
                        anyhow::bail!("call result references event monitor")
                    };
                    Ok(SinkResult::Call {
                        monitor_id: call.monitor_id.get(),
                        tx_hash: call.transaction_hash,
                        from: call.from,
                        to: call.to,
                        params: super::storage::canonical_params(&target.inputs, &call.params)?,
                    })
                }
                DecodedResult::Event(event) => {
                    let monitor = monitors
                        .iter()
                        .find(|monitor| monitor.id == event.monitor_id)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "result references unknown monitor {}",
                                event.monitor_id
                            )
                        })?;
                    let Target::Event(target) = &monitor.target else {
                        anyhow::bail!("event result references call monitor")
                    };
                    Ok(SinkResult::Event {
                        monitor_id: event.monitor_id.get(),
                        tx_hash: event.transaction_hash,
                        emitter: target.address,
                        log_index: event.log_index,
                        params: super::storage::canonical_params(&target.params, &event.params)?,
                    })
                }
            })
            .collect::<anyhow::Result<_>>()?;
        Ok(Some(Self { version: 1, chain_id: chain.id, block_number, results }))
    }
}

/// One decoded result inside a [`SinkBatch`].
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SinkResult {
    /// A decoded call.
    Call {
        /// Monitor that produced this result.
        monitor_id: u64,
        /// Transaction hash.
        tx_hash: TxHash,
        /// Sender address.
        from: Address,
        /// Recipient address.
        to: Address,
        /// Canonical JSON-encoded parameters.
        params: serde_json::Value,
    },
    /// A decoded event.
    Event {
        /// Monitor that produced this result.
        monitor_id: u64,
        /// Transaction hash that emitted the log.
        tx_hash: TxHash,
        /// Emitter address.
        emitter: Address,
        /// Log index within the block.
        log_index: u64,
        /// Canonical JSON-encoded parameters.
        params: serde_json::Value,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suppresses_empty_sink_batches() {
        assert!(SinkBatch::new(Chain::new(1), 10, &[], &[]).unwrap().is_none());
    }
}
