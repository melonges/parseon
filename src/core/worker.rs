use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::indexer;
use super::ports::{BlockCache, BlockCommit, BlockSource, Storage};
use super::status::RuntimeStatus;
use super::{Chain, DecodedResult, Target, scheduler};

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub chain: Chain,
    pub batch_size: i64,
    pub poll_interval: Duration,
}

pub async fn run(
    config: WorkerConfig,
    storage: Arc<dyn Storage>,
    source: Arc<dyn BlockSource>,
    cache: Arc<dyn BlockCache>,
    status: RuntimeStatus,
    cancel: CancellationToken,
) {
    tracing::info!(chain_id = config.chain.id, "worker started");
    loop {
        if cancel.is_cancelled() {
            break;
        }
        match run_once(
            &config,
            storage.as_ref(),
            source.as_ref(),
            cache.as_ref(),
            &cancel,
        )
        .await
        {
            Ok(poll) => status.record_success(poll.finalized_head),
            Err(error) => {
                status.record_error(&error);
                tracing::warn!(chain_id = config.chain.id, "worker tick: {error:#}");
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(config.poll_interval) => {}
            _ = cancel.cancelled() => break,
        }
    }
    tracing::info!(chain_id = config.chain.id, "worker stopped");
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollResult {
    pub finalized_head: i64,
    pub decoded: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceStatus {
    pub chain_id: u64,
    pub finalized_head: i64,
}

pub async fn probe_source(source: &dyn BlockSource) -> anyhow::Result<SourceStatus> {
    let chain_id = source.chain_id().await?;
    let finalized_head = source.finalized_head().await.map_err(|error| {
        anyhow::anyhow!("RPC does not support the required finalized block tag: {error:#}")
    })?;
    Ok(SourceStatus {
        chain_id,
        finalized_head,
    })
}

pub async fn run_once(
    config: &WorkerConfig,
    storage: &dyn Storage,
    source: &dyn BlockSource,
    cache: &dyn BlockCache,
    cancel: &CancellationToken,
) -> anyhow::Result<PollResult> {
    let finalized_head = source.finalized_head().await?;
    let monitors = storage.load_monitors().await?;
    let active = monitors
        .iter()
        .filter(|monitor| monitor.enabled && !monitor.completed)
        .collect::<Vec<_>>();
    if active.is_empty() {
        return Ok(PollResult {
            finalized_head,
            decoded: 0,
        });
    }

    let wanted = scheduler::plan_blocks(&active, finalized_head, config.batch_size);
    let mut decoded = 0;

    for block_number in wanted {
        if cancel.is_cancelled() {
            break;
        }
        let covering = active
            .iter()
            .filter(|monitor| {
                monitor.covers(block_number)
                    && monitor.cursor.0.is_none_or(|cursor| cursor < block_number)
            })
            .map(|monitor| (*monitor).clone())
            .collect::<Vec<_>>();
        if covering.is_empty() {
            continue;
        }

        let call_monitors = covering
            .iter()
            .filter(|m| matches!(&m.target, Target::Call(_)))
            .cloned()
            .collect::<Vec<_>>();
        let event_monitors = covering
            .iter()
            .filter(|m| matches!(&m.target, Target::Event(_)))
            .cloned()
            .collect::<Vec<_>>();
        let mut results = Vec::new();
        if !call_monitors.is_empty() {
            let block = match cache.get(config.chain, block_number) {
                Some(block) => block,
                None => {
                    let block = source.fetch_block(block_number).await?;
                    cache.put(config.chain, block.clone());
                    block
                }
            };
            let candidates = block
                .transactions
                .iter()
                .filter(|transaction| {
                    let selector = transaction.input.get(..4).unwrap_or_default();
                    call_monitors
                        .iter()
                        .any(|monitor| monitor.matches_call(transaction.to, selector))
                })
                .cloned()
                .collect::<Vec<_>>();
            let executed = source.fetch_receipts(&candidates).await?;
            results.extend(
                indexer::decode_calls(&block, &call_monitors, executed)
                    .into_iter()
                    .map(DecodedResult::Call),
            );
        }
        if !event_monitors.is_empty() {
            let mut addresses = Vec::new();
            let mut topic0s = Vec::new();
            for monitor in &event_monitors {
                if let Target::Event(target) = &monitor.target {
                    addresses.push(target.address);
                    topic0s.push(target.topic0);
                }
            }
            addresses.sort_unstable();
            addresses.dedup();
            topic0s.sort_unstable();
            topic0s.dedup();
            let logs = source
                .fetch_logs(block_number, &addresses, &topic0s)
                .await?;
            results.extend(
                indexer::decode_events(block_number, &event_monitors, logs)?
                    .into_iter()
                    .map(DecodedResult::Event),
            );
        }
        decoded += storage
            .commit_block(BlockCommit {
                block_number,
                monitors: covering,
                results,
            })
            .await?;
    }

    let min_cursor = active
        .iter()
        .map(|monitor| monitor.cursor.0.unwrap_or(monitor.start_block - 1))
        .min()
        .unwrap_or(0);
    cache.evict_before(config.chain, min_cursor);
    Ok(PollResult {
        finalized_head,
        decoded,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use alloy::primitives::{Address, B256};
    use async_trait::async_trait;

    use super::*;
    use crate::core::filter::Filter;
    use crate::core::monitor::Monitor;
    use crate::core::ports::{BlockCache, BlockCommit, BlockSource, Storage};
    use crate::core::{
        BlockTransaction, CallTarget, Cursor, ExecutedTransaction, SourceBlock, Target,
    };

    struct FakeStorage {
        monitor: Monitor,
        commits: Mutex<Vec<BlockCommit>>,
    }

    #[async_trait]
    impl Storage for FakeStorage {
        async fn load_monitors(&self) -> anyhow::Result<Vec<Monitor>> {
            Ok(vec![self.monitor.clone()])
        }

        async fn commit_block(&self, commit: BlockCommit) -> anyhow::Result<usize> {
            let count = commit.results.len();
            self.commits.lock().unwrap().push(commit);
            Ok(count)
        }
    }

    struct FakeSource;

    struct UnsupportedFinalizedSource;

    #[derive(Default)]
    struct FakeCache {
        blocks: Mutex<HashMap<(i64, i64), SourceBlock>>,
    }

    impl BlockCache for FakeCache {
        fn get(&self, chain: Chain, block_number: i64) -> Option<SourceBlock> {
            self.blocks
                .lock()
                .unwrap()
                .get(&(chain.id, block_number))
                .cloned()
        }

        fn put(&self, chain: Chain, block: SourceBlock) {
            self.blocks
                .lock()
                .unwrap()
                .insert((chain.id, block.number), block);
        }

        fn evict_before(&self, chain: Chain, block_number: i64) {
            self.blocks
                .lock()
                .unwrap()
                .retain(|(chain_id, number), _| *chain_id != chain.id || *number >= block_number);
        }
    }

    #[async_trait]
    impl BlockSource for FakeSource {
        async fn chain_id(&self) -> anyhow::Result<u64> {
            Ok(1)
        }

        async fn finalized_head(&self) -> anyhow::Result<i64> {
            Ok(10)
        }

        async fn fetch_block(&self, block_number: i64) -> anyhow::Result<SourceBlock> {
            Ok(SourceBlock {
                number: block_number,
                hash: B256::ZERO,
                transactions: Vec::new(),
            })
        }

        async fn fetch_receipts(
            &self,
            _transactions: &[BlockTransaction],
        ) -> anyhow::Result<Vec<ExecutedTransaction>> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl BlockSource for UnsupportedFinalizedSource {
        async fn chain_id(&self) -> anyhow::Result<u64> {
            Ok(1)
        }

        async fn finalized_head(&self) -> anyhow::Result<i64> {
            anyhow::bail!("invalid argument: finalized")
        }

        async fn fetch_block(&self, _block_number: i64) -> anyhow::Result<SourceBlock> {
            unreachable!()
        }

        async fn fetch_receipts(
            &self,
            _transactions: &[BlockTransaction],
        ) -> anyhow::Result<Vec<ExecutedTransaction>> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn startup_probe_requires_finalized_tag_support() {
        assert_eq!(
            probe_source(&FakeSource).await.unwrap(),
            SourceStatus {
                chain_id: 1,
                finalized_head: 10,
            }
        );

        let error = probe_source(&UnsupportedFinalizedSource).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not support the required finalized block tag")
        );
    }

    #[tokio::test]
    async fn commits_cursor_progress_when_block_has_no_matches() {
        let storage = FakeStorage {
            monitor: Monitor {
                id: 7,
                target: Target::Call(CallTarget {
                    address: Address::ZERO,
                    selector: [1, 2, 3, 4],
                    signature: "f(uint256)".into(),
                    inputs: Vec::new(),
                }),
                start_block: 10,
                end_block: None,
                cursor: Cursor(None),
                completed: false,
                enabled: true,
                filter: Filter::All,
            },
            commits: Mutex::new(Vec::new()),
        };
        let config = WorkerConfig {
            chain: Chain::new(1).unwrap(),
            batch_size: 1,
            poll_interval: Duration::from_millis(100),
        };
        let poll = run_once(
            &config,
            &storage,
            &FakeSource,
            &FakeCache::default(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(poll.finalized_head, 10);
        assert_eq!(poll.decoded, 0);
        let commits = storage.commits.lock().unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].block_number, 10);
        assert_eq!(commits[0].monitors[0].id, 7);
    }

    #[tokio::test]
    async fn status_keeps_last_head_and_degrades_after_rpc_failure() {
        let status = RuntimeStatus::new(1, 9);
        let storage = FakeStorage {
            monitor: Monitor {
                id: 7,
                target: Target::Call(CallTarget {
                    address: Address::ZERO,
                    selector: [1, 2, 3, 4],
                    signature: "f(uint256)".into(),
                    inputs: Vec::new(),
                }),
                start_block: 11,
                end_block: None,
                cursor: Cursor(None),
                completed: false,
                enabled: true,
                filter: Filter::All,
            },
            commits: Mutex::new(Vec::new()),
        };
        let config = WorkerConfig {
            chain: Chain::new(1).unwrap(),
            batch_size: 1,
            poll_interval: Duration::from_millis(100),
        };

        let poll = run_once(
            &config,
            &storage,
            &FakeSource,
            &FakeCache::default(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        status.record_success(poll.finalized_head);

        let error = run_once(
            &config,
            &storage,
            &UnsupportedFinalizedSource,
            &FakeCache::default(),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        status.record_error(&error);

        let snapshot = status.snapshot();
        assert_eq!(snapshot.finalized_head, 10);
        assert_eq!(
            snapshot.worker_state,
            super::super::status::WorkerState::Degraded
        );
        assert!(snapshot.last_error.unwrap().contains("invalid argument"));
    }
}
