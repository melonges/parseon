use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::cache::BlockCache;
use crate::core::Chain;
use crate::indexer::pipeline;
use crate::rpc::BlockSource;
use crate::scheduler;
use crate::storage::{BlockCommit, Storage};

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
    cancel: CancellationToken,
) {
    tracing::info!(chain_id = config.chain.id, "worker started");
    loop {
        if cancel.is_cancelled() {
            break;
        }
        if let Err(error) = run_once(
            &config,
            storage.as_ref(),
            source.as_ref(),
            cache.as_ref(),
            &cancel,
        )
        .await
        {
            tracing::warn!(chain_id = config.chain.id, "worker tick: {error:#}");
        }
        tokio::select! {
            _ = tokio::time::sleep(config.poll_interval) => {}
            _ = cancel.cancelled() => break,
        }
    }
    tracing::info!(chain_id = config.chain.id, "worker stopped");
}

pub async fn run_once(
    config: &WorkerConfig,
    storage: &dyn Storage,
    source: &dyn BlockSource,
    cache: &dyn BlockCache,
    cancel: &CancellationToken,
) -> anyhow::Result<usize> {
    let monitors = storage.load_monitors().await?;
    let active = monitors
        .iter()
        .filter(|monitor| monitor.enabled && !monitor.completed)
        .collect::<Vec<_>>();
    if active.is_empty() {
        return Ok(0);
    }

    let finalized_head = source.finalized_head().await?;
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
                covering
                    .iter()
                    .any(|monitor| monitor.matches(transaction.to, selector))
            })
            .cloned()
            .collect::<Vec<_>>();
        let executed = source.fetch_receipts(&candidates).await?;
        let calls = pipeline::decode_calls(&block, &covering, executed);
        decoded += storage
            .commit_block(BlockCommit {
                block_number,
                monitors: covering,
                calls,
            })
            .await?;
    }

    let min_cursor = active
        .iter()
        .map(|monitor| monitor.cursor.0.unwrap_or(monitor.start_block - 1))
        .min()
        .unwrap_or(0);
    cache.evict_before(config.chain, min_cursor);
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use alloy::primitives::{Address, B256};
    use async_trait::async_trait;

    use super::*;
    use crate::cache::MemoryBlockCache;
    use crate::core::{BlockTransaction, Cursor, ExecutedTransaction, SourceBlock, Target};
    use crate::filter::Filter;
    use crate::monitor::Monitor;

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
            let count = commit.calls.len();
            self.commits.lock().unwrap().push(commit);
            Ok(count)
        }
    }

    struct FakeSource;

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

    #[tokio::test]
    async fn commits_cursor_progress_when_block_has_no_matches() {
        let storage = FakeStorage {
            monitor: Monitor {
                id: 7,
                target: Target {
                    address: Address::ZERO,
                    selector: [1, 2, 3, 4],
                    signature: "f(uint256)".into(),
                    inputs: Vec::new(),
                },
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
        let decoded = run_once(
            &config,
            &storage,
            &FakeSource,
            &MemoryBlockCache::new(4),
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(decoded, 0);
        let commits = storage.commits.lock().unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].block_number, 10);
        assert_eq!(commits[0].monitors[0].id, 7);
    }
}
