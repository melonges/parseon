use std::time::Duration;

use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::error::AppResult;
use crate::indexer::decode_persist;
use crate::metrics;
use crate::rpc::{block_cache::BlockCache, fetch::fetch_block, provider};
use crate::watcher::registry::Registry;

/// Configuration for the single chain being indexed.
#[derive(Debug, Clone)]
pub struct ChainConfig {
    pub chain_id: i64,
    pub rpc_url: String,
    pub batch_size: i32,
}

/// Run the per-chain coordinator loop until the cancellation token fires.
///
/// Each tick:
///   1. Snapshot active monitors for this chain.
///   2. If none, sleep and continue.
///   3. Fetch chain head.
///   4. Compute the union of blocks needed across monitors (cursor+1 .. min(end||head, head)), capped at batch_size.
///   5. Fetch each unique block once (shared cache), fan out to covering monitors, decode + persist.
///   6. Advance each monitor's cursor; mark completed if end reached.
///   7. Evict cache entries below the minimum cursor.
pub async fn run(
    chain: ChainConfig,
    pool: PgPool,
    registry: Registry,
    cache_cap: usize,
    poll_interval_ms: u64,
    cancel: CancellationToken,
) -> AppResult<()> {
    let chain_label = chain.chain_id.to_string();
    let provider = provider::build(&chain.rpc_url)?;
    let cache = BlockCache::new(cache_cap);
    let poll = Duration::from_millis(poll_interval_ms.max(100));
    let batch = chain.batch_size.max(1) as i64;

    tracing::info!(chain = %chain_label, chain_id = chain.chain_id, "coordinator started");

    loop {
        if cancel.is_cancelled() {
            tracing::info!(chain = %chain_label, "coordinator shutting down");
            break;
        }

        let monitors = registry.get_all().await;
        let active: Vec<_> = monitors
            .iter()
            .filter(|m| m.enabled && !m.completed)
            .collect();
        let active_n = active.len() as i64;
        metrics::monitors_active(&chain_label, active_n);

        if active.is_empty() {
            tokio::select! {
                _ = tokio::time::sleep(poll) => {}
                _ = cancel.cancelled() => break,
            }
            continue;
        }

        let head = match provider::head_number(&provider).await {
            Ok(h) => h as i64,
            Err(e) => {
                tracing::warn!(chain = %chain_label, "head fetch: {e}");
                tokio::select! {
                    _ = tokio::time::sleep(poll) => {}
                    _ = cancel.cancelled() => break,
                }
                continue;
            }
        };
        metrics::head_lag(
            &chain_label,
            head - active
                .iter()
                .map(|m| m.cursor.unwrap_or(m.start_block - 1))
                .max()
                .unwrap_or(head),
        );

        // Compute the union of blocks to process.
        let mut wanted: Vec<i64> = Vec::new();
        for m in &active {
            let from = m.next_block();
            let to = m.end_block.unwrap_or(head).min(head);
            if from > to {
                continue;
            }
            let to = to.min(from + batch - 1);
            for n in from..=to {
                if !wanted.contains(&n) {
                    wanted.push(n);
                }
            }
        }
        wanted.sort_unstable();

        for block_number in wanted {
            if cancel.is_cancelled() {
                break;
            }

            // Fetch (or pull from cache) the block.
            let cached = cache.get(block_number);
            let (block_hash, txs) = if let Some(c) = cached {
                (c.block_hash, c.txs)
            } else {
                match fetch_block(&provider, block_number as u64, &chain_label).await {
                    Ok(res) => {
                        let (h, t) = res;
                        let h_clone = h;
                        let t_clone = t.clone();
                        cache.put(
                            block_number,
                            crate::rpc::block_cache::CachedBlock {
                                block_hash: h_clone,
                                txs: t_clone,
                            },
                        );
                        (h, t)
                    }
                    Err(e) => {
                        tracing::warn!(chain = %chain_label, block = block_number, "fetch block: {e}");
                        metrics::decode_error(&chain_label, "fetch_block");
                        continue;
                    }
                }
            };

            let block_hash_hex = format!("{block_hash}");

            // Fan out to monitors that cover this block and haven't passed it.
            let covering: Vec<_> = active
                .iter()
                .filter(|m| m.covers(block_number) && m.cursor.is_none_or(|c| c < block_number))
                .map(|&m| m.clone())
                .collect();

            if covering.is_empty() {
                continue;
            }

            let pool_clone = pool.clone();
            if let Err(e) = decode_persist::process_block(
                &pool_clone,
                chain.chain_id,
                &chain_label,
                block_number,
                &block_hash_hex,
                &covering,
                &txs,
            )
            .await
            {
                tracing::warn!(chain = %chain_label, block = block_number, "process block: {e}");
            }

            // Advance cursors for monitors that covered this block.
            for m in &covering {
                if let Err(e) = decode_persist::advance_cursor(&pool, m, block_number).await {
                    tracing::warn!(chain = %chain_label, monitor = m.id, "advance cursor: {e}");
                }
                metrics::monitor_cursor(&chain_label, m.id, block_number);
                if m.end_block.is_some_and(|end| block_number >= end) {
                    metrics::monitor_completed(&chain_label, m.id, true);
                    tracing::info!(chain = %chain_label, monitor = m.id, "monitor completed range");
                }
            }
        }

        // Evict cache entries below the minimum cursor.
        let min_cursor = active
            .iter()
            .map(|m| m.cursor.unwrap_or(m.start_block - 1))
            .min()
            .unwrap_or(0);
        cache.evict_below(min_cursor);

        tokio::select! {
            _ = tokio::time::sleep(poll) => {}
            _ = cancel.cancelled() => break,
        }
    }

    tracing::info!(chain = %chain_label, "coordinator stopped");
    Ok(())
}
