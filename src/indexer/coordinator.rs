use std::time::Duration;

use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::db::monitor_repo;
use crate::error::AppResult;
use crate::indexer::decode_persist;
use crate::rpc::{
    block_cache::BlockCache,
    fetch::{fetch_block, fetch_receipts},
    provider,
};
use crate::watcher::model::Monitor;

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
///   1. Load active monitors and their current cursors from PostgreSQL.
///   2. If none, sleep and continue.
///   3. Fetch the finalized chain head.
///   4. Compute the union of blocks needed across monitors (cursor+1 .. min(end||head, head)), capped at batch_size.
///   5. Fetch each unique block once (shared cache), fan out to covering monitors, decode + persist.
///   6. Commit decoded rows, parameter rows, and cursor advances atomically.
///   7. Evict cache entries below the minimum cursor.
pub async fn run(
    chain: ChainConfig,
    pool: PgPool,
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

        let rows = match monitor_repo::list(&pool).await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(chain = %chain_label, "load monitors: {e}");
                sleep_or_cancel(poll, &cancel).await;
                continue;
            }
        };
        let monitors = match rows
            .iter()
            .map(Monitor::try_from)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(monitors) => monitors,
            Err(e) => {
                tracing::warn!(chain = %chain_label, "invalid monitor row: {e}");
                sleep_or_cancel(poll, &cancel).await;
                continue;
            }
        };
        let active: Vec<_> = monitors
            .iter()
            .filter(|m| m.enabled && !m.completed)
            .collect();
        if active.is_empty() {
            sleep_or_cancel(poll, &cancel).await;
            continue;
        }

        let head = match provider::finalized_number(&provider).await {
            Ok(h) => h as i64,
            Err(e) => {
                tracing::warn!(chain = %chain_label, "finalized head fetch: {e}");
                sleep_or_cancel(poll, &cancel).await;
                continue;
            }
        };

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

            // Fan out to monitors that cover this block and haven't passed it.
            let covering: Vec<_> = active
                .iter()
                .filter(|m| m.covers(block_number) && m.cursor.is_none_or(|c| c < block_number))
                .map(|&m| m.clone())
                .collect();

            if covering.is_empty() {
                continue;
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
                        break;
                    }
                }
            };

            let block_hash_hex = format!("{block_hash}");

            let candidates: Vec<_> = txs
                .iter()
                .filter(|tx| {
                    let selector = tx.input.get(..4).unwrap_or(&[]);
                    covering
                        .iter()
                        .any(|m| m.address == tx.to && m.selector == selector)
                })
                .cloned()
                .collect();
            let matched_txs = match fetch_receipts(&provider, &candidates).await {
                Ok(txs) => txs,
                Err(e) => {
                    tracing::warn!(chain = %chain_label, block = block_number, "fetch receipts: {e}");
                    break;
                }
            };

            if let Err(e) = decode_persist::process_block(
                &pool,
                &chain_label,
                block_number,
                &block_hash_hex,
                &covering,
                &matched_txs,
            )
            .await
            {
                tracing::warn!(chain = %chain_label, block = block_number, "process block: {e}");
                break;
            }

            for m in &covering {
                if m.end_block.is_some_and(|end| block_number >= end) {
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

        sleep_or_cancel(poll, &cancel).await;
    }

    tracing::info!(chain = %chain_label, "coordinator stopped");
    Ok(())
}

async fn sleep_or_cancel(poll: Duration, cancel: &CancellationToken) {
    tokio::select! {
        _ = tokio::time::sleep(poll) => {}
        _ = cancel.cancelled() => {}
    }
}
