mod abi;
mod api;
mod cache;
mod config;
mod core;
mod db;
mod error;
mod filter;
mod indexer;
mod monitor;
mod rpc;
mod scheduler;
mod storage;
mod worker;

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::rpc::BlockSource;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = config::Config::load();

    // Logging.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.rust_log)),
        )
        .try_init();

    tracing::info!(chain_id = config.chain_id, "starting parseon");

    // Database.
    let pool = db::pool::connect(&config.database_url).await?;

    // Fail fast if the direct RPC endpoint points at a different chain.
    let chain = core::Chain::new(config.chain_id)?;
    let block_source = Arc::new(rpc::provider::JsonRpcBlockSource::connect(&config.rpc_url)?);
    let rpc_chain_id = block_source.chain_id().await?;
    let configured_chain_id = u64::try_from(chain.id)?;
    anyhow::ensure!(
        rpc_chain_id == configured_chain_id,
        "RPC chain ID {rpc_chain_id} does not match configured CHAIN_ID {configured_chain_id}"
    );
    tracing::info!(chain_id = rpc_chain_id, "RPC chain ID validated");

    // Cancellation token shared by worker and API.
    let cancel = CancellationToken::new();

    let worker_config = worker::WorkerConfig {
        chain,
        batch_size: i64::try_from(config.default_batch_size).unwrap_or(i64::MAX),
        poll_interval: Duration::from_millis(config.poll_interval_ms.max(100)),
    };
    let storage = Arc::new(db::storage::PostgresStorage::new(pool.clone()));
    let block_cache = Arc::new(cache::MemoryBlockCache::new(config.block_cache_size));

    // Single-chain indexing worker.
    let worker_handle = tokio::spawn({
        let worker_config = worker_config;
        let storage = storage.clone();
        let block_source = block_source.clone();
        let block_cache = block_cache.clone();
        let cancel = cancel.clone();
        async move {
            worker::run(worker_config, storage, block_source, block_cache, cancel).await;
        }
    });

    // HTTP API.
    let state = api::AppState::new((*storage).clone());
    let app = api::router(state);
    let listener = tokio::net::TcpListener::bind(&config.http_listen).await?;
    tracing::info!(listen = %config.http_listen, "http API listening");
    let server_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("http server: {e}");
        }
    });

    // Graceful shutdown on SIGINT, or if worker/server task ends.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = worker_handle => {
            tracing::warn!("worker task exited unexpectedly");
        }
        _ = server_handle => {
            tracing::warn!("http server task exited unexpectedly");
        }
    }

    tracing::info!("shutting down");
    cancel.cancel();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    tracing::info!("bye");
    Ok(())
}
