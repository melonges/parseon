mod abi;
mod api;
mod config;
mod db;
mod error;
mod indexer;
mod rpc;
mod watcher;

use tokio_util::sync::CancellationToken;

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
    let rpc_provider = rpc::provider::build(&config.rpc_url)?;
    let rpc_chain_id = rpc::provider::chain_id(&rpc_provider).await?;
    let configured_chain_id = u64::try_from(config.chain_id)
        .map_err(|_| anyhow::anyhow!("CHAIN_ID must be non-negative"))?;
    anyhow::ensure!(
        rpc_chain_id == configured_chain_id,
        "RPC chain ID {rpc_chain_id} does not match configured CHAIN_ID {configured_chain_id}"
    );
    tracing::info!(chain_id = rpc_chain_id, "RPC chain ID validated");

    // Cancellation token shared by coordinator and API.
    let cancel = CancellationToken::new();

    let chain_config = indexer::coordinator::ChainConfig {
        chain_id: config.chain_id,
        rpc_url: config.rpc_url.clone(),
        batch_size: config.default_batch_size as i32,
    };

    // Coordinator: single-chain indexing loop.
    let coordinator_handle = tokio::spawn({
        let chain = chain_config;
        let pool = pool.clone();
        let cancel = cancel.clone();
        async move {
            if let Err(e) = indexer::coordinator::run(
                chain,
                pool,
                config.block_cache_size,
                config.poll_interval_ms,
                cancel,
            )
            .await
            {
                tracing::error!("coordinator: {e}");
            }
        }
    });

    // HTTP API.
    let state = api::AppState::new(pool.clone());
    let app = api::router(state);
    let listener = tokio::net::TcpListener::bind(&config.http_listen).await?;
    tracing::info!(listen = %config.http_listen, "http API listening");
    let server_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("http server: {e}");
        }
    });

    // Graceful shutdown on SIGINT, or if coordinator/server task ends.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = coordinator_handle => {
            tracing::warn!("coordinator task exited unexpectedly");
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
