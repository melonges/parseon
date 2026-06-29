mod abi;
mod api;
mod config;
mod db;
mod error;
mod indexer;
mod metrics;
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

    // Metrics.
    let metrics_handle = metrics::init();
    metrics::describe();

    // Database.
    let pool = db::pool::connect(&config.database_url).await?;

    // Watcher registry.
    let registry = watcher::registry::Registry::new();
    registry.reload(&pool).await?;

    // Cancellation token shared by coordinator and API.
    let cancel = CancellationToken::new();

    // Build RPC URL: {erpc_url}/{chain_id}
    let rpc_url = format!("{}/{}", config.erpc_url, config.chain_id);
    let chain_config = indexer::coordinator::ChainConfig {
        chain_id: config.chain_id,
        rpc_url,
        batch_size: config.default_batch_size as i32,
    };

    // Coordinator: single-chain indexing loop.
    let coordinator_handle = tokio::spawn({
        let chain = chain_config;
        let pool = pool.clone();
        let registry = registry.clone();
        let cancel = cancel.clone();
        async move {
            if let Err(e) = indexer::coordinator::run(
                chain,
                pool,
                registry,
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
    let state = api::AppState::new(pool.clone(), registry.clone());
    let app = api::router(state, metrics_handle);
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
