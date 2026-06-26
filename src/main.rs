mod abi;
mod api;
mod config;
mod db;
mod error;
mod indexer;
mod metrics;
mod rpc;
mod supervisor;
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

    tracing::info!("starting evm-indexer");

    // Metrics.
    let metrics_handle = metrics::init();
    metrics::describe();

    // Database.
    let pool = db::pool::connect(&config.database_url).await?;

    // Watcher registry.
    let registry = watcher::registry::Registry::new();
    registry.reload(&pool).await?;

    // Cancellation token shared by the supervisor and API.
    let cancel = CancellationToken::new();

    // Supervisor: per-chain coordinators.
    let supervisor = supervisor::Supervisor::new(
        pool.clone(),
        registry.clone(),
        config.block_cache_size,
        cancel.clone(),
    );
    let mut supervisor_handle = tokio::spawn(async move {
        if let Err(e) = supervisor.run().await {
            tracing::error!("supervisor: {e}");
        }
    });

    // HTTP API.
    let state = api::AppState::new(pool.clone(), registry.clone());
    let app = api::router(state, metrics_handle);
    let listener = tokio::net::TcpListener::bind(&config.http_listen).await?;
    tracing::info!(listen = %config.http_listen, "http API listening");
    let mut server_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("http server: {e}");
        }
    });

    // Graceful shutdown on SIGINT, or if the supervisor/server task ends.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = &mut supervisor_handle => {
            tracing::warn!("supervisor task exited unexpectedly");
        }
        _ = &mut server_handle => {
            tracing::warn!("http server task exited unexpectedly");
        }
    }

    tracing::info!("shutting down");
    cancel.cancel();
    // Give in-flight work a moment to drain.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    tracing::info!("bye");
    Ok(())
}
