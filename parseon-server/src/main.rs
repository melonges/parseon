mod api;
mod config;
mod error;
mod metrics;

use std::sync::Arc;
use std::time::Duration;

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

    tracing::info!("starting parseon");

    // Database.
    let pool = parseon_postgres::pool::connect(&config.database_url).await?;

    // Cancellation token shared by the supervisor and API.
    let cancel = CancellationToken::new();
    let supervisor_config = parseon_core::supervisor::SupervisorConfig {
        batch_size: i64::try_from(config.default_batch_size).unwrap_or(i64::MAX),
        poll_interval: Duration::from_millis(config.poll_interval_ms.max(100)),
        block_concurrency: config.block_concurrency.max(1),
        db_write_concurrency: config.db_write_concurrency.max(1),
    };
    let storage = Arc::new(parseon_postgres::PostgresStorage::new(pool.clone()));
    let telemetry = Arc::new(metrics::Metrics::default());
    let source_factory = Arc::new(parseon_rpc::JsonRpcBlockSourceFactory::new(
        parseon_rpc::RpcConfig {
            request_concurrency: config.rpc_request_concurrency.max(1),
            batch_size: config.rpc_batch_size.max(1),
        },
        telemetry.clone(),
    ));
    let runtime_status = parseon_core::status::RuntimeStatus::default();

    // Reconciles the database registry and runs one isolated worker per enabled chain.
    let supervisor_handle = tokio::spawn({
        let cancel = cancel.clone();
        let supervisor = parseon_core::supervisor::Supervisor::new(
            supervisor_config,
            storage.clone(),
            storage.clone(),
            source_factory.clone(),
            Arc::new(parseon_memory_cache::MemoryBlockCacheFactory::new(
                config.block_cache_size,
            )),
            runtime_status.clone(),
            telemetry.clone(),
        );
        async move {
            supervisor.run(cancel).await;
        }
    });

    // HTTP API.
    let state = api::AppState::new(
        (*storage).clone(),
        runtime_status,
        source_factory,
        telemetry,
    );
    let app = api::router(state);
    let listener = tokio::net::TcpListener::bind(&config.http_listen).await?;
    tracing::info!(listen = %config.http_listen, "http API listening");
    let server_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("http server: {e}");
        }
    });

    // Graceful shutdown on SIGINT, or if supervisor/server task ends.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = supervisor_handle => {
            tracing::warn!("supervisor task exited unexpectedly");
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
