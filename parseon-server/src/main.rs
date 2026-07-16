mod api;
mod config;
mod error;
mod metrics;

#[cfg(all(feature = "postgres-storage", feature = "mongodb-storage"))]
compile_error!("enable exactly one of `postgres-storage` or `mongodb-storage`");
#[cfg(not(any(feature = "postgres-storage", feature = "mongodb-storage")))]
compile_error!("enable exactly one of `postgres-storage` or `mongodb-storage`");

use std::sync::Arc;

use parseon_core::ports::{NoopSink, Sink, Storage};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = config::Config::load();

    // Logging.
    drop(
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                    tracing_subscriber::EnvFilter::new(&config.server.rust_log)
                }),
            )
            .try_init(),
    );

    tracing::info!("starting parseon");

    // Storage.
    #[cfg(feature = "postgres-storage")]
    let storage: Arc<dyn Storage> = Arc::new(parseon_postgres::PostgresStorage::new(
        parseon_postgres::pool::connect(&config.storage.storage_url).await?,
    ));
    #[cfg(feature = "mongodb-storage")]
    let storage: Arc<dyn Storage> = Arc::new(
        parseon_mongodb::MongoStorage::connect(
            &config.storage.storage_url,
            &config.storage.storage_database,
        )
        .await?,
    );
    let registered_chains = storage.list_registered_chains().await?;

    #[cfg(feature = "webhook-sink")]
    let sink: Arc<dyn Sink> = Arc::new(parseon_webhook_sink::WebhookSink::new(
        config.webhook.url.clone(),
        config.webhook.concurrency,
    )?);
    #[cfg(not(feature = "webhook-sink"))]
    let sink: Arc<dyn Sink> = Arc::new(NoopSink);

    // Cancellation token shared by the supervisor and API.
    let cancel = CancellationToken::new();
    let supervisor_config = parseon_core::supervisor::SupervisorConfig {
        batch_size: config.indexing.default_batch_size,
        poll_interval: config.indexing.poll_interval,
        block_concurrency: config.indexing.block_concurrency,
        storage_write_concurrency: config.indexing.storage_write_concurrency,
    };
    let telemetry = Arc::new(metrics::Metrics::default());
    let source_factory = Arc::new(parseon_rpc::JsonRpcBlockSourceFactory::new(
        parseon_rpc::RpcConfig {
            request_concurrency: config.rpc.request_concurrency,
            batch_size: config.rpc.batch_size,
        },
        telemetry.clone(),
    ));
    let runtime_status = parseon_core::status::RuntimeStatus::default();
    let chains = parseon_core::services::ChainService::new(storage.clone(), source_factory.clone());
    let monitors =
        parseon_core::services::MonitorService::new(storage.clone(), source_factory.clone());

    // Runs one isolated worker per chain enabled in the startup registry snapshot.
    let supervisor_handle = tokio::spawn({
        let cancel = cancel.clone();
        let supervisor = parseon_core::supervisor::Supervisor::new(
            supervisor_config,
            registered_chains,
            storage.clone(),
            sink.clone(),
            source_factory.clone(),
            Arc::new(parseon_memory_cache::MemoryBlockCacheFactory::new(
                config.indexing.block_cache_size,
            )),
            runtime_status.clone(),
            telemetry.clone(),
        );
        async move {
            supervisor.run(cancel).await;
        }
    });

    // HTTP API.
    let state = api::AppState::new(chains, monitors, runtime_status, telemetry);
    let app = api::router(state);
    let listener = tokio::net::TcpListener::bind(&config.server.http_listen).await?;
    tracing::info!(listen = %config.server.http_listen, "http API listening");
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
    sink.shutdown();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    tracing::info!("bye");
    Ok(())
}
