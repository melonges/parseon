mod api;
mod config;
mod error;
mod metrics;

#[cfg(all(feature = "postgres-storage", feature = "mongodb-storage"))]
compile_error!("enable exactly one of `postgres-storage` or `mongodb-storage`");
#[cfg(not(any(feature = "postgres-storage", feature = "mongodb-storage")))]
compile_error!("enable exactly one of `postgres-storage` or `mongodb-storage`");

use std::sync::Arc;

#[cfg(not(feature = "webhook-sink"))]
use parseon_core::ports::NoopSink;
use parseon_core::ports::{Sink, Storage};
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
    let listener = tokio::net::TcpListener::bind(&config.server.http_listen).await?;
    tracing::info!(listen = %config.server.http_listen, "http API listening");

    // Runs one isolated worker per chain enabled in the startup registry snapshot.
    let mut supervisor_handle = tokio::spawn({
        let cancel = cancel.clone();
        let supervisor = parseon_core::supervisor::Supervisor::new(
            supervisor_config,
            registered_chains,
            parseon_core::supervisor::SupervisorDependencies {
                storage: storage.clone(),
                sink: sink.clone(),
                source_factory: source_factory.clone(),
                cache_factory: Arc::new(parseon_memory_cache::MemoryBlockCacheFactory::new(
                    config.indexing.block_cache_size,
                )),
                statuses: runtime_status.clone(),
                telemetry: telemetry.clone(),
            },
        );
        async move {
            supervisor.run(cancel).await;
        }
    });

    // HTTP API.
    let state = api::AppState::new(chains, monitors, runtime_status, telemetry);
    let app = api::router(state);
    let server_cancel = cancel.clone();
    let mut server_handle = tokio::spawn(async move {
        if let Err(e) =
            axum::serve(listener, app).with_graceful_shutdown(server_cancel.cancelled_owned()).await
        {
            tracing::error!("http server: {e}");
        }
    });

    // Graceful shutdown on SIGINT, or if supervisor/server task ends.
    enum ShutdownReason {
        Signal,
        Supervisor(Result<(), tokio::task::JoinError>),
        Server(Result<(), tokio::task::JoinError>),
    }
    let reason = tokio::select! {
        _ = tokio::signal::ctrl_c() => ShutdownReason::Signal,
        result = &mut supervisor_handle => ShutdownReason::Supervisor(result),
        result = &mut server_handle => ShutdownReason::Server(result),
    };

    tracing::info!("shutting down");
    cancel.cancel();
    match reason {
        ShutdownReason::Signal => {
            if let Err(error) = supervisor_handle.await {
                tracing::error!(%error, "supervisor task failed during shutdown");
            }
            if let Err(error) = server_handle.await {
                tracing::error!(%error, "HTTP server task failed during shutdown");
            }
        }
        ShutdownReason::Supervisor(result) => {
            if let Err(error) = result {
                tracing::error!(%error, "supervisor task failed");
            } else {
                tracing::warn!("supervisor task exited unexpectedly");
            }
            if let Err(error) = server_handle.await {
                tracing::error!(%error, "HTTP server task failed during shutdown");
            }
        }
        ShutdownReason::Server(result) => {
            if let Err(error) = result {
                tracing::error!(%error, "HTTP server task failed");
            } else {
                tracing::warn!("HTTP server task exited unexpectedly");
            }
            if let Err(error) = supervisor_handle.await {
                tracing::error!(%error, "supervisor task failed during shutdown");
            }
        }
    }
    sink.shutdown();
    tracing::info!("bye");
    Ok(())
}
