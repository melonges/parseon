use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use clap::{Args, Parser};
use parseon_core::Url;

#[derive(Debug, Clone, Parser)]
#[command(name = "parseon", about = "Parseon — EVM indexer with runtime ABI decoding")]
pub(crate) struct Config {
    #[command(flatten)]
    pub storage: StorageConfig,
    #[command(flatten)]
    pub server: ServerConfig,
    #[command(flatten)]
    pub indexing: IndexingConfig,
    #[command(flatten)]
    pub rpc: RpcConfig,
    #[cfg(feature = "webhook-sink")]
    #[command(flatten)]
    pub webhook: WebhookConfig,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct StorageConfig {
    /// Selected storage adapter connection string
    #[arg(long, env = "STORAGE_URL")]
    pub storage_url: Url,
    /// MongoDB database name
    #[cfg(feature = "mongodb-storage")]
    #[arg(long, env = "STORAGE_DATABASE", default_value = "parseon")]
    pub storage_database: String,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ServerConfig {
    /// HTTP listen address for the API
    #[arg(long, env = "HTTP_LISTEN", default_value = "0.0.0.0:8080")]
    pub http_listen: SocketAddr,
    /// Log filter directive (e.g. `info,parseon=debug`)
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    pub rust_log: String,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct IndexingConfig {
    /// Global poll interval used by the chain worker
    #[arg(long, env = "POLL_INTERVAL_MS", default_value = "2000", value_parser = parse_poll_interval)]
    pub poll_interval: Duration,
    /// Default batch size applied to each chain
    #[arg(long, env = "DEFAULT_BATCH_SIZE", default_value = "10")]
    pub default_batch_size: NonZeroU64,
    /// Block cache capacity per chain worker; zero disables caching
    #[arg(long, env = "BLOCK_CACHE_SIZE", default_value = "512")]
    pub block_cache_size: usize,
    /// Blocks prepared concurrently by each chain worker
    #[arg(long, env = "BLOCK_CONCURRENCY", default_value = "4")]
    pub block_concurrency: NonZeroUsize,
    /// Maximum concurrent atomic storage commits across all chains
    #[arg(long, env = "STORAGE_WRITE_CONCURRENCY", default_value = "4")]
    pub storage_write_concurrency: NonZeroUsize,
}

#[cfg(feature = "webhook-sink")]
#[derive(Debug, Clone, Args)]
pub(crate) struct WebhookConfig {
    /// Destination for best-effort post-commit result batches
    #[arg(long = "webhook-url", env = "WEBHOOK_URL")]
    pub url: Url,
    /// Maximum in-flight webhook attempts before new batches are dropped
    #[arg(long = "webhook-concurrency", env = "WEBHOOK_CONCURRENCY", default_value = "16")]
    pub concurrency: NonZeroUsize,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct RpcConfig {
    /// Maximum concurrent RPC requests per registered chain
    #[arg(long, env = "RPC_REQUEST_CONCURRENCY", default_value = "16")]
    pub request_concurrency: NonZeroUsize,
    /// Maximum JSON-RPC calls grouped into one receipt batch
    #[arg(long, env = "RPC_BATCH_SIZE", default_value = "20")]
    pub batch_size: NonZeroUsize,
}

fn parse_poll_interval(value: &str) -> Result<Duration, String> {
    let milliseconds =
        value.parse::<u64>().map_err(|error| format!("invalid poll interval: {error}"))?;
    if milliseconds < 100 {
        return Err("poll interval must be at least 100 ms".into());
    }
    Ok(Duration::from_millis(milliseconds))
}

impl Config {
    pub(crate) fn load() -> Self {
        drop(dotenvy::dotenv());
        Self::parse()
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, parse_poll_interval};
    use clap::Parser;
    use std::time::Duration;

    #[cfg(feature = "webhook-sink")]
    fn with_webhook(mut args: Vec<&'static str>) -> Vec<&'static str> {
        args.extend(["--webhook-url", "http://localhost/hook"]);
        args
    }

    #[cfg(not(feature = "webhook-sink"))]
    fn with_webhook(args: Vec<&'static str>) -> Vec<&'static str> {
        args
    }

    #[test]
    fn poll_interval_is_validated_once() {
        assert_eq!(
            parse_poll_interval("100").expect("valid poll interval"),
            Duration::from_millis(100)
        );
        assert!(parse_poll_interval("99").is_err());
        assert!(parse_poll_interval("invalid").is_err());
    }

    #[test]
    fn accepts_storage_url() {
        let args = with_webhook(vec!["parseon", "--storage-url", "postgres://localhost/parseon"]);
        assert_eq!(
            Config::try_parse_from(args).unwrap().storage.storage_url.as_str(),
            "postgres://localhost/parseon"
        );
    }

    #[test]
    fn accepts_zero_block_cache_size() {
        let args = with_webhook(vec![
            "parseon",
            "--storage-url",
            "postgres://localhost/parseon",
            "--block-cache-size",
            "0",
        ]);
        assert_eq!(Config::try_parse_from(args).unwrap().indexing.block_cache_size, 0);
    }

    #[test]
    fn rejects_removed_database_configuration_names() {
        let args = with_webhook(vec![
            "parseon",
            "--storage-url",
            "postgres://localhost/parseon",
            "--database-url",
            "postgres://localhost/legacy",
        ]);
        assert!(Config::try_parse_from(args).is_err());

        let args = with_webhook(vec![
            "parseon",
            "--storage-url",
            "postgres://localhost/parseon",
            "--db-write-concurrency",
            "1",
        ]);
        assert!(Config::try_parse_from(args).is_err());
    }
}
