use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use clap::{Args, Parser};
use parseon_core::Url;

#[derive(Debug, Clone, Parser)]
#[command(name = "parseon", about = "Parseon — EVM indexer with runtime ABI decoding")]
pub struct Config {
    #[command(flatten)]
    pub database: DatabaseConfig,
    #[command(flatten)]
    pub server: ServerConfig,
    #[command(flatten)]
    pub indexing: IndexingConfig,
    #[command(flatten)]
    pub rpc: RpcConfig,
}

#[derive(Debug, Clone, Args)]
pub struct DatabaseConfig {
    /// PostgreSQL connection string
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Url,
}

#[derive(Debug, Clone, Args)]
pub struct ServerConfig {
    /// HTTP listen address for the API
    #[arg(long, env = "HTTP_LISTEN", default_value = "0.0.0.0:8080")]
    pub http_listen: SocketAddr,
    /// Log filter directive (e.g. `info,parseon=debug`)
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    pub rust_log: String,
}

#[derive(Debug, Clone, Args)]
pub struct IndexingConfig {
    /// Global poll interval used by the chain worker
    #[arg(long, env = "POLL_INTERVAL_MS", default_value = "2000", value_parser = parse_poll_interval)]
    pub poll_interval: Duration,
    /// Default batch size applied to each chain
    #[arg(long, env = "DEFAULT_BATCH_SIZE", default_value = "10")]
    pub default_batch_size: NonZeroU64,
    /// Block cache capacity per chain worker
    #[arg(long, env = "BLOCK_CACHE_SIZE", default_value = "512")]
    pub block_cache_size: NonZeroUsize,
    /// Blocks prepared concurrently by each chain worker
    #[arg(long, env = "BLOCK_CONCURRENCY", default_value = "4")]
    pub block_concurrency: NonZeroUsize,
    /// Maximum concurrent atomic database commits across all chains
    #[arg(long, env = "DB_WRITE_CONCURRENCY", default_value = "4")]
    pub db_write_concurrency: NonZeroUsize,
}

#[derive(Debug, Clone, Args)]
pub struct RpcConfig {
    /// Maximum concurrent RPC requests per registered chain
    #[arg(long, env = "RPC_REQUEST_CONCURRENCY", default_value = "16")]
    pub request_concurrency: NonZeroUsize,
    /// Maximum JSON-RPC calls grouped into one receipt batch
    #[arg(long, env = "RPC_BATCH_SIZE", default_value = "20")]
    pub batch_size: NonZeroUsize,
}

fn parse_poll_interval(value: &str) -> Result<Duration, String> {
    let milliseconds = value
        .parse::<u64>()
        .map_err(|error| format!("invalid poll interval: {error}"))?;
    if milliseconds < 100 {
        return Err("poll interval must be at least 100 ms".into());
    }
    Ok(Duration::from_millis(milliseconds))
}

#[cfg(test)]
mod tests {
    use super::parse_poll_interval;
    use std::time::Duration;

    #[test]
    fn poll_interval_is_validated_once() {
        assert_eq!(parse_poll_interval("100").unwrap(), Duration::from_millis(100));
        assert!(parse_poll_interval("99").is_err());
        assert!(parse_poll_interval("invalid").is_err());
    }
}

impl Config {
    pub fn load() -> Self {
        let _ = dotenvy::dotenv();
        Self::parse()
    }
}
