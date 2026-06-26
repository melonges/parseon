use clap::Parser;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Parser)]
#[command(name = "evm-indexer", about = "EVM indexer with runtime ABI decoding")]
pub struct Config {
    /// PostgreSQL connection string
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: String,

    /// HTTP listen address for the API
    #[arg(long, env = "HTTP_LISTEN", default_value = "0.0.0.0:8080")]
    pub http_listen: String,

    /// Log filter directive (e.g. `info,evm_indexer=debug`)
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    pub rust_log: String,

    /// Poll interval override applied when a chain has no explicit poll_interval_ms
    #[arg(long, env = "DEFAULT_POLL_INTERVAL_MS", default_value_t = 2000)]
    pub default_poll_interval_ms: u64,

    /// Default batch size applied when a chain has no explicit batch_size
    #[arg(long, env = "DEFAULT_BATCH_SIZE", default_value_t = 10)]
    pub default_batch_size: u64,

    /// Block cache capacity per chain coordinator
    #[arg(long, env = "BLOCK_CACHE_SIZE", default_value_t = 512)]
    pub block_cache_size: usize,
}

impl Config {
    pub fn load() -> Self {
        let _ = dotenvy::dotenv();
        Config::parse()
    }
}
