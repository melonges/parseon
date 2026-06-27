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

    /// Global poll interval used by every chain coordinator
    #[arg(long, env = "POLL_INTERVAL_MS", default_value_t = 2000)]
    pub poll_interval_ms: u64,

    /// Default batch size applied to each chain
    #[arg(long, env = "DEFAULT_BATCH_SIZE", default_value_t = 10)]
    pub default_batch_size: u64,

    /// Block cache capacity per chain coordinator
    #[arg(long, env = "BLOCK_CACHE_SIZE", default_value_t = 512)]
    pub block_cache_size: usize,

    /// erpc base URL (e.g. http://erpc:4000/main/evm)
    #[arg(long, env = "ERPC_URL")]
    pub erpc_url: String,

    /// Chain ID to index (e.g. 1 for Ethereum, 42161 for Arbitrum)
    #[arg(long, env = "CHAIN_ID")]
    pub chain_id: i64,
}

impl Config {
    pub fn load() -> Self {
        let _ = dotenvy::dotenv();
        Config::parse()
    }
}
