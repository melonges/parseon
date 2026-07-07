use clap::{Args, Parser};

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
    pub database_url: String,
}

#[derive(Debug, Clone, Args)]
pub struct ServerConfig {
    /// HTTP listen address for the API
    #[arg(long, env = "HTTP_LISTEN", default_value = "0.0.0.0:8080")]
    pub http_listen: String,
    /// Log filter directive (e.g. `info,parseon=debug`)
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    pub rust_log: String,
}

#[derive(Debug, Clone, Args)]
pub struct IndexingConfig {
    /// Global poll interval used by the chain worker
    #[arg(long, env = "POLL_INTERVAL_MS", default_value_t = 2000)]
    pub poll_interval_ms: u64,
    /// Default batch size applied to each chain
    #[arg(long, env = "DEFAULT_BATCH_SIZE", default_value_t = 10)]
    pub default_batch_size: u64,
    /// Block cache capacity per chain worker
    #[arg(long, env = "BLOCK_CACHE_SIZE", default_value_t = 512)]
    pub block_cache_size: usize,
    /// Blocks prepared concurrently by each chain worker
    #[arg(long, env = "BLOCK_CONCURRENCY", default_value_t = 4)]
    pub block_concurrency: usize,
    /// Maximum concurrent atomic database commits across all chains
    #[arg(long, env = "DB_WRITE_CONCURRENCY", default_value_t = 4)]
    pub db_write_concurrency: usize,
}

#[derive(Debug, Clone, Args)]
pub struct RpcConfig {
    /// Maximum concurrent RPC requests per registered chain
    #[arg(long, env = "RPC_REQUEST_CONCURRENCY", default_value_t = 16)]
    pub request_concurrency: usize,
    /// Maximum JSON-RPC calls grouped into one receipt batch
    #[arg(long, env = "RPC_BATCH_SIZE", default_value_t = 20)]
    pub batch_size: usize,
}

impl Config {
    pub fn load() -> Self {
        let _ = dotenvy::dotenv();
        Self::parse()
    }
}
