use alloy::providers::{Provider, RootProvider};
use alloy::transports::http::reqwest::Client;

use crate::error::{AppError, AppResult};

/// HTTP RPC provider for an EVM chain (default Ethereum network).
pub type HttpProvider = RootProvider;

/// Build an HTTP RPC provider for the given URL.
pub fn build(rpc_url: &str) -> AppResult<HttpProvider> {
    let url = rpc_url
        .parse()
        .map_err(|e| AppError::BadRequest(format!("invalid rpc_url: {e}")))?;
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| AppError::Internal(anyhow::anyhow!("http client: {e}")))?;
    let rpc_client = alloy::rpc::client::ClientBuilder::default().http_with_client(client, url);
    Ok(RootProvider::new(rpc_client))
}

/// Convenience: current chain head number.
pub async fn head_number(provider: &HttpProvider) -> AppResult<u64> {
    let n = provider.get_block_number().await.map_err(AppError::Rpc)?;
    Ok(n)
}
