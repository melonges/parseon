use alloy::eips::BlockNumberOrTag;
use alloy::network::AnyNetwork;
use alloy::network::BlockResponse;
use alloy::providers::{Provider, RootProvider};
use alloy::transports::http::reqwest::Client;

use crate::error::{AppError, AppResult};

/// HTTP RPC provider for an EVM chain (default Ethereum network).
pub type HttpProvider = RootProvider<AnyNetwork>;

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
    Ok(RootProvider::<AnyNetwork>::new(rpc_client))
}

pub async fn chain_id(provider: &HttpProvider) -> AppResult<u64> {
    provider.get_chain_id().await.map_err(AppError::Rpc)
}

/// Highest block that the RPC endpoint reports as finalized.
pub async fn finalized_number(provider: &HttpProvider) -> AppResult<u64> {
    let block = provider
        .get_block_by_number(BlockNumberOrTag::Finalized)
        .await
        .map_err(AppError::Rpc)?
        .ok_or_else(|| AppError::NotFound("finalized block".to_string()))?;
    Ok(block.header().number)
}
