use std::collections::HashMap;
use std::time::Instant;

use alloy::consensus::Transaction;
use alloy::eips::BlockNumberOrTag;
use alloy::network::primitives::BlockTransactionsKind;
use alloy::network::{BlockResponse, ReceiptResponse, TransactionResponse};
use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;

use crate::error::{AppError, AppResult};
use crate::metrics;
use crate::rpc::provider::HttpProvider;

/// A transaction paired with its receipt, ready for decoding/persistence.
#[derive(Clone)]
pub struct MatchedTx {
    pub hash: B256,
    pub from: Address,
    pub to: Address,
    pub input: Vec<u8>,
    pub value: U256,
    pub gas_used: u64,
    pub gas_price: u128,
    #[allow(dead_code)]
    pub status: bool,
}

/// Fetch a block's transactions + receipts and return matched txs that have
/// both a known `to` address and a successful receipt.
///
/// Failed transactions (status=false) are skipped: their calldata is not
/// meaningful for indexing.
pub async fn fetch_block(
    provider: &HttpProvider,
    block_number: u64,
    chain_label: &str,
) -> AppResult<(B256, Vec<MatchedTx>)> {
    let start = Instant::now();

    let block = provider
        .get_block_by_number(BlockNumberOrTag::Number(block_number))
        .kind(BlockTransactionsKind::Full)
        .await
        .map_err(AppError::Rpc)?
        .ok_or_else(|| AppError::NotFound(format!("block {block_number}")))?;

    let block_hash = block.header().hash;
    let receipts = provider
        .get_block_receipts(block_number.into())
        .await
        .map_err(AppError::Rpc)?
        .unwrap_or_default();

    let receipts_by_hash: HashMap<B256, &alloy::rpc::types::TransactionReceipt> = receipts
        .iter()
        .filter_map(|r| {
            let h = r.transaction_hash();
            (h != B256::ZERO).then_some((h, r))
        })
        .collect();

    let mut out = Vec::new();
    for tx in block.transactions().txns() {
        let Some(to) = tx.to() else { continue };
        let hash = tx.tx_hash();
        let Some(receipt) = receipts_by_hash.get(&hash) else {
            continue;
        };
        if !receipt.status() {
            continue;
        }
        out.push(MatchedTx {
            hash,
            from: tx.from(),
            to,
            input: tx.input().to_vec(),
            value: tx.value(),
            gas_used: receipt.gas_used(),
            gas_price: receipt.effective_gas_price(),
            status: true,
        });
    }

    metrics::block_fetch_seconds(chain_label).record(start.elapsed().as_secs_f64());
    metrics::blocks_fetched(chain_label);
    Ok((block_hash, out))
}
