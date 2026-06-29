use alloy::consensus::Transaction;
use alloy::eips::BlockNumberOrTag;
use alloy::network::primitives::BlockTransactionsKind;
use alloy::network::{BlockResponse, ReceiptResponse, TransactionResponse};
use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;

use crate::error::{AppError, AppResult};
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

/// Transaction fields cached from a full block before monitor matching.
#[derive(Clone)]
pub struct BlockTx {
    pub hash: B256,
    pub from: Address,
    pub to: Address,
    pub input: Vec<u8>,
    pub value: U256,
}

/// Fetch and cache the transaction fields needed for monitor matching.
pub async fn fetch_block(
    provider: &HttpProvider,
    block_number: u64,
    _chain_label: &str,
) -> AppResult<(B256, Vec<BlockTx>)> {
    let block = provider
        .get_block_by_number(BlockNumberOrTag::Number(block_number))
        .kind(BlockTransactionsKind::Full)
        .await
        .map_err(AppError::Rpc)?
        .ok_or_else(|| AppError::NotFound(format!("block {block_number}")))?;

    let block_hash = block.header().hash;
    let mut out = Vec::new();
    for tx in block.transactions().txns() {
        let Some(to) = tx.to() else { continue };
        out.push(BlockTx {
            hash: tx.tx_hash(),
            from: tx.from(),
            to,
            input: tx.input().to_vec(),
            value: tx.value(),
        });
    }

    Ok((block_hash, out))
}

/// Fetch receipts only for transactions that matched a monitor. Base's public
/// RPC does not expose `eth_getBlockReceipts`, so this avoids one receipt call
/// per unrelated transaction in the block.
pub async fn fetch_receipts(provider: &HttpProvider, txs: &[BlockTx]) -> AppResult<Vec<MatchedTx>> {
    let mut out = Vec::with_capacity(txs.len());
    for tx in txs {
        let receipt = provider
            .get_transaction_receipt(tx.hash)
            .await
            .map_err(AppError::Rpc)?
            .ok_or_else(|| AppError::NotFound(format!("receipt for {}", tx.hash)))?;
        if !receipt.status() {
            continue;
        }
        out.push(MatchedTx {
            hash: tx.hash,
            from: tx.from,
            to: tx.to,
            input: tx.input.clone(),
            value: tx.value,
            gas_used: receipt.gas_used(),
            gas_price: receipt.effective_gas_price(),
            status: true,
        });
    }
    Ok(out)
}
