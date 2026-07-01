use alloy::consensus::Transaction;
use alloy::eips::BlockNumberOrTag;
use alloy::network::primitives::BlockTransactionsKind;
use alloy::network::{BlockResponse, ReceiptResponse, TransactionResponse};
use alloy::providers::Provider;

use crate::core::{BlockTransaction, ExecutedTransaction, SourceBlock};
use crate::error::{AppError, AppResult};
use crate::rpc::provider::HttpProvider;

/// Fetch and cache the transaction fields needed for monitor matching.
pub async fn fetch_block(provider: &HttpProvider, block_number: u64) -> AppResult<SourceBlock> {
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
        out.push(BlockTransaction {
            hash: tx.tx_hash(),
            from: tx.from(),
            to,
            input: tx.input().to_vec(),
            value: tx.value(),
        });
    }

    Ok(SourceBlock {
        number: i64::try_from(block_number).map_err(|error| AppError::Internal(error.into()))?,
        hash: block_hash,
        transactions: out,
    })
}

/// Fetch receipts only for transactions that matched a monitor. Base's public
/// RPC does not expose `eth_getBlockReceipts`, so this avoids one receipt call
/// per unrelated transaction in the block.
pub async fn fetch_receipts(
    provider: &HttpProvider,
    txs: &[BlockTransaction],
) -> AppResult<Vec<ExecutedTransaction>> {
    let mut out = Vec::with_capacity(txs.len());
    for tx in txs {
        let receipt = provider
            .get_transaction_receipt(tx.hash)
            .await
            .map_err(AppError::Rpc)?
            .ok_or_else(|| AppError::NotFound(format!("receipt for {}", tx.hash)))?;
        out.push(ExecutedTransaction {
            transaction: tx.clone(),
            gas_used: receipt.gas_used(),
            gas_price: receipt.effective_gas_price(),
            succeeded: receipt.status(),
        });
    }
    Ok(out)
}
