use alloy::consensus::Transaction;
use alloy::eips::BlockNumberOrTag;
use alloy::network::primitives::BlockTransactionsKind;
use alloy::network::{BlockResponse, ReceiptResponse, TransactionResponse};
use alloy::providers::Provider;

use crate::core::{BlockTransaction, ExecutedTransaction, SourceBlock, SourceLog};
use crate::error::{AppError, AppResult};
use crate::rpc::provider::HttpProvider;
use alloy::primitives::{Address, B256};
use alloy::rpc::types::Filter;

/// Fetch and cache the transaction fields needed for monitor matching.
pub async fn fetch_block(provider: &HttpProvider, block_number: u64) -> AppResult<SourceBlock> {
    let block = provider
        .get_block_by_number(BlockNumberOrTag::Number(block_number))
        .kind(BlockTransactionsKind::Full)
        .await
        .map_err(AppError::Rpc)?
        .ok_or_else(|| AppError::NotFound(format!("block {block_number}")))?;

    let mut out = Vec::new();
    for tx in block.transactions().txns() {
        let Some(to) = tx.to() else { continue };
        out.push(BlockTransaction {
            hash: tx.tx_hash(),
            to,
            input: tx.input().to_vec(),
        });
    }

    Ok(SourceBlock {
        number: i64::try_from(block_number).map_err(|error| AppError::Internal(error.into()))?,
        transactions: out,
    })
}

pub async fn fetch_logs(
    provider: &HttpProvider,
    block_number: u64,
    addresses: &[Address],
    topic0s: &[B256],
) -> AppResult<Vec<SourceLog>> {
    let filter = Filter::new()
        .select(block_number)
        .address(addresses.to_vec())
        .event_signature(topic0s.to_vec());
    provider
        .get_logs(&filter)
        .await
        .map_err(AppError::Rpc)?
        .into_iter()
        .map(|log| {
            Ok(SourceLog {
                block_number: log
                    .block_number
                    .map(|n| i64::try_from(n))
                    .transpose()
                    .map_err(|e| AppError::Internal(e.into()))?,
                transaction_hash: log.transaction_hash,
                log_index: log.log_index,
                address: log.address(),
                topics: log.topics().to_vec(),
                data: log.data().data.to_vec(),
                removed: log.removed,
            })
        })
        .collect()
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
            succeeded: receipt.status(),
        });
    }
    Ok(out)
}
