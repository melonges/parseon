use alloy::consensus::Transaction;
use alloy::eips::BlockNumberOrTag;
use alloy::network::primitives::BlockTransactionsKind;
use alloy::network::{BlockResponse, ReceiptResponse, TransactionResponse};
use alloy::providers::Provider;
use alloy_rpc_types_any::AnyTransactionReceipt;
use anyhow::Context;

use alloy::rpc::types::Filter;
use parseon_core::{BlockTransaction, ExecutionOutcome, SourceBlock, SourceLog, TxHash};

use crate::provider::HttpProvider;

#[derive(Debug, thiserror::Error)]
pub(crate) enum BlockReceiptsResponseError {
    #[error("block receipts response missing for block {0}")]
    MissingBlock(u64),
    #[error("block receipts response is missing transaction {0}")]
    MissingTransaction(TxHash),
}

/// Fetch and cache the transaction fields needed for monitor matching.
pub(crate) async fn fetch_block(
    provider: &HttpProvider,
    block_number: u64,
) -> anyhow::Result<SourceBlock> {
    let block = provider
        .get_block_by_number(BlockNumberOrTag::Number(block_number))
        .kind(BlockTransactionsKind::Full)
        .await?
        .with_context(|| format!("block {block_number} not found"))?;
    anyhow::ensure!(
        block.header().number == block_number,
        "block source returned block {} for request {block_number}",
        block.header().number
    );
    let transactions = block
        .try_into_transactions()
        .map_err(|_| anyhow::anyhow!("block {block_number} did not contain full transactions"))?;

    let mut out = Vec::with_capacity(transactions.len());
    for tx in transactions {
        let Some(to) = tx.to() else { continue };
        out.push(BlockTransaction {
            hash: tx.tx_hash(),
            from: tx.from(),
            to,
            input: tx.input().clone(),
        });
    }

    Ok(SourceBlock { number: block_number, transactions: out })
}

pub(crate) async fn fetch_logs(
    provider: &HttpProvider,
    filter: &Filter,
) -> anyhow::Result<Vec<SourceLog>> {
    provider
        .get_logs(filter)
        .await?
        .into_iter()
        .map(|log| {
            let address = log.inner.address;
            let (topics, data) = log.inner.data.split();
            Ok(SourceLog {
                block_number: log.block_number,
                transaction_hash: log.transaction_hash,
                log_index: log.log_index,
                address,
                topics,
                data,
                removed: log.removed,
            })
        })
        .collect()
}

pub(crate) async fn fetch_receipt(
    provider: &HttpProvider,
    transaction_hash: TxHash,
) -> anyhow::Result<ExecutionOutcome> {
    let receipt = provider
        .get_transaction_receipt(transaction_hash)
        .await?
        .with_context(|| format!("receipt for {transaction_hash} not found"))?;
    anyhow::ensure!(
        receipt.transaction_hash() == transaction_hash,
        "receipt response hash does not match request {transaction_hash}"
    );
    Ok(ExecutionOutcome { transaction_hash, succeeded: receipt.status() })
}

pub(crate) async fn fetch_receipt_batch(
    provider: &HttpProvider,
    transaction_hashes: &[TxHash],
) -> anyhow::Result<Vec<ExecutionOutcome>> {
    let mut batch = alloy::rpc::client::BatchRequest::new(provider.client());
    let waiters = transaction_hashes
        .iter()
        .map(|transaction_hash| {
            batch
                .add_call::<_, Option<AnyTransactionReceipt>>(
                    "eth_getTransactionReceipt",
                    &(*transaction_hash,),
                )
                .map_err(anyhow::Error::from)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    batch.send().await?;

    let mut out = Vec::with_capacity(transaction_hashes.len());
    for (transaction_hash, waiter) in transaction_hashes.iter().copied().zip(waiters) {
        let receipt =
            waiter.await?.with_context(|| format!("receipt for {transaction_hash} not found"))?;
        anyhow::ensure!(
            receipt.transaction_hash() == transaction_hash,
            "receipt response hash does not match request {transaction_hash}"
        );
        out.push(ExecutionOutcome { transaction_hash, succeeded: receipt.status() });
    }
    Ok(out)
}

pub(crate) async fn fetch_block_receipts(
    provider: &HttpProvider,
    block_number: u64,
    transaction_hashes: &[TxHash],
) -> anyhow::Result<Vec<ExecutionOutcome>> {
    let receipts = provider
        .get_block_receipts(BlockNumberOrTag::Number(block_number).into())
        .await?
        .ok_or(BlockReceiptsResponseError::MissingBlock(block_number))?;
    let mut statuses = receipts
        .into_iter()
        .map(|receipt| (receipt.transaction_hash(), receipt.status()))
        .collect::<std::collections::HashMap<_, _>>();
    transaction_hashes
        .iter()
        .copied()
        .map(|transaction_hash| {
            let succeeded = statuses
                .remove(&transaction_hash)
                .ok_or(BlockReceiptsResponseError::MissingTransaction(transaction_hash))?;
            Ok(ExecutionOutcome { transaction_hash, succeeded })
        })
        .collect()
}
