use alloy::primitives::U256;
use sqlx::PgPool;
use std::time::Instant;

use crate::abi::{decode_calldata, parse::hex_encode};
use crate::db::{dyn_table, monitor_repo, tx_repo, tx_repo::TxInput};
use crate::error::AppResult;
use crate::metrics;
use crate::rpc::fetch::MatchedTx;
use crate::watcher::model::Monitor;

/// Process all matched txs in a block for the monitors that cover it.
///
/// For each tx, find the monitor whose (address, selector) matches, decode the
/// calldata, and persist the tx metadata + params. Cursor advancement is done
/// by the caller after this returns.
pub async fn process_block(
    pool: &PgPool,
    chain_id: i64,
    chain_label: &str,
    block_number: i64,
    block_hash: &str,
    monitors: &[Monitor],
    txs: &[MatchedTx],
) -> AppResult<usize> {
    let mut decoded_count = 0usize;
    let mut conn = pool.acquire().await?;

    for tx in txs {
        let selector_bytes = tx.input.get(..4).unwrap_or(&[]);
        let selector_hex = format!("0x{}", hex_encode(selector_bytes));

        let Some(monitor) = monitors
            .iter()
            .find(|m| m.address == tx.to && m.selector == selector_bytes)
        else {
            continue;
        };

        let calldata = &tx.input[4.min(tx.input.len())..];

        let params = match decode_calldata(&monitor.input_types, calldata) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    chain = chain_label,
                    monitor = monitor.id,
                    tx = %tx.hash,
                    "decode error: {e}"
                );
                metrics::decode_error(chain_label, "decode");
                continue;
            }
        };

        let start = Instant::now();
        let tx_input = TxInput {
            tx_hash: &tx.hash.to_string(),
            chain_id,
            monitor_id: monitor.id,
            block_number,
            block_hash,
            from_addr: &tx.from.to_string(),
            to_addr: &tx.to.to_string(),
            value: uint_to_decimal(tx.value),
            gas_used: tx.gas_used.to_string(),
            gas_price: tx.gas_price.to_string(),
            status: 1,
            input_raw: tx.input.clone(),
            selector: &selector_hex,
        };

        if let Err(e) = tx_repo::insert_tx(&mut conn, &tx_input).await {
            tracing::warn!(chain = chain_label, tx = %tx.hash, "insert tx: {e}");
            metrics::decode_error(chain_label, "insert_tx");
            continue;
        }

        if let Err(e) = dyn_table::insert_params(
            &mut conn,
            monitor.id,
            &monitor.params,
            &tx.hash.to_string(),
            &params,
        )
        .await
        {
            tracing::warn!(chain = chain_label, tx = %tx.hash, "insert params: {e}");
            metrics::decode_error(chain_label, "insert_params");
            continue;
        }

        metrics::db_insert_seconds(chain_label).record(start.elapsed().as_secs_f64());
        metrics::txs_decoded(chain_label, monitor.id);
        decoded_count += 1;
    }

    Ok(decoded_count)
}

/// Advance a monitor's cursor, marking completed if it reached its end_block.
pub async fn advance_cursor(pool: &PgPool, monitor: &Monitor, block_number: i64) -> AppResult<()> {
    let completed = monitor.end_block.is_some_and(|end| block_number >= end);
    monitor_repo::set_cursor(pool, monitor.id, block_number, completed).await?;
    Ok(())
}

/// Convert a U256 to a decimal string (no scientific notation).
fn uint_to_decimal(v: U256) -> String {
    v.to_string()
}
