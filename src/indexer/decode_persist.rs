use alloy::hex;
use sqlx::PgPool;
use sqlx::types::BigDecimal;
use std::str::FromStr;

use crate::abi::decode_calldata;
use crate::db::{dyn_table, monitor_repo, tx_repo, tx_repo::TxInput};
use crate::error::AppResult;
use crate::rpc::fetch::MatchedTx;
use crate::watcher::model::Monitor;

/// Process all matched txs in a block for the monitors that cover it.
///
/// For each tx, find the monitor whose (address, selector) matches, decode the
/// calldata, and persist the tx metadata + params. All storage and cursor
/// updates for the block commit atomically.
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
    let mut db_tx = pool.begin().await?;

    for tx in txs {
        let selector_bytes = tx.input.get(..4).unwrap_or(&[]);
        let selector_hex = format!("0x{}", hex::encode(selector_bytes));

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
                continue;
            }
        };

        let tx_input = TxInput {
            tx_hash: &tx.hash.to_string(),
            chain_id,
            monitor_id: monitor.id,
            block_number,
            block_hash,
            from_addr: &tx.from.to_string(),
            to_addr: &tx.to.to_string(),
            value: uint_to_decimal(tx.value),
            gas_used: BigDecimal::from_str(&tx.gas_used.to_string()).expect("u64 fits BigDecimal"),
            gas_price: BigDecimal::from_str(&tx.gas_price.to_string())
                .expect("u128 fits BigDecimal"),
            status: 1,
            input_raw: tx.input.clone(),
            selector: &selector_hex,
        };

        tx_repo::insert_tx(&mut db_tx, &tx_input).await?;
        dyn_table::insert_params(
            &mut db_tx,
            monitor.id,
            &monitor.params,
            &tx.hash.to_string(),
            &params,
        )
        .await?;

        decoded_count += 1;
    }

    for monitor in monitors {
        let completed = monitor.end_block.is_some_and(|end| block_number >= end);
        monitor_repo::set_cursor(&mut db_tx, monitor.id, block_number, completed).await?;
    }

    db_tx.commit().await?;
    Ok(decoded_count)
}

/// Convert a U256 to a BigDecimal via its decimal string form (no scientific
/// notation), preserving full 256-bit precision for `NUMERIC` storage.
fn uint_to_decimal(v: alloy::primitives::U256) -> BigDecimal {
    BigDecimal::from_str(&v.to_string()).expect("U256 decimal fits BigDecimal")
}
