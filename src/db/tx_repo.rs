use sqlx::types::BigDecimal;
use sqlx::PgConnection;

/// Fields required to insert a row into the `transactions` table.
///
/// Integer-ish numeric fields use `BigDecimal` (unbounded) so any `U256`
/// / `u128` / `u64` value binds natively to the column's `NUMERIC` type.
pub struct TxInput<'a> {
    pub tx_hash: &'a str,
    pub chain_id: i64,
    pub monitor_id: i64,
    pub block_number: i64,
    pub block_hash: &'a str,
    pub from_addr: &'a str,
    pub to_addr: &'a str,
    pub value: BigDecimal,
    pub gas_used: BigDecimal,
    pub gas_price: BigDecimal,
    pub status: i16,
    pub input_raw: Vec<u8>,
    pub selector: &'a str,
}

/// Insert a decoded transaction row, idempotent on `tx_hash`.
pub async fn insert_tx(
    conn: &mut PgConnection,
    t: &TxInput<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO transactions
             (tx_hash, chain_id, monitor_id, block_number, block_hash,
              from_addr, to_addr, value, gas_used, gas_price,
              status, input_raw, selector)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
           ON CONFLICT (tx_hash) DO NOTHING"#,
    )
    .bind(t.tx_hash)
    .bind(t.chain_id)
    .bind(t.monitor_id)
    .bind(t.block_number)
    .bind(t.block_hash)
    .bind(t.from_addr)
    .bind(t.to_addr)
    .bind(&t.value)
    .bind(&t.gas_used)
    .bind(&t.gas_price)
    .bind(t.status)
    .bind(&t.input_raw)
    .bind(t.selector)
    .execute(conn)
    .await?;
    Ok(())
}