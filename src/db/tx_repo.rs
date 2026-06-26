use sqlx::PgConnection;

/// Input for persisting a matched transaction's metadata.
pub struct TxInput<'a> {
    pub tx_hash: &'a str,
    pub chain_id: i64,
    pub monitor_id: i64,
    pub block_number: i64,
    pub block_hash: &'a str,
    pub from_addr: &'a str,
    pub to_addr: &'a str,
    pub value: String,
    pub gas_used: String,
    pub gas_price: String,
    pub status: i16,
    pub input_raw: Vec<u8>,
    pub selector: &'a str,
}

/// Insert a transaction row. Idempotent via `ON CONFLICT (tx_hash) DO NOTHING`.
pub async fn insert_tx(conn: &mut PgConnection, tx: &TxInput<'_>) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO transactions
             (tx_hash, chain_id, monitor_id, block_number, block_hash,
              from_addr, to_addr, value, gas_used, gas_price, status,
              input_raw, selector)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8::numeric, $9::numeric,
                   $10::numeric, $11, $12, $13)
           ON CONFLICT (tx_hash) DO NOTHING"#,
    )
    .bind(tx.tx_hash)
    .bind(tx.chain_id)
    .bind(tx.monitor_id)
    .bind(tx.block_number)
    .bind(tx.block_hash)
    .bind(tx.from_addr)
    .bind(tx.to_addr)
    .bind(&tx.value)
    .bind(&tx.gas_used)
    .bind(&tx.gas_price)
    .bind(tx.status)
    .bind(&tx.input_raw)
    .bind(tx.selector)
    .execute(conn)
    .await?;
    Ok(())
}
