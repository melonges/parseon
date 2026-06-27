use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{FromRow, PgPool};

use crate::abi::parse_signature;
use crate::db::dyn_table;
use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct MonitorRow {
    pub id: i64,
    pub chain_id: i64,
    pub address: String,
    pub name: String,
    pub signature: String,
    pub selector: String,
    pub input_types: String,
    pub param_schema: serde_json::Value,
    pub start_block: i64,
    pub end_block: Option<i64>,
    pub cursor: Option<i64>,
    pub completed: bool,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct MonitorInput {
    pub chain_id: i64,
    pub address: String,
    pub signature: String,
    pub start_block: Option<i64>,
    pub end_block: Option<i64>,
}

/// Create a monitor: parse the signature, insert the row, then create its
/// dedicated params table.
pub async fn create(pool: &PgPool, input: &MonitorInput) -> AppResult<MonitorRow> {
    let spec = parse_signature(&input.signature)?;

    // Resolve start_block: explicit > chain.start_block (validated by caller).
    let start_block = input.start_block.unwrap_or(0);

    let param_schema = serde_json::to_value(&spec.params).map_err(anyhow::Error::from)?;

    let row = sqlx::query_as::<_, MonitorRow>(
        r#"INSERT INTO monitors
             (chain_id, address, name, signature, selector, input_types,
              param_schema, start_block, end_block)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
           RETURNING *"#,
    )
    .bind(input.chain_id)
    .bind(&input.address.to_ascii_lowercase())
    .bind(&spec.name)
    .bind(&spec.canonical_signature)
    .bind(&spec.selector)
    .bind(&spec.input_types)
    .bind(&param_schema)
    .bind(start_block)
    .bind(input.end_block)
    .fetch_one(pool)
    .await?;

    dyn_table::create_params_table(pool, row.id, &spec.params).await?;
    tracing::info!(
        monitor_id = row.id,
        table = dyn_table::params_table_name(row.id),
        "created params table for monitor"
    );

    Ok(row)
}

pub async fn list(pool: &PgPool) -> AppResult<Vec<MonitorRow>> {
    let rows = sqlx::query_as::<_, MonitorRow>("SELECT * FROM monitors ORDER BY id")
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

#[allow(dead_code)]
pub async fn list_for_chain(pool: &PgPool, chain_id: i64) -> AppResult<Vec<MonitorRow>> {
    let rows =
        sqlx::query_as::<_, MonitorRow>("SELECT * FROM monitors WHERE chain_id = $1 ORDER BY id")
            .bind(chain_id)
            .fetch_all(pool)
            .await?;
    Ok(rows)
}

pub async fn get(pool: &PgPool, id: i64) -> AppResult<MonitorRow> {
    let row = sqlx::query_as::<_, MonitorRow>("SELECT * FROM monitors WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await?;
    Ok(row)
}

pub async fn delete(pool: &PgPool, id: i64) -> AppResult<()> {
    // Drop the params table first, then the row (cascades to transactions).
    dyn_table::drop_params_table(pool, id).await?;
    let res = sqlx::query("DELETE FROM monitors WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    if res.rows_affected() == 0 {
        return Err(AppError::NotFound(format!("monitor {id}")));
    }
    Ok(())
}

/// Live-update the range/enabled state of a monitor.
///
/// - `start_block` lowered below the current cursor resets the cursor so the
///   gap is re-indexed.
/// - `end_block` extended beyond the cursor clears `completed`.
/// - `end_block` shrunk below the cursor sets `completed = true`.
pub async fn update(
    pool: &PgPool,
    id: i64,
    start_block: Option<i64>,
    end_block: Option<Option<i64>>,
    enabled: Option<bool>,
) -> AppResult<MonitorRow> {
    let current = get(pool, id).await?;

    let mut cursor = current.cursor;
    if let Some(new_start) = start_block
        && new_start < current.start_block
    {
        // Widening backwards: reset cursor to re-index the gap.
        cursor = Some(new_start - 1);
    }

    let new_end = end_block.unwrap_or(current.end_block);
    let completed = match new_end {
        Some(e) => cursor.is_some_and(|c| c >= e),
        None => false,
    };

    let row = sqlx::query_as::<_, MonitorRow>(
        r#"UPDATE monitors
             SET start_block = COALESCE($1, start_block),
                 end_block   = $2,
                 enabled     = COALESCE($3, enabled),
                 cursor      = $4,
                 completed   = $5,
                 updated_at  = NOW()
           WHERE id = $6
           RETURNING *"#,
    )
    .bind(start_block)
    .bind(new_end)
    .bind(enabled)
    .bind(cursor)
    .bind(completed)
    .bind(id)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

/// Persist the cursor position for a monitor.
pub async fn set_cursor(pool: &PgPool, id: i64, cursor: i64, completed: bool) -> AppResult<()> {
    sqlx::query(
        "UPDATE monitors SET cursor = $1, completed = $2, updated_at = NOW() WHERE id = $3",
    )
    .bind(cursor)
    .bind(completed)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}
