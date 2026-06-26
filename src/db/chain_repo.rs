use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::FromRow;
use sqlx::PgPool;

use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct ChainRow {
    pub id: i64,
    pub name: String,
    pub chain_id: i64,
    pub rpc_url: String,
    pub start_block: i64,
    pub poll_interval_ms: i32,
    pub batch_size: i32,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ChainInput {
    pub name: String,
    pub chain_id: i64,
    pub rpc_url: String,
    pub start_block: i64,
    pub poll_interval_ms: i32,
    pub batch_size: i32,
    pub enabled: bool,
}

pub async fn create(pool: &PgPool, input: &ChainInput) -> AppResult<ChainRow> {
    let row = sqlx::query_as::<_, ChainRow>(
        r#"INSERT INTO chains (name, chain_id, rpc_url, start_block, poll_interval_ms, batch_size, enabled)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           RETURNING *"#,
    )
    .bind(&input.name)
    .bind(input.chain_id)
    .bind(&input.rpc_url)
    .bind(input.start_block)
    .bind(input.poll_interval_ms)
    .bind(input.batch_size)
    .bind(input.enabled)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

pub async fn list(pool: &PgPool) -> AppResult<Vec<ChainRow>> {
    let rows = sqlx::query_as::<_, ChainRow>("SELECT * FROM chains ORDER BY id")
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn get(pool: &PgPool, id: i64) -> AppResult<ChainRow> {
    let row = sqlx::query_as::<_, ChainRow>("SELECT * FROM chains WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await?;
    Ok(row)
}

pub async fn get_by_chain_id(pool: &PgPool, chain_id: i64) -> AppResult<ChainRow> {
    let row = sqlx::query_as::<_, ChainRow>("SELECT * FROM chains WHERE chain_id = $1")
        .bind(chain_id)
        .fetch_one(pool)
        .await?;
    Ok(row)
}

pub async fn delete(pool: &PgPool, id: i64) -> AppResult<()> {
    let res = sqlx::query("DELETE FROM chains WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    if res.rows_affected() == 0 {
        return Err(AppError::NotFound(format!("chain {id}")));
    }
    Ok(())
}

pub async fn set_enabled(pool: &PgPool, id: i64, enabled: bool) -> AppResult<ChainRow> {
    let row = sqlx::query_as::<_, ChainRow>(
        "UPDATE chains SET enabled = $1, updated_at = NOW() WHERE id = $2 RETURNING *",
    )
    .bind(enabled)
    .bind(id)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

pub async fn list_enabled(pool: &PgPool) -> AppResult<Vec<ChainRow>> {
    let rows =
        sqlx::query_as::<_, ChainRow>("SELECT * FROM chains WHERE enabled = true ORDER BY id")
            .fetch_all(pool)
            .await?;
    Ok(rows)
}
