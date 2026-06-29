use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{FromRow, PgPool};
use sqlx::types::Json;

use crate::abi::{ParamSpec, parse_func_signature};
use crate::db::dyn_table;
use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct MonitorRow {
    pub id: i64,
    pub address: String,
    pub name: String,
    pub signature: String,
    pub selector: String,
    pub param_schema: Json<Vec<ParamSpec>>,
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
    pub address: String,
    pub name: Option<String>,
    pub signature: String,
    pub start_block: Option<i64>,
    pub end_block: Option<i64>,
}

pub async fn create(pool: &PgPool, input: &MonitorInput) -> AppResult<MonitorRow> {
    let spec = parse_func_signature(&input.signature)?;

    let name = input.name.clone().unwrap_or_else(|| {
        format!(
            "{}_{}",
            input.address.to_ascii_lowercase(),
            spec.selector
        )
    });

    let row = sqlx::query_as::<_, MonitorRow>(
        r#"INSERT INTO monitors
             (address, name, signature, selector,
              param_schema, start_block, end_block)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           RETURNING *"#,
    )
    .bind(&input.address.to_ascii_lowercase())
    .bind(&name)
    .bind(&input.signature)
    .bind(&spec.selector)
    .bind(Json(spec.params.clone()))
    .bind(input.start_block)
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

pub async fn get(pool: &PgPool, id: i64) -> AppResult<MonitorRow> {
    let row = sqlx::query_as::<_, MonitorRow>("SELECT * FROM monitors WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await?;
    Ok(row)
}

pub async fn delete(pool: &PgPool, id: i64) -> AppResult<()> {
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
