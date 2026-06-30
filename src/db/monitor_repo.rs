use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::types::Json;
use sqlx::{FromRow, PgConnection, PgPool};

use crate::abi::{ParamSpec, parse_func_signature};
use crate::db::dyn_table;
use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, FromRow, Serialize, utoipa::ToSchema)]
pub struct MonitorRow {
    pub id: i64,
    pub address: String,
    pub signature: String,
    pub selector: String,
    #[schema(value_type = Vec<ParamSpec>)]
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
    pub signature: String,
    pub start_block: i64,
    pub end_block: Option<i64>,
}

pub async fn create(pool: &PgPool, input: &MonitorInput) -> AppResult<MonitorRow> {
    validate_range(input.start_block, input.end_block)?;
    let normalized_address = validate_address(&input.address)?;
    let spec = parse_func_signature(&input.signature)?;

    let mut tx = pool.begin().await?;

    let row = sqlx::query_as::<_, MonitorRow>(
        r#"INSERT INTO monitors
             (address, signature, selector,
              param_schema, start_block, end_block)
           VALUES ($1, $2, $3, $4, $5, $6)
           RETURNING *"#,
    )
    .bind(&normalized_address)
    .bind(&input.signature)
    .bind(&spec.selector)
    .bind(Json(spec.params.clone()))
    .bind(input.start_block)
    .bind(input.end_block)
    .fetch_one(&mut *tx)
    .await?;

    dyn_table::create_result_table(&mut tx, &row.address, &row.selector, &spec.params).await?;
    tx.commit().await?;
    tracing::info!(
        monitor_id = row.id,
        table = dyn_table::result_table_name(&row.address, &row.selector)?,
        "created result table for monitor"
    );

    Ok(row)
}

pub async fn list(pool: &PgPool) -> AppResult<Vec<MonitorRow>> {
    let rows = sqlx::query_as::<_, MonitorRow>("SELECT * FROM monitors ORDER BY id")
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn count(pool: &PgPool) -> AppResult<usize> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM monitors")
        .fetch_one(pool)
        .await?;
    usize::try_from(count).map_err(|e| AppError::Internal(e.into()))
}

pub async fn get(pool: &PgPool, id: i64) -> AppResult<MonitorRow> {
    let row = sqlx::query_as::<_, MonitorRow>("SELECT * FROM monitors WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await?;
    Ok(row)
}

pub async fn delete(pool: &PgPool, id: i64) -> AppResult<()> {
    let mut tx = pool.begin().await?;
    let current =
        sqlx::query_as::<_, MonitorRow>("SELECT * FROM monitors WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("monitor {id}")))?;

    dyn_table::drop_result_table(&mut tx, &current.address, &current.selector).await?;
    let res = sqlx::query("DELETE FROM monitors WHERE id = $1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    debug_assert_eq!(res.rows_affected(), 1);
    tx.commit().await?;
    Ok(())
}

pub async fn update(
    pool: &PgPool,
    id: i64,
    start_block: Option<i64>,
    end_block: Option<Option<i64>>,
    enabled: Option<bool>,
) -> AppResult<MonitorRow> {
    let mut tx = pool.begin().await?;
    let current =
        sqlx::query_as::<_, MonitorRow>("SELECT * FROM monitors WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("monitor {id}")))?;

    let new_start = start_block.unwrap_or(current.start_block);
    let new_end = end_block.unwrap_or(current.end_block);
    validate_range(new_start, new_end)?;

    let requires_reindex = new_start != current.start_block
        || new_end.is_some_and(|end| current.cursor.is_some_and(|cursor| cursor > end));

    let cursor = if requires_reindex {
        dyn_table::truncate_result_table(&mut tx, &current.address, &current.selector).await?;
        Some(new_start - 1)
    } else {
        current.cursor
    };

    if requires_reindex {
        tracing::info!(
            monitor_id = id,
            table = dyn_table::result_table_name(&current.address, &current.selector)?,
            cursor,
            "truncated result table and reset monitor cursor"
        );
    }

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
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(row)
}

pub async fn set_cursor(
    conn: &mut PgConnection,
    id: i64,
    cursor: i64,
    completed: bool,
) -> AppResult<()> {
    sqlx::query(
        "UPDATE monitors SET cursor = $1, completed = $2, updated_at = NOW() WHERE id = $3",
    )
    .bind(cursor)
    .bind(completed)
    .bind(id)
    .execute(conn)
    .await?;
    Ok(())
}

fn validate_range(start_block: i64, end_block: Option<i64>) -> AppResult<()> {
    if start_block < 0 {
        return Err(AppError::BadRequest(
            "start_block must be non-negative".to_string(),
        ));
    }
    if end_block.is_some_and(|end| end < start_block) {
        return Err(AppError::BadRequest(
            "end_block must be greater than or equal to start_block".to_string(),
        ));
    }
    Ok(())
}

fn validate_address(value: &str) -> AppResult<String> {
    let address: Address = value
        .parse()
        .map_err(|e| AppError::BadRequest(format!("invalid address: {e}")))?;
    Ok(address.to_string().to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::{validate_address, validate_range};

    #[test]
    fn validates_and_normalizes_addresses() {
        assert_eq!(
            validate_address("0x000000000000000000000000000000000000000A").unwrap(),
            "0x000000000000000000000000000000000000000a"
        );
        assert!(validate_address("not-an-address").is_err());
    }

    #[test]
    fn validates_monitor_ranges() {
        assert!(validate_range(0, None).is_ok());
        assert!(validate_range(10, Some(10)).is_ok());
        assert!(validate_range(-1, None).is_err());
        assert!(validate_range(10, Some(9)).is_err());
    }
}
