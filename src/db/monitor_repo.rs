use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::{FromRow, PgConnection, PgPool};

use crate::core::abi::{AbiParam, TargetSpec, parse_abi_type, parse_target_signature};
use crate::db::dyn_table;
use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StoredParam {
    pub name: String,
    pub sol_type: String,
    #[serde(default)]
    pub indexed: bool,
}

impl StoredParam {
    pub fn from_abi(param: &AbiParam) -> Self {
        Self {
            name: param.name.clone(),
            sol_type: param.sol_type(),
            indexed: param.indexed,
        }
    }

    pub fn to_abi(&self) -> Result<AbiParam, crate::core::abi::AbiError> {
        Ok(
            AbiParam::new(self.name.clone(), parse_abi_type(&self.sol_type)?)?
                .with_indexed(self.indexed),
        )
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct MonitorRecord {
    pub id: i64,
    pub chain_id: i64,
    pub address: String,
    pub signature: String,
    pub kind: String,
    pub signature_hash: String,
    pub param_schema: Json<Vec<StoredParam>>,
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
    pub start_block: i64,
    pub end_block: Option<i64>,
}

pub async fn create(pool: &PgPool, input: &MonitorInput) -> AppResult<MonitorRecord> {
    validate_range(input.start_block, input.end_block)?;
    let normalized_address = validate_address(&input.address)?;
    let spec = parse_target_signature(&input.signature)?;
    let (kind, hash, abi_params) = match &spec {
        TargetSpec::Call(spec) => (
            "call",
            format!("0x{}", alloy::hex::encode(spec.selector)),
            &spec.params,
        ),
        TargetSpec::Event(spec) => ("event", spec.topic0.to_string(), &spec.params),
    };
    let params = abi_params
        .iter()
        .map(StoredParam::from_abi)
        .collect::<Vec<_>>();
    let mut tx = pool.begin().await?;

    let chain_exists: Option<i64> =
        sqlx::query_scalar("SELECT chain_id FROM chains WHERE chain_id = $1 FOR KEY SHARE")
            .bind(input.chain_id)
            .fetch_optional(&mut *tx)
            .await?;
    if chain_exists.is_none() {
        return Err(AppError::NotFound(format!("chain {}", input.chain_id)));
    }

    let row = sqlx::query_as::<_, MonitorRecord>(
        r#"INSERT INTO monitors
             (chain_id, address, signature, kind, signature_hash, param_schema, start_block, end_block)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
           RETURNING *"#,
    )
    .bind(input.chain_id)
    .bind(&normalized_address)
    .bind(&input.signature)
    .bind(kind)
    .bind(&hash)
    .bind(Json(params.clone()))
    .bind(input.start_block)
    .bind(input.end_block)
    .fetch_one(&mut *tx)
    .await?;

    dyn_table::create_result_table(&mut tx, row.id, &row.kind, &params).await?;
    tx.commit().await?;
    tracing::info!(
        monitor_id = row.id,
        table = dyn_table::result_table_name(row.id)?,
        "created result table for monitor"
    );

    Ok(row)
}

pub async fn list(pool: &PgPool, chain_id: Option<i64>) -> AppResult<Vec<MonitorRecord>> {
    let rows = sqlx::query_as::<_, MonitorRecord>(
        "SELECT * FROM monitors WHERE ($1::BIGINT IS NULL OR chain_id = $1) ORDER BY id",
    )
    .bind(chain_id)
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

pub async fn get(pool: &PgPool, id: i64) -> AppResult<MonitorRecord> {
    let row = sqlx::query_as::<_, MonitorRecord>("SELECT * FROM monitors WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await?;
    Ok(row)
}

pub async fn delete(pool: &PgPool, id: i64) -> AppResult<()> {
    let mut tx = pool.begin().await?;
    let current =
        sqlx::query_as::<_, MonitorRecord>("SELECT * FROM monitors WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("monitor {id}")))?;

    dyn_table::drop_result_table(&mut tx, current.id).await?;
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
) -> AppResult<MonitorRecord> {
    let mut tx = pool.begin().await?;
    let current =
        sqlx::query_as::<_, MonitorRecord>("SELECT * FROM monitors WHERE id = $1 FOR UPDATE")
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
        dyn_table::truncate_result_table(&mut tx, current.id).await?;
        Some(new_start - 1)
    } else {
        current.cursor
    };

    if requires_reindex {
        tracing::info!(
            monitor_id = id,
            table = dyn_table::result_table_name(current.id)?,
            cursor,
            "truncated result table and reset monitor cursor"
        );
    }

    let completed = match new_end {
        Some(e) => cursor.is_some_and(|c| c >= e),
        None => false,
    };

    let row = sqlx::query_as::<_, MonitorRecord>(
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
    chain_id: i64,
    cursor: i64,
    completed: bool,
) -> AppResult<()> {
    sqlx::query(
        "UPDATE monitors SET cursor = $1, completed = $2, updated_at = NOW() WHERE id = $3 AND chain_id = $4",
    )
    .bind(cursor)
    .bind(completed)
    .bind(id)
    .bind(chain_id)
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
    use super::{StoredParam, validate_address, validate_range};

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

    #[test]
    fn persists_only_semantic_abi_fields() {
        let param = StoredParam {
            name: "value".into(),
            sol_type: "uint256".into(),
            indexed: false,
        };
        assert_eq!(
            serde_json::to_value(&param).unwrap(),
            serde_json::json!({"name": "value", "sol_type": "uint256", "indexed": false})
        );
        assert_eq!(param.to_abi().unwrap().sol_type(), "uint256");
        assert!(
            serde_json::from_value::<StoredParam>(serde_json::json!({
                "name": "value",
                "sol_type": "uint256",
                "sql_kind": "numeric"
            }))
            .is_err()
        );
    }
}
