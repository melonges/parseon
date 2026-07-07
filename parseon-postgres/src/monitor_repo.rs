use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::{FromRow, PgConnection, PgPool};

use parseon_core::abi::{AbiParam, parse_abi_type};
use parseon_core::ports::{MonitorUpdate, NewMonitor};
use crate::dyn_table;
type AppResult<T> = anyhow::Result<T>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StoredParam {
    pub name: String,
    pub sol_type: String,
    #[serde(default)]
    pub indexed: bool,
}

impl StoredParam {
    pub fn to_abi(&self) -> Result<AbiParam, parseon_core::abi::AbiError> {
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

pub async fn create_prepared(pool: &PgPool, input: &NewMonitor) -> AppResult<MonitorRecord> {
    let params = input
        .param_schema
        .iter()
        .map(|param| StoredParam {
            name: param.name.clone(),
            sol_type: param.sol_type.clone(),
            indexed: param.indexed,
        })
        .collect::<Vec<_>>();
    let mut tx = pool.begin().await?;
    let chain_exists: Option<i64> =
        sqlx::query_scalar("SELECT chain_id FROM chains WHERE chain_id = $1 FOR KEY SHARE")
            .bind(input.chain.id)
            .fetch_optional(&mut *tx)
            .await?;
    anyhow::ensure!(chain_exists.is_some(), "chain {} not found", input.chain.id);
    let row = sqlx::query_as::<_, MonitorRecord>(
        r#"INSERT INTO monitors
             (chain_id, address, signature, kind, signature_hash, param_schema, start_block, end_block)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
           RETURNING *"#,
    )
    .bind(input.chain.id)
    .bind(&input.address)
    .bind(&input.signature)
    .bind(input.kind.as_str())
    .bind(&input.signature_hash)
    .bind(Json(params.clone()))
    .bind(input.start_block)
    .bind(input.end_block)
    .fetch_one(&mut *tx)
    .await?;
    dyn_table::create_result_table(&mut tx, row.id, &row.kind, &params).await?;
    tx.commit().await?;
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
    Ok(usize::try_from(count)?)
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
            .ok_or_else(|| anyhow::anyhow!("monitor {id} not found"))?;

    dyn_table::drop_result_table(&mut tx, current.id).await?;
    let res = sqlx::query("DELETE FROM monitors WHERE id = $1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    debug_assert_eq!(res.rows_affected(), 1);
    tx.commit().await?;
    Ok(())
}

pub async fn update_prepared(
    pool: &PgPool,
    id: i64,
    update: &MonitorUpdate,
) -> AppResult<MonitorRecord> {
    let mut tx = pool.begin().await?;
    let current = sqlx::query_as::<_, MonitorRecord>(
        "SELECT * FROM monitors WHERE id = $1 FOR UPDATE",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| anyhow::anyhow!("monitor {id} not found"))?;
    if update.reindex {
        dyn_table::truncate_result_table(&mut tx, current.id).await?;
    }
    let row = sqlx::query_as::<_, MonitorRecord>(
        r#"UPDATE monitors
             SET start_block = $1, end_block = $2, enabled = $3,
                 cursor = $4, completed = $5, updated_at = NOW()
           WHERE id = $6
           RETURNING *"#,
    )
    .bind(update.start_block)
    .bind(update.end_block)
    .bind(update.enabled)
    .bind(update.cursor)
    .bind(update.completed)
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

#[cfg(test)]
mod tests {
    use super::StoredParam;

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
