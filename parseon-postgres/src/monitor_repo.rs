use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::{FromRow, PgConnection, PgPool};

use crate::{dyn_table, pg_types};
use parseon_core::abi::{AbiParam, parse_abi_type};
use parseon_core::filter::FilterExpression;
use parseon_core::ports::{MonitorKind, NewMonitor};
use parseon_core::{BlockNumber, ChainId, MonitorId, Target};
type AppResult<T> = anyhow::Result<T>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredParam {
    pub name: String,
    pub sol_type: String,
    #[serde(default)]
    pub indexed: bool,
}

impl StoredParam {
    pub(crate) fn to_abi(&self) -> Result<AbiParam, parseon_core::abi::AbiError> {
        Ok(AbiParam::new(self.name.clone(), parse_abi_type(&self.sol_type)?)?
            .with_indexed(self.indexed))
    }
}

#[derive(Debug, Clone, FromRow)]
pub(crate) struct MonitorRecord {
    pub id: i64,
    pub chain_id: i64,
    pub address: Vec<u8>,
    pub kind: String,
    pub signature_hash: Vec<u8>,
    pub param_schema: Json<Vec<StoredParam>>,
    pub start_block: i64,
    pub end_block: Option<i64>,
    pub cursor: Option<i64>,
    pub completed: bool,
    pub enabled: bool,
    pub filter_ast: Option<Json<FilterExpression>>,
    pub filter_version: Option<i16>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub(crate) async fn create_prepared(pool: &PgPool, input: &NewMonitor) -> AppResult<MonitorRecord> {
    let (address, kind, signature_hash, abi_params) = match &input.target {
        Target::Call(target) => (
            target.address.as_slice(),
            MonitorKind::Call,
            target.selector.as_slice(),
            &target.inputs,
        ),
        Target::Event(target) => (
            target.address.as_slice(),
            MonitorKind::Event,
            target.topic0.as_slice(),
            &target.params,
        ),
    };
    let params = abi_params
        .iter()
        .map(|param| StoredParam {
            name: param.name.clone(),
            sol_type: param.sol_type(),
            indexed: param.indexed,
        })
        .collect::<Vec<_>>();
    let chain_id = pg_types::to_i64(input.chain.id, "chain id")?;
    let start_block = pg_types::to_i64(input.start_block, "start block")?;
    let end_block =
        input.end_block.map(|block| pg_types::to_i64(block, "end block")).transpose()?;
    let (filter_ast, filter_version) = input.filter.as_ref().map_or((None, None), |filter| {
        (Some(Json(filter.expression.clone())), Some(filter.version))
    });
    let mut tx = pool.begin().await?;
    let chain_exists: Option<i64> =
        sqlx::query_scalar("SELECT chain_id FROM chains WHERE chain_id = $1 FOR KEY SHARE")
            .bind(chain_id)
            .fetch_optional(&mut *tx)
            .await?;
    anyhow::ensure!(chain_exists.is_some(), "chain {} not found", input.chain.id);
    let row = sqlx::query_as::<_, MonitorRecord>(
        r#"INSERT INTO monitors
             (chain_id, address, kind, signature_hash, param_schema, start_block, end_block,
              filter_ast, filter_version)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
           RETURNING *"#,
    )
    .bind(chain_id)
    .bind(address)
    .bind(kind.as_str())
    .bind(signature_hash)
    .bind(Json(params.clone()))
    .bind(start_block)
    .bind(end_block)
    .bind(filter_ast)
    .bind(filter_version)
    .fetch_one(&mut *tx)
    .await?;
    dyn_table::create_result_table(&mut tx, row.id, &row.kind, &params).await?;
    tx.commit().await?;
    Ok(row)
}

pub(crate) async fn list(
    pool: &PgPool,
    chain_id: Option<ChainId>,
) -> AppResult<Vec<MonitorRecord>> {
    let chain_id = chain_id.map(|id| pg_types::to_i64(id, "chain id")).transpose()?;
    let rows = sqlx::query_as::<_, MonitorRecord>(
        "SELECT * FROM monitors WHERE ($1::BIGINT IS NULL OR chain_id = $1) ORDER BY id",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub(crate) async fn count(pool: &PgPool) -> AppResult<usize> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM monitors").fetch_one(pool).await?;
    Ok(usize::try_from(count)?)
}

pub(crate) async fn get(pool: &PgPool, id: MonitorId) -> AppResult<MonitorRecord> {
    let id = pg_types::to_monitor_id(id)?;
    let row = sqlx::query_as::<_, MonitorRecord>("SELECT * FROM monitors WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await?;
    Ok(row)
}

pub(crate) async fn delete(pool: &PgPool, id: MonitorId) -> AppResult<()> {
    let id = pg_types::to_monitor_id(id)?;
    let mut tx = pool.begin().await?;
    let current =
        sqlx::query_as::<_, MonitorRecord>("SELECT * FROM monitors WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| anyhow::anyhow!("monitor {id} not found"))?;

    dyn_table::drop_result_table(&mut tx, current.id).await?;
    let res = sqlx::query("DELETE FROM monitors WHERE id = $1").bind(id).execute(&mut *tx).await?;
    debug_assert_eq!(res.rows_affected(), 1);
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn set_enabled(
    pool: &PgPool,
    id: MonitorId,
    enabled: bool,
) -> AppResult<MonitorRecord> {
    let id = pg_types::to_monitor_id(id)?;
    let row = sqlx::query_as::<_, MonitorRecord>(
        r#"UPDATE monitors
             SET enabled = $1, updated_at = NOW()
           WHERE id = $2
           RETURNING *"#,
    )
    .bind(enabled)
    .bind(id)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

pub(crate) async fn set_cursor(
    conn: &mut PgConnection,
    id: MonitorId,
    chain_id: ChainId,
    cursor: BlockNumber,
    completed: bool,
) -> AppResult<()> {
    let id = pg_types::to_monitor_id(id)?;
    let chain_id = pg_types::to_i64(chain_id, "chain id")?;
    let cursor = pg_types::to_i64(cursor, "cursor")?;
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
        let param =
            StoredParam { name: "value".into(), sol_type: "uint256".into(), indexed: false };
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
