use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};

use crate::core::Chain;
use crate::core::ports::RegisteredChain;
use crate::db::dyn_table;
use crate::error::{AppError, AppResult};

#[derive(Clone, FromRow)]
pub struct ChainRecord {
    pub chain_id: i64,
    pub rpc_url: String,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ChainRecord {
    pub fn registered(&self) -> anyhow::Result<RegisteredChain> {
        Ok(RegisteredChain {
            chain: Chain::new(self.chain_id)?,
            rpc_url: self.rpc_url.clone(),
            enabled: self.enabled,
        })
    }
}

pub async fn create(
    pool: &PgPool,
    chain: Chain,
    rpc_url: &str,
    enabled: bool,
) -> AppResult<ChainRecord> {
    Ok(sqlx::query_as::<_, ChainRecord>(
        r#"INSERT INTO chains (chain_id, rpc_url, enabled)
           VALUES ($1, $2, $3)
           RETURNING *"#,
    )
    .bind(chain.id)
    .bind(rpc_url)
    .bind(enabled)
    .fetch_one(pool)
    .await?)
}

pub async fn list(pool: &PgPool) -> AppResult<Vec<ChainRecord>> {
    Ok(
        sqlx::query_as::<_, ChainRecord>("SELECT * FROM chains ORDER BY chain_id")
            .fetch_all(pool)
            .await?,
    )
}

pub async fn get(pool: &PgPool, chain_id: i64) -> AppResult<ChainRecord> {
    sqlx::query_as::<_, ChainRecord>("SELECT * FROM chains WHERE chain_id = $1")
        .bind(chain_id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("chain {chain_id}")))
}

pub async fn update(
    pool: &PgPool,
    chain_id: i64,
    rpc_url: Option<&str>,
    enabled: Option<bool>,
) -> AppResult<ChainRecord> {
    sqlx::query_as::<_, ChainRecord>(
        r#"UPDATE chains
           SET rpc_url = COALESCE($1, rpc_url),
               enabled = COALESCE($2, enabled),
               updated_at = NOW()
           WHERE chain_id = $3
           RETURNING *"#,
    )
    .bind(rpc_url)
    .bind(enabled)
    .bind(chain_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("chain {chain_id}")))
}

pub async fn delete(pool: &PgPool, chain_id: i64) -> AppResult<()> {
    let mut tx = pool.begin().await?;
    let exists: Option<i64> =
        sqlx::query_scalar("SELECT chain_id FROM chains WHERE chain_id = $1 FOR UPDATE")
            .bind(chain_id)
            .fetch_optional(&mut *tx)
            .await?;
    if exists.is_none() {
        return Err(AppError::NotFound(format!("chain {chain_id}")));
    }

    let monitor_ids: Vec<i64> =
        sqlx::query_scalar("SELECT id FROM monitors WHERE chain_id = $1 ORDER BY id FOR UPDATE")
            .bind(chain_id)
            .fetch_all(&mut *tx)
            .await?;
    for monitor_id in monitor_ids {
        dyn_table::drop_result_table(&mut tx, monitor_id).await?;
    }
    sqlx::query("DELETE FROM chains WHERE chain_id = $1")
        .bind(chain_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
