use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};

use parseon_core::{Chain, Url};
use parseon_core::ports::RegisteredChain;
use crate::dyn_table;
type AppResult<T> = anyhow::Result<T>;

#[derive(Clone, FromRow)]
pub struct ChainRecord {
    pub chain_id: i64,
    pub rpc_url: String,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ChainRecord {
    pub fn rpc_url(&self) -> anyhow::Result<Url> {
        Ok(self.rpc_url.parse()?)
    }

    pub fn registered(&self) -> anyhow::Result<RegisteredChain> {
        Ok(RegisteredChain {
            chain: Chain::new(self.chain_id)?,
            rpc_url: self.rpc_url()?,
            enabled: self.enabled,
        })
    }
}

pub async fn create(
    pool: &PgPool,
    chain: Chain,
    rpc_url: &Url,
    enabled: bool,
) -> AppResult<ChainRecord> {
    Ok(sqlx::query_as::<_, ChainRecord>(
        r#"INSERT INTO chains (chain_id, rpc_url, enabled)
           VALUES ($1, $2, $3)
           RETURNING *"#,
    )
    .bind(chain.id)
    .bind(rpc_url.as_str())
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
        .ok_or_else(|| anyhow::anyhow!("chain {chain_id} not found"))
}

pub async fn update(
    pool: &PgPool,
    chain_id: i64,
    rpc_url: Option<&Url>,
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
    .bind(rpc_url.map(Url::as_str))
    .bind(enabled)
    .bind(chain_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| anyhow::anyhow!("chain {chain_id} not found"))
}

pub async fn delete(pool: &PgPool, chain_id: i64) -> AppResult<()> {
    let mut tx = pool.begin().await?;
    let exists: Option<i64> =
        sqlx::query_scalar("SELECT chain_id FROM chains WHERE chain_id = $1 FOR UPDATE")
            .bind(chain_id)
            .fetch_optional(&mut *tx)
            .await?;
    if exists.is_none() {
        anyhow::bail!("chain {chain_id} not found");
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
