use std::sync::Arc;

use alloy::primitives::Address;
use tokio::sync::RwLock;

use crate::db::monitor_repo::{self, MonitorRow};
use crate::error::AppResult;
use crate::watcher::model::Monitor;

/// In-memory registry of monitors for the single indexed chain.
///
/// The coordinator reads a snapshot each tick; API mutations trigger a reload.
#[derive(Clone)]
pub struct Registry {
    inner: Arc<RwLock<Vec<Monitor>>>,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn reload(&self, pool: &sqlx::PgPool) -> AppResult<()> {
        let rows = monitor_repo::list(pool).await?;
        let monitors: Vec<Monitor> = rows
            .into_iter()
            .filter_map(|row| row_to_monitor(&row).ok())
            .collect();
        let mut guard = self.inner.write().await;
        *guard = monitors;
        tracing::info!(monitors = guard.len(), "registry reloaded");
        Ok(())
    }

    pub async fn get_all(&self) -> Vec<Monitor> {
        self.inner.read().await.clone()
    }
}

fn row_to_monitor(row: &MonitorRow) -> Result<Monitor, anyhow::Error> {
    let address: Address = row
        .address
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid monitor address {}: {e}", row.address))?;
    let selector = Monitor::parse_selector(&row.selector);

    let params: Vec<crate::abi::ParamSpec> = serde_json::from_value(row.param_schema.clone())
        .map_err(|e| anyhow::anyhow!("invalid param_schema for monitor {}: {e}", row.id))?;

    Ok(Monitor {
        id: row.id,
        address,
        selector,
        name: row.name.clone(),
        canonical_signature: row.signature.clone(),
        input_types: row.input_types.clone(),
        params,
        start_block: row.start_block,
        end_block: row.end_block,
        cursor: row.cursor,
        completed: row.completed,
        enabled: row.enabled,
    })
}