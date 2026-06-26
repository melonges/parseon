use std::collections::HashMap;
use std::sync::Arc;

use alloy::primitives::Address;
use tokio::sync::RwLock;

use crate::db::monitor_repo::{self, MonitorRow};
use crate::error::AppResult;
use crate::watcher::model::Monitor;

/// In-memory registry of monitors, keyed by chain id, then by (address, selector).
///
/// The coordinator reads a snapshot each tick; API mutations trigger a reload.
#[derive(Clone)]
pub struct Registry {
    inner: Arc<RwLock<HashMap<i64, Vec<Monitor>>>>,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Reload the entire registry from the database.
    pub async fn reload(&self, pool: &sqlx::PgPool) -> AppResult<()> {
        let rows = monitor_repo::list(pool).await?;
        let mut map: HashMap<i64, Vec<Monitor>> = HashMap::new();
        for row in rows {
            if let Ok(m) = row_to_monitor(&row) {
                map.entry(m.chain_id).or_default().push(m);
            }
        }
        let mut guard = self.inner.write().await;
        *guard = map;
        tracing::info!(
            monitors = guard.values().map(|v| v.len()).sum::<usize>(),
            "registry reloaded"
        );
        Ok(())
    }

    /// Snapshot of monitors for a chain.
    pub async fn for_chain(&self, chain_id: i64) -> Vec<Monitor> {
        self.inner
            .read()
            .await
            .get(&chain_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Count of active monitors for a chain (enabled, not completed).
    #[allow(dead_code)]
    pub async fn active_count(&self, chain_id: i64) -> usize {
        self.inner
            .read()
            .await
            .get(&chain_id)
            .map(|v| v.iter().filter(|m| m.enabled && !m.completed).count())
            .unwrap_or(0)
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
        chain_id: row.chain_id,
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
