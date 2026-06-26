use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::db::chain_repo;
use crate::error::AppResult;
use crate::indexer::coordinator;
use crate::watcher::registry::Registry;

/// A running coordinator task and its cancellation token.
type TaskEntry = (JoinHandle<()>, CancellationToken);

/// Manages the lifetime of per-chain coordinator tasks.
///
/// A supervisor task periodically reconciles the set of enabled chains in the
/// DB against the running coordinators, spawning new ones and cancelling
/// removed/disabled ones.
pub struct Supervisor {
    pool: PgPool,
    registry: Registry,
    cache_cap: usize,
    /// chain_id -> (handle, cancel token)
    tasks: Arc<RwLock<HashMap<i64, TaskEntry>>>,
    cancel: CancellationToken,
}

impl Supervisor {
    pub fn new(
        pool: PgPool,
        registry: Registry,
        cache_cap: usize,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            pool,
            registry,
            cache_cap,
            tasks: Arc::new(RwLock::new(HashMap::new())),
            cancel,
        }
    }

    /// Run the supervisor loop until the cancellation token fires.
    pub async fn run(self) -> AppResult<()> {
        let interval = Duration::from_secs(5);
        loop {
            if self.cancel.is_cancelled() {
                break;
            }
            if let Err(e) = self.reconcile().await {
                tracing::warn!("supervisor reconcile: {e}");
            }
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = self.cancel.cancelled() => break,
            }
        }
        // Cancel all remaining coordinators and await them.
        let mut tasks = self.tasks.write().await;
        let drained: Vec<(JoinHandle<()>, CancellationToken)> =
            tasks.drain().map(|(_, v)| v).collect();
        for (_, token) in &drained {
            token.cancel();
        }
        for (handle, _) in drained {
            let _ = handle.await;
        }
        tracing::info!("supervisor stopped");
        Ok(())
    }

    /// Reconcile running coordinators with the set of enabled chains in the DB.
    async fn reconcile(&self) -> AppResult<()> {
        let enabled = chain_repo::list_enabled(&self.pool).await?;
        let mut tasks = self.tasks.write().await;

        // Spawn coordinators for newly-enabled chains.
        let mut seen = std::collections::HashSet::new();
        for chain in &enabled {
            seen.insert(chain.id);
            let entry = tasks.entry(chain.id);
            if let std::collections::hash_map::Entry::Occupied(_) = entry {
                continue;
            }
            let token = self.cancel.child_token();
            let handle = tokio::spawn({
                let chain = chain.clone();
                let pool = self.pool.clone();
                let registry = self.registry.clone();
                let cap = self.cache_cap;
                let token = token.clone();
                async move {
                    if let Err(e) = coordinator::run(chain, pool, registry, cap, token).await {
                        tracing::error!("coordinator: {e}");
                    }
                }
            });
            entry.or_insert((handle, token));
            tracing::info!(chain_id = chain.id, "spawned coordinator");
        }

        // Cancel coordinators for chains no longer enabled or removed.
        let to_remove: Vec<i64> = tasks
            .keys()
            .filter(|id| !seen.contains(id))
            .copied()
            .collect();
        for id in to_remove {
            if let Some((handle, token)) = tasks.remove(&id) {
                token.cancel();
                drop(handle); // let it finish in the background
                tracing::info!(chain_id = id, "cancelled coordinator");
            }
        }

        Ok(())
    }
}
