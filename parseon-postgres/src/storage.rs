use async_trait::async_trait;
use sqlx::PgPool;
use std::collections::HashMap;

use parseon_core::commands::ResultQuery;
use parseon_core::filter::{Filter, FilterDefinition};
use parseon_core::monitor::Monitor;
use parseon_core::ports::{
    BlockCommit, ChainRecord as CoreChainRecord, ChainRepository, ChainUpdate, IndexStorage,
    MonitorRecord as CoreMonitorRecord, MonitorRepository, NewChain, NewMonitor, RegisteredChain,
    ResultRecord as CoreResultRecord, ResultRepository,
};
use parseon_core::{CallTarget, Chain, Cursor, DecodedResult, EventTarget, MonitorId, Target};

use super::dyn_table::{CallResultInput, EventResultInput, ResultRecord, SearchParams};
use super::{chain_repo, dyn_table, monitor_repo, pg_types};

#[derive(Clone)]
pub struct PostgresStorage {
    pool: PgPool,
}

impl PostgresStorage {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    fn target(row: &monitor_repo::MonitorRecord) -> anyhow::Result<Target> {
        let address = pg_types::address(&row.address)?;
        let params = row
            .param_schema
            .0
            .iter()
            .map(monitor_repo::StoredParam::to_abi)
            .collect::<Result<Vec<_>, _>>()?;
        match row.kind.as_str() {
            "call" => Ok(Target::Call(CallTarget {
                address,
                selector: pg_types::selector(&row.signature_hash)?,
                inputs: params,
            })),
            "event" => Ok(Target::Event(EventTarget {
                address,
                topic0: pg_types::b256(&row.signature_hash, "event topic0")?,
                params,
            })),
            kind => anyhow::bail!("invalid monitor kind {kind}"),
        }
    }

    fn to_monitor(row: &monitor_repo::MonitorRecord) -> anyhow::Result<Monitor> {
        let target = Self::target(row)?;
        let filter = match (&row.filter_ast, row.filter_version) {
            (None, None) => Filter::All,
            (Some(filter), Some(version)) => {
                FilterDefinition { version, expression: filter.0.clone() }.compile(&target)?
            }
            _ => anyhow::bail!("monitor {} has incomplete filter state", row.id),
        };
        Ok(Monitor {
            id: pg_types::from_monitor_id(row.id)?,
            chain: Chain::new(pg_types::from_i64(row.chain_id, "chain id")?),
            target,
            start_block: pg_types::from_i64(row.start_block, "start block")?,
            end_block: row
                .end_block
                .map(|value| pg_types::from_i64(value, "end block"))
                .transpose()?,
            cursor: Cursor(
                row.cursor.map(|value| pg_types::from_i64(value, "cursor")).transpose()?,
            ),
            completed: row.completed,
            enabled: row.enabled,
            filter,
        })
    }

    fn chain_record(row: chain_repo::ChainRecord) -> anyhow::Result<CoreChainRecord> {
        let rpc_url = row.rpc_url()?;
        Ok(CoreChainRecord {
            chain: Chain::new(pg_types::from_i64(row.chain_id, "chain id")?),
            rpc_url,
            enabled: row.enabled,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }

    fn monitor_record(row: monitor_repo::MonitorRecord) -> anyhow::Result<CoreMonitorRecord> {
        let filter = match (row.filter_ast.as_ref(), row.filter_version) {
            (None, None) => None,
            (Some(filter), Some(version)) => {
                Some(FilterDefinition { version, expression: filter.0.clone() })
            }
            _ => anyhow::bail!("monitor {} has incomplete filter state", row.id),
        };
        Ok(CoreMonitorRecord {
            id: pg_types::from_monitor_id(row.id)?,
            chain: Chain::new(pg_types::from_i64(row.chain_id, "chain id")?),
            target: Self::target(&row)?,
            start_block: pg_types::from_i64(row.start_block, "start block")?,
            end_block: row
                .end_block
                .map(|value| pg_types::from_i64(value, "end block"))
                .transpose()?,
            cursor: row.cursor.map(|value| pg_types::from_i64(value, "cursor")).transpose()?,
            completed: row.completed,
            enabled: row.enabled,
            filter,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[async_trait]
impl IndexStorage for PostgresStorage {
    async fn load_monitors(&self, chain: Chain) -> anyhow::Result<Vec<Monitor>> {
        monitor_repo::list(&self.pool, Some(chain.id)).await?.iter().map(Self::to_monitor).collect()
    }

    async fn commit_block(&self, commit: &BlockCommit) -> anyhow::Result<()> {
        anyhow::ensure!(
            commit.monitors.iter().all(|monitor| monitor.chain == commit.chain),
            "cross-chain monitor set rejected for chain {}",
            commit.chain.id
        );
        let mut tx = self.pool.begin().await?;
        let mut monitor_ids = commit
            .monitors
            .iter()
            .map(|monitor| pg_types::to_monitor_id(monitor.id))
            .collect::<Result<Vec<_>, _>>()?;
        monitor_ids.sort_unstable();
        monitor_ids.dedup();

        let block_number = pg_types::to_i64(commit.block_number, "block number")?;
        let chain_id = pg_types::to_i64(commit.chain.id, "chain id")?;
        let rows = sqlx::query_as::<_, monitor_repo::MonitorRecord>(
            r#"WITH locked AS MATERIALIZED (
                 SELECT id FROM monitors
                 WHERE id = ANY($2) AND chain_id = $3
                 ORDER BY id FOR UPDATE
               )
               UPDATE monitors AS monitor
               SET cursor = $1,
                   completed = COALESCE(monitor.end_block <= $1, FALSE),
                   updated_at = NOW()
               FROM locked
               WHERE monitor.id = locked.id
               RETURNING monitor.*"#,
        )
        .bind(block_number)
        .bind(&monitor_ids)
        .bind(chain_id)
        .fetch_all(&mut *tx)
        .await?;
        anyhow::ensure!(
            rows.len() == monitor_ids.len(),
            "monitor set changed before block {} could be committed",
            commit.block_number
        );
        let rows = rows
            .into_iter()
            .map(|row| Ok((pg_types::from_monitor_id(row.id)?, row)))
            .collect::<anyhow::Result<HashMap<_, _>>>()?;

        let (mut calls, mut events) = (HashMap::new(), HashMap::new());
        for result in &commit.results {
            match result {
                DecodedResult::Call(call) => {
                    let row = rows.get(&call.monitor_id).ok_or_else(|| {
                        anyhow::anyhow!("monitor {} was not locked", call.monitor_id)
                    })?;
                    anyhow::ensure!(row.kind == "call", "call result references event monitor");
                    calls.entry(call.monitor_id).or_insert_with(Vec::new).push(CallResultInput {
                        tx_hash: call.transaction_hash,
                        block_number: call.block_number,
                        params: &call.params,
                    });
                }
                DecodedResult::Event(event) => {
                    let row = rows.get(&event.monitor_id).ok_or_else(|| {
                        anyhow::anyhow!("monitor {} was not locked", event.monitor_id)
                    })?;
                    anyhow::ensure!(row.kind == "event", "event result references call monitor");
                    events.entry(event.monitor_id).or_insert_with(Vec::new).push(
                        EventResultInput {
                            tx_hash: event.transaction_hash,
                            log_index: event.log_index,
                            block_number: event.block_number,
                            params: &event.params,
                        },
                    );
                }
            }
        }

        for (id, inputs) in calls {
            let row =
                rows.get(&id).ok_or_else(|| anyhow::anyhow!("monitor {id} was not locked"))?;
            dyn_table::insert_calls(&mut tx, row.id, &row.param_schema.0, &inputs).await?;
        }
        for (id, inputs) in events {
            let row =
                rows.get(&id).ok_or_else(|| anyhow::anyhow!("monitor {id} was not locked"))?;
            dyn_table::insert_events(&mut tx, row.id, &row.param_schema.0, &inputs).await?;
        }

        tx.commit().await?;
        Ok(())
    }
}

#[async_trait]
impl ChainRepository for PostgresStorage {
    async fn list_registered_chains(&self) -> anyhow::Result<Vec<RegisteredChain>> {
        chain_repo::list(&self.pool)
            .await?
            .iter()
            .map(chain_repo::ChainRecord::registered)
            .collect()
    }

    async fn create_chain(&self, input: NewChain) -> anyhow::Result<CoreChainRecord> {
        Self::chain_record(
            chain_repo::create(&self.pool, input.chain, &input.rpc_url, input.enabled).await?,
        )
    }

    async fn list_chains(&self) -> anyhow::Result<Vec<CoreChainRecord>> {
        chain_repo::list(&self.pool).await?.into_iter().map(Self::chain_record).collect()
    }

    async fn get_chain(&self, chain: Chain) -> anyhow::Result<CoreChainRecord> {
        Self::chain_record(chain_repo::get(&self.pool, chain.id).await?)
    }

    async fn update_chain(
        &self,
        chain: Chain,
        update: ChainUpdate,
    ) -> anyhow::Result<CoreChainRecord> {
        Self::chain_record(
            chain_repo::update(&self.pool, chain.id, update.rpc_url.as_ref(), update.enabled)
                .await?,
        )
    }

    async fn delete_chain(&self, chain: Chain) -> anyhow::Result<()> {
        chain_repo::delete(&self.pool, chain.id).await
    }
}

#[async_trait]
impl MonitorRepository for PostgresStorage {
    async fn count_monitors(&self) -> anyhow::Result<usize> {
        monitor_repo::count(&self.pool).await
    }

    async fn create_monitor(&self, monitor: NewMonitor) -> anyhow::Result<CoreMonitorRecord> {
        Self::monitor_record(monitor_repo::create_prepared(&self.pool, &monitor).await?)
    }

    async fn list_monitors(&self, chain: Option<Chain>) -> anyhow::Result<Vec<CoreMonitorRecord>> {
        monitor_repo::list(&self.pool, chain.map(|chain| chain.id))
            .await?
            .into_iter()
            .map(Self::monitor_record)
            .collect()
    }

    async fn get_monitor(&self, id: MonitorId) -> anyhow::Result<CoreMonitorRecord> {
        Self::monitor_record(monitor_repo::get(&self.pool, id).await?)
    }

    async fn set_monitor_enabled(
        &self,
        id: MonitorId,
        enabled: bool,
    ) -> anyhow::Result<CoreMonitorRecord> {
        Self::monitor_record(monitor_repo::set_enabled(&self.pool, id, enabled).await?)
    }

    async fn delete_monitor(&self, id: MonitorId) -> anyhow::Result<()> {
        monitor_repo::delete(&self.pool, id).await
    }
}

#[async_trait]
impl ResultRepository for PostgresStorage {
    async fn query_results(
        &self,
        monitor: &CoreMonitorRecord,
        query: ResultQuery,
    ) -> anyhow::Result<Vec<CoreResultRecord>> {
        let (kind, params) = match &monitor.target {
            Target::Call(target) => ("call", &target.inputs),
            Target::Event(target) => ("event", &target.params),
        };
        let schema = params
            .iter()
            .map(|param| monitor_repo::StoredParam {
                name: param.name.clone(),
                sol_type: param.sol_type(),
                indexed: param.indexed,
            })
            .collect::<Vec<_>>();
        Ok(dyn_table::query_results(
            &self.pool,
            pg_types::to_monitor_id(monitor.id)?,
            kind,
            &schema,
            &SearchParams { limit: query.limit, offset: query.offset },
        )
        .await?
        .into_iter()
        .map(|record| match record {
            ResultRecord::Call(record) => CoreResultRecord::Call {
                tx_hash: record.tx_hash,
                block_number: record.block_number,
                params: record.params,
            },
            ResultRecord::Event(record) => CoreResultRecord::Event {
                tx_hash: record.tx_hash,
                log_index: record.log_index,
                block_number: record.block_number,
                params: record.params,
            },
        })
        .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy::primitives::Address;
    use sqlx::postgres::PgPoolOptions;

    use super::*;
    use parseon_core::filter::Filter;
    use parseon_core::monitor::Monitor;
    use parseon_core::ports::Storage;

    fn assert_storage<T: Storage>() {}

    #[test]
    fn implements_unified_storage_port() {
        assert_storage::<PostgresStorage>();
    }

    fn monitor(chain_id: u64) -> Monitor {
        Monitor {
            id: MonitorId::new(1).unwrap(),
            chain: Chain::new(chain_id),
            target: Target::Call(CallTarget {
                address: Address::ZERO,
                selector: [0; 4].into(),
                inputs: Vec::new(),
            }),
            start_block: 0,
            end_block: None,
            cursor: Cursor(None),
            completed: false,
            enabled: true,
            filter: Filter::All,
        }
    }

    #[tokio::test]
    async fn rejects_cross_chain_commits_before_database_access() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@localhost/parseon")
            .unwrap();
        let storage = PostgresStorage::new(pool);
        let commit = BlockCommit {
            chain: Chain::new(1),
            block_number: 10,
            monitors: vec![Arc::new(monitor(2))],
            results: Vec::new(),
        };
        let error = storage.commit_block(&commit).await.unwrap_err();
        assert!(error.to_string().contains("cross-chain monitor set rejected"));
    }
}
