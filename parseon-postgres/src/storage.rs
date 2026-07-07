use async_trait::async_trait;
use sqlx::PgPool;
use std::collections::HashMap;

use parseon_core::abi::parse_selector;
use parseon_core::commands::ResultQuery;
use parseon_core::filter::Filter;
use parseon_core::monitor::Monitor;
use parseon_core::ports::{
    BlockCommit, ChainRecord as CoreChainRecord, ChainRepository, ChainUpdate, IndexStorage,
    MonitorKind, MonitorRecord as CoreMonitorRecord, MonitorRepository, MonitorUpdate, NewChain,
    NewMonitor, ParamSchema, RegisteredChain, ResultRecord as CoreResultRecord, ResultRepository,
};
use parseon_core::{CallTarget, Chain, Cursor, DecodedResult, EventTarget, Target};

use super::dyn_table::{CallResultInput, EventResultInput, ResultRecord, SearchParams};
use super::{chain_repo, dyn_table, monitor_repo};

#[derive(Clone)]
pub struct PostgresStorage {
    pool: PgPool,
}

impl PostgresStorage {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    fn to_monitor(row: &monitor_repo::MonitorRecord) -> anyhow::Result<Monitor> {
        Ok(Monitor {
            id: row.id,
            chain: Chain::new(row.chain_id)?,
            target: if row.kind == "call" {
                Target::Call(CallTarget {
                    address: row.address.parse()?,
                    selector: parse_selector(&row.signature_hash)?,
                    signature: row.signature.clone(),
                    inputs: row
                        .param_schema
                        .0
                        .iter()
                        .map(|param| param.to_abi())
                        .collect::<Result<Vec<_>, _>>()?,
                })
            } else {
                Target::Event(EventTarget {
                    address: row.address.parse()?,
                    topic0: row.signature_hash.parse()?,
                    signature: row.signature.clone(),
                    params: row
                        .param_schema
                        .0
                        .iter()
                        .map(|p| p.to_abi())
                        .collect::<Result<Vec<_>, _>>()?,
                })
            },
            start_block: row.start_block,
            end_block: row.end_block,
            cursor: Cursor(row.cursor),
            completed: row.completed,
            enabled: row.enabled,
            filter: Filter::All,
        })
    }

    fn chain_record(row: chain_repo::ChainRecord) -> anyhow::Result<CoreChainRecord> {
        let rpc_url = row.rpc_url()?;
        Ok(CoreChainRecord {
            chain: Chain::new(row.chain_id)?,
            rpc_url,
            enabled: row.enabled,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }

    fn monitor_record(row: monitor_repo::MonitorRecord) -> anyhow::Result<CoreMonitorRecord> {
        Ok(CoreMonitorRecord {
            id: row.id,
            chain: Chain::new(row.chain_id)?,
            address: row.address,
            signature: row.signature,
            kind: match row.kind.as_str() {
                "call" => MonitorKind::Call,
                "event" => MonitorKind::Event,
                kind => anyhow::bail!("invalid monitor kind {kind}"),
            },
            signature_hash: row.signature_hash,
            param_schema: row
                .param_schema
                .0
                .into_iter()
                .map(|param| ParamSchema {
                    name: param.name,
                    sol_type: param.sol_type,
                    indexed: param.indexed,
                })
                .collect(),
            start_block: row.start_block,
            end_block: row.end_block,
            cursor: row.cursor,
            completed: row.completed,
            enabled: row.enabled,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[async_trait]
impl IndexStorage for PostgresStorage {
    async fn load_monitors(&self, chain: Chain) -> anyhow::Result<Vec<Monitor>> {
        monitor_repo::list(&self.pool, Some(chain.id))
            .await?
            .iter()
            .map(Self::to_monitor)
            .collect()
    }

    async fn commit_block(&self, commit: BlockCommit) -> anyhow::Result<usize> {
        anyhow::ensure!(
            commit
                .monitors
                .iter()
                .all(|monitor| monitor.chain == commit.chain),
            "cross-chain monitor set rejected for chain {}",
            commit.chain.id
        );
        let mut tx = self.pool.begin().await?;
        let mut monitor_ids = commit
            .monitors
            .iter()
            .map(|monitor| monitor.id)
            .collect::<Vec<_>>();
        monitor_ids.sort_unstable();
        monitor_ids.dedup();

        let rows = sqlx::query_as::<_, monitor_repo::MonitorRecord>(
            "SELECT * FROM monitors WHERE id = ANY($1) AND chain_id = $2 ORDER BY id FOR UPDATE",
        )
        .bind(&monitor_ids)
        .bind(commit.chain.id)
        .fetch_all(&mut *tx)
        .await?;
        anyhow::ensure!(
            rows.len() == monitor_ids.len(),
            "monitor set changed before block {} could be committed",
            commit.block_number
        );
        let rows = rows
            .into_iter()
            .map(|row| (row.id, row))
            .collect::<HashMap<_, _>>();

        for result in &commit.results {
            match result {
                DecodedResult::Call(call) => {
                    let row = rows.get(&call.monitor_id).ok_or_else(|| {
                        anyhow::anyhow!("monitor {} was not locked", call.monitor_id)
                    })?;
                    let executed = &call.transaction;
                    let result = CallResultInput {
                        tx_hash: executed.transaction.hash.to_string(),
                        block_number: call.block_number,
                        params: call.params.clone(),
                    };
                    dyn_table::insert_call(&mut tx, row.id, &row.param_schema.0, &result).await?;
                }
                DecodedResult::Event(event) => {
                    let row = rows.get(&event.monitor_id).ok_or_else(|| {
                        anyhow::anyhow!("monitor {} was not locked", event.monitor_id)
                    })?;
                    let result = EventResultInput {
                        tx_hash: event.transaction_hash.to_string(),
                        log_index: i64::try_from(event.log_index)?,
                        block_number: event.block_number,
                        params: event.params.clone(),
                    };
                    dyn_table::insert_event(&mut tx, row.id, &row.param_schema.0, &result).await?;
                }
            }
        }

        for monitor in &commit.monitors {
            let completed = monitor
                .end_block
                .is_some_and(|end| commit.block_number >= end);
            monitor_repo::set_cursor(
                &mut tx,
                monitor.id,
                commit.chain.id,
                commit.block_number,
                completed,
            )
            .await?;
        }

        tx.commit().await?;
        Ok(commit.results.len())
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
        Self::chain_record(chain_repo::create(&self.pool, input.chain, &input.rpc_url, input.enabled).await?)
    }

    async fn list_chains(&self) -> anyhow::Result<Vec<CoreChainRecord>> {
        chain_repo::list(&self.pool).await?.into_iter().map(Self::chain_record).collect()
    }

    async fn get_chain(&self, chain: Chain) -> anyhow::Result<CoreChainRecord> {
        Self::chain_record(chain_repo::get(&self.pool, chain.id).await?)
    }

    async fn update_chain(&self, chain: Chain, update: ChainUpdate) -> anyhow::Result<CoreChainRecord> {
        Self::chain_record(chain_repo::update(&self.pool, chain.id, update.rpc_url.as_ref(), update.enabled).await?)
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
        monitor_repo::list(&self.pool, chain.map(|chain| chain.id)).await?.into_iter().map(Self::monitor_record).collect()
    }

    async fn get_monitor(&self, id: i64) -> anyhow::Result<CoreMonitorRecord> {
        Self::monitor_record(monitor_repo::get(&self.pool, id).await?)
    }

    async fn update_monitor(&self, id: i64, update: MonitorUpdate) -> anyhow::Result<CoreMonitorRecord> {
        Self::monitor_record(monitor_repo::update_prepared(&self.pool, id, &update).await?)
    }

    async fn delete_monitor(&self, id: i64) -> anyhow::Result<()> {
        monitor_repo::delete(&self.pool, id).await
    }
}

#[async_trait]
impl ResultRepository for PostgresStorage {
    async fn query_results(&self, monitor: &CoreMonitorRecord, query: ResultQuery) -> anyhow::Result<Vec<CoreResultRecord>> {
        let schema = monitor.param_schema.iter().map(|param| monitor_repo::StoredParam {
            name: param.name.clone(), sol_type: param.sol_type.clone(), indexed: param.indexed,
        }).collect::<Vec<_>>();
        Ok(dyn_table::query_results(&self.pool, monitor.id, monitor.kind.as_str(), &schema, &SearchParams { limit: query.limit, offset: query.offset }).await?.into_iter().map(|record| match record {
            ResultRecord::Call(record) => CoreResultRecord::Call { tx_hash: record.tx_hash, block_number: record.block_number, params: record.params },
            ResultRecord::Event(record) => CoreResultRecord::Event { tx_hash: record.tx_hash, log_index: record.log_index, block_number: record.block_number, params: record.params },
        }).collect())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;
    use sqlx::postgres::PgPoolOptions;

    use super::*;
    use parseon_core::filter::Filter;
    use parseon_core::monitor::Monitor;

    fn monitor(chain_id: i64) -> Monitor {
        Monitor {
            id: 1,
            chain: Chain::new(chain_id).unwrap(),
            target: Target::Call(CallTarget {
                address: Address::ZERO,
                selector: [0; 4],
                signature: "f()".into(),
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
        let error = storage
            .commit_block(BlockCommit {
                chain: Chain::new(1).unwrap(),
                block_number: 10,
                monitors: vec![monitor(2)],
                results: Vec::new(),
            })
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cross-chain monitor set rejected")
        );
    }
}
