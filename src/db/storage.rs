use async_trait::async_trait;
use sqlx::PgPool;
use std::collections::HashMap;

use crate::core::abi::parse_selector;
use crate::core::filter::Filter;
use crate::core::monitor::Monitor;
use crate::core::ports::{BlockCommit, Storage};
use crate::core::{CallTarget, Cursor, DecodedResult, EventTarget, Target};
use crate::error::AppResult;

use super::dyn_table::{CallResultInput, EventResultInput, ResultRecord, SearchParams};
use super::{dyn_table, monitor_repo};

#[derive(Clone)]
pub struct PostgresStorage {
    pool: PgPool,
}

impl PostgresStorage {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn monitor_count(&self) -> AppResult<usize> {
        monitor_repo::count(&self.pool).await
    }

    pub async fn create_monitor(
        &self,
        input: &monitor_repo::MonitorInput,
    ) -> AppResult<monitor_repo::MonitorRecord> {
        monitor_repo::create(&self.pool, input).await
    }

    pub async fn list_monitor_records(&self) -> AppResult<Vec<monitor_repo::MonitorRecord>> {
        monitor_repo::list(&self.pool).await
    }

    pub async fn get_monitor(&self, id: i64) -> AppResult<monitor_repo::MonitorRecord> {
        monitor_repo::get(&self.pool, id).await
    }

    pub async fn update_monitor(
        &self,
        id: i64,
        start_block: Option<i64>,
        end_block: Option<Option<i64>>,
        enabled: Option<bool>,
    ) -> AppResult<monitor_repo::MonitorRecord> {
        monitor_repo::update(&self.pool, id, start_block, end_block, enabled).await
    }

    pub async fn delete_monitor(&self, id: i64) -> AppResult<()> {
        monitor_repo::delete(&self.pool, id).await
    }

    pub async fn query_results(
        &self,
        monitor: &monitor_repo::MonitorRecord,
        search: &SearchParams,
    ) -> AppResult<Vec<ResultRecord>> {
        dyn_table::query_results(
            &self.pool,
            monitor.id,
            &monitor.kind,
            &monitor.param_schema.0,
            search,
        )
        .await
    }

    fn to_monitor(row: &monitor_repo::MonitorRecord) -> anyhow::Result<Monitor> {
        Ok(Monitor {
            id: row.id,
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
}

#[async_trait]
impl Storage for PostgresStorage {
    async fn load_monitors(&self) -> anyhow::Result<Vec<Monitor>> {
        monitor_repo::list(&self.pool)
            .await?
            .iter()
            .map(Self::to_monitor)
            .collect()
    }

    async fn commit_block(&self, commit: BlockCommit) -> anyhow::Result<usize> {
        let mut tx = self.pool.begin().await?;
        let mut monitor_ids = commit
            .monitors
            .iter()
            .map(|monitor| monitor.id)
            .collect::<Vec<_>>();
        monitor_ids.sort_unstable();
        monitor_ids.dedup();

        let rows = sqlx::query_as::<_, monitor_repo::MonitorRecord>(
            "SELECT * FROM monitors WHERE id = ANY($1) ORDER BY id FOR UPDATE",
        )
        .bind(&monitor_ids)
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
            monitor_repo::set_cursor(&mut tx, monitor.id, commit.block_number, completed).await?;
        }

        tx.commit().await?;
        Ok(commit.results.len())
    }
}
