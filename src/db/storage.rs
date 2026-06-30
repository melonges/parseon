use std::collections::HashMap;
use std::str::FromStr;

use async_trait::async_trait;
use sqlx::PgPool;
use sqlx::types::BigDecimal;

use crate::core::abi::parse_selector;
use crate::core::filter::Filter;
use crate::core::monitor::Monitor;
use crate::core::ports::{BlockCommit, Storage};
use crate::core::{Cursor, Target};
use crate::error::AppResult;

use super::dyn_table::{ResultInput, ResultRecord, SearchParams};
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
            &monitor.address,
            &monitor.selector,
            &monitor.param_schema.0,
            search,
        )
        .await
    }

    fn to_monitor(row: &monitor_repo::MonitorRecord) -> anyhow::Result<Monitor> {
        Ok(Monitor {
            id: row.id,
            target: Target {
                address: row.address.parse()?,
                selector: parse_selector(&row.selector)?,
                signature: row.signature.clone(),
                inputs: row
                    .param_schema
                    .0
                    .iter()
                    .map(|param| param.to_abi())
                    .collect::<Result<Vec<_>, _>>()?,
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

        for call in &commit.calls {
            let row = rows
                .get(&call.monitor_id)
                .ok_or_else(|| anyhow::anyhow!("monitor {} was not locked", call.monitor_id))?;
            let executed = &call.transaction;
            let result = ResultInput {
                tx_hash: executed.transaction.hash.to_string(),
                block_number: call.block_number,
                block_hash: call.block_hash.to_string(),
                from_addr: executed.transaction.from.to_string(),
                to_addr: executed.transaction.to.to_string(),
                value: BigDecimal::from_str(&executed.transaction.value.to_string())?,
                gas_used: BigDecimal::from(executed.gas_used),
                gas_price: BigDecimal::from_str(&executed.gas_price.to_string())?,
                status: if executed.succeeded { 1 } else { 0 },
                input_raw: executed.transaction.input.clone(),
                params: call.params.clone(),
            };
            dyn_table::insert_result(
                &mut tx,
                &row.address,
                &row.selector,
                &row.param_schema.0,
                &result,
            )
            .await?;
        }

        for monitor in &commit.monitors {
            let completed = monitor
                .end_block
                .is_some_and(|end| commit.block_number >= end);
            monitor_repo::set_cursor(&mut tx, monitor.id, commit.block_number, completed).await?;
        }

        tx.commit().await?;
        Ok(commit.calls.len())
    }
}
