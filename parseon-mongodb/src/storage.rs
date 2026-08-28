use std::collections::HashMap;
use std::str::FromStr;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use mongodb::bson::{self, Binary, Bson, Document, doc};
use mongodb::error::ErrorKind;
use mongodb::options::{IndexOptions, ReturnDocument};
use mongodb::{Client, ClientSession, Collection, Database, IndexModel};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use parseon_core::abi::{AbiParam, parse_abi_type};
use parseon_core::commands::ResultQuery;
use parseon_core::filter::{Filter, FilterDefinition, FilterExpression};
use parseon_core::monitor::Monitor;
use parseon_core::ports::{
    BlockCommit, CanonicalBlock, ChainRecord, ChainRepository, ChainUpdate, IndexStorage,
    MonitorRecord, MonitorRepository, NewChain, NewMonitor, RegisteredChain, ResultRecord,
    ResultRepository,
};
use parseon_core::{
    Address, B256, BlockMetadata, CallTarget, Chain, Cursor, DecodedResult, DecodedValue,
    EventTarget, Finality, MonitorId, Selector, Target, TxHash, Url,
};

type AppResult<T> = anyhow::Result<T>;

const LEGACY_MONITOR_TARGET_INDEX: &str = "monitors_target";
const MONITOR_TARGET_LOOKUP_INDEX: &str = "monitors_target_lookup";
const NAMESPACE_NOT_FOUND: i32 = 26;
const INDEX_NOT_FOUND: i32 = 27;
const SCHEMA_VERSION: i64 = 1;
const MAX_TRANSACTION_ATTEMPTS: usize = 8;

#[derive(Clone)]
pub struct MongoStorage {
    client: Client,
    database: Database,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChainDocument {
    chain_id: i64,
    rpc_url: String,
    enabled: bool,
    created_at: bson::DateTime,
    updated_at: bson::DateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredParam {
    name: String,
    sol_type: String,
    #[serde(default)]
    indexed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MonitorDocument {
    id: i64,
    chain_id: i64,
    address: String,
    kind: String,
    signature_hash: String,
    param_schema: Vec<StoredParam>,
    start_block: i64,
    end_block: Option<i64>,
    cursor: Option<i64>,
    completed: bool,
    enabled: bool,
    filter_ast: Option<FilterExpression>,
    filter_version: Option<i16>,
    created_at: bson::DateTime,
    updated_at: bson::DateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CanonicalBlockDocument {
    chain_id: i64,
    block_number: i64,
    block_hash: String,
    parent_hash: String,
    block_timestamp: i64,
    finality: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResultDocument {
    chain_id: i64,
    monitor_id: i64,
    kind: String,
    tx_hash: String,
    block_hash: String,
    log_index: Option<i64>,
    block_number: i64,
    from: Option<String>,
    to: Option<String>,
    finality: String,
    params: Document,
}

impl MongoStorage {
    pub async fn connect(storage_url: &Url, database: &str) -> AppResult<Self> {
        anyhow::ensure!(!database.is_empty(), "storage database must not be empty");
        let client = Client::with_uri_str(storage_url.as_str()).await?;
        let hello = client.database("admin").run_command(doc! { "hello": 1 }).await?;
        anyhow::ensure!(
            supports_transactions(&hello),
            "MongoDB storage requires a replica set or sharded deployment"
        );
        let storage = Self { database: client.database(database), client };
        storage.ensure_schema().await?;
        storage.create_indexes().await?;
        tracing::info!(database, "MongoDB storage initialized");
        Ok(storage)
    }

    fn chains(&self) -> Collection<ChainDocument> {
        self.database.collection("chains")
    }

    fn monitors(&self) -> Collection<MonitorDocument> {
        self.database.collection("monitors")
    }

    fn results(&self) -> Collection<ResultDocument> {
        self.database.collection("results")
    }

    fn blocks(&self) -> Collection<CanonicalBlockDocument> {
        self.database.collection("canonical_blocks")
    }

    fn counters(&self) -> Collection<Document> {
        self.database.collection("counters")
    }

    async fn ensure_schema(&self) -> AppResult<()> {
        self.ensure_required_fields(
            "chains",
            &["chain_id", "rpc_url", "enabled", "created_at", "updated_at"],
        )
        .await?;
        self.ensure_required_fields(
            "monitors",
            &[
                "id",
                "chain_id",
                "address",
                "kind",
                "signature_hash",
                "param_schema",
                "start_block",
                "end_block",
                "cursor",
                "completed",
                "enabled",
                "created_at",
                "updated_at",
            ],
        )
        .await?;
        self.ensure_required_fields(
            "results",
            &[
                "chain_id",
                "monitor_id",
                "kind",
                "tx_hash",
                "block_hash",
                "block_number",
                "finality",
                "params",
            ],
        )
        .await?;
        self.ensure_required_fields(
            "canonical_blocks",
            &[
                "chain_id",
                "block_number",
                "block_hash",
                "parent_hash",
                "block_timestamp",
                "finality",
            ],
        )
        .await?;
        self.ensure_decodable::<ChainDocument>("chains").await?;
        self.ensure_decodable::<MonitorDocument>("monitors").await?;
        self.ensure_decodable::<ResultDocument>("results").await?;
        self.ensure_decodable::<CanonicalBlockDocument>("canonical_blocks").await?;
        let metadata = self.database.collection::<Document>("schema_metadata");
        let row = metadata.find_one(doc! { "_id": "parseon" }).await?;
        if let Some(row) = row {
            anyhow::ensure!(
                row.get_i64("version")? == SCHEMA_VERSION,
                "unsupported Parseon MongoDB schema version"
            );
        } else {
            metadata.insert_one(doc! { "_id": "parseon", "version": SCHEMA_VERSION }).await?;
        }
        Ok(())
    }

    async fn ensure_required_fields(&self, collection: &str, fields: &[&str]) -> AppResult<()> {
        let missing = fields
            .iter()
            .map(|field| {
                let mut condition = Document::new();
                condition.insert("$exists", false);
                let mut clause = Document::new();
                clause.insert(*field, condition);
                Bson::Document(clause)
            })
            .collect::<Vec<_>>();
        let legacy = self
            .database
            .collection::<Document>(collection)
            .find_one(doc! { "$or": Bson::Array(missing) })
            .await?;
        anyhow::ensure!(
            legacy.is_none(),
            "MongoDB collection {collection} contains pre-v1 documents with missing required fields; reset and reindex before upgrade"
        );
        Ok(())
    }

    async fn ensure_decodable<T>(&self, collection: &str) -> AppResult<()>
    where
        T: DeserializeOwned,
    {
        if let Some(document) =
            self.database.collection::<Document>(collection).find_one(doc! {}).await?
        {
            bson::from_document::<T>(document).map_err(|error| {
                anyhow::anyhow!(
                    "MongoDB collection {collection} contains an invalid v1 document: {error}"
                )
            })?;
        }
        Ok(())
    }

    async fn create_indexes(&self) -> AppResult<()> {
        match self.blocks().drop_index("blocks_chain_hash").await {
            Ok(_) => {}
            Err(error) if missing_index(&error) => {}
            Err(error) => return Err(error.into()),
        }
        self.drop_legacy_monitor_target_index().await?;
        self.drop_legacy_result_indexes().await?;
        for (collection, index) in indexes() {
            self.database.collection::<Document>(collection).create_index(index).await?;
        }
        Ok(())
    }

    async fn drop_legacy_result_indexes(&self) -> AppResult<()> {
        for name in ["results_call_identity", "results_event_identity"] {
            match self.results().drop_index(name).await {
                Ok(_) => {}
                Err(error) if missing_index(&error) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    async fn drop_legacy_monitor_target_index(&self) -> AppResult<()> {
        match self
            .database
            .collection::<Document>("monitors")
            .drop_index(LEGACY_MONITOR_TARGET_INDEX)
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if missing_index(&error) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn transaction<T>(
        &self,
        mut operation: impl for<'a> FnMut(
            &'a mut ClientSession,
        ) -> futures_util::future::BoxFuture<'a, AppResult<T>>,
    ) -> AppResult<T> {
        let mut session = self.client.start_session().await?;
        'transaction: for attempt in 0..MAX_TRANSACTION_ATTEMPTS {
            session.start_transaction().await?;
            let value = match operation(&mut session).await {
                Ok(value) => value,
                Err(error) => {
                    drop(session.abort_transaction().await);
                    if transient(&error) && attempt + 1 < MAX_TRANSACTION_ATTEMPTS {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            1_u64 << attempt.min(6),
                        ))
                        .await;
                        continue 'transaction;
                    }
                    return Err(error);
                }
            };
            match session.commit_transaction().await {
                Ok(()) => return Ok(value),
                Err(error) if error.contains_label("UnknownTransactionCommitResult") => {
                    return Err(error.into());
                }
                Err(error)
                    if error.contains_label("TransientTransactionError")
                        && attempt + 1 < MAX_TRANSACTION_ATTEMPTS =>
                {
                    drop(session.abort_transaction().await);
                    tokio::time::sleep(std::time::Duration::from_millis(1_u64 << attempt.min(6)))
                        .await;
                    continue 'transaction;
                }
                Err(error) => return Err(error.into()),
            }
        }
        anyhow::bail!("MongoDB transaction exceeded retry limit")
    }

    fn target(row: &MonitorDocument) -> AppResult<Target> {
        let address = Address::from_str(&row.address)?;
        let params = row
            .param_schema
            .iter()
            .map(|param| {
                Ok(AbiParam::new(param.name.clone(), parse_abi_type(&param.sol_type)?)?
                    .with_indexed(param.indexed))
            })
            .collect::<AppResult<Vec<_>>>()?;
        match row.kind.as_str() {
            "call" => Ok(Target::Call(CallTarget {
                address,
                selector: Selector::from_str(&row.signature_hash)?,
                inputs: params,
            })),
            "event" => Ok(Target::Event(EventTarget {
                address,
                topic0: B256::from_str(&row.signature_hash)?,
                params,
            })),
            kind => anyhow::bail!("invalid monitor kind {kind}"),
        }
    }

    fn monitor(row: &MonitorDocument) -> AppResult<Monitor> {
        let target = Self::target(row)?;
        let filter = match (&row.filter_ast, row.filter_version) {
            (None, None) => Filter::All,
            (Some(expression), Some(version)) => {
                FilterDefinition { version, expression: expression.clone() }.compile(&target)?
            }
            _ => anyhow::bail!("monitor {} has incomplete filter state", row.id),
        };
        Ok(Monitor {
            id: MonitorId::new(from_i64(row.id, "monitor id")?)?,
            chain: Chain::new(from_i64(row.chain_id, "chain id")?),
            target,
            start_block: from_i64(row.start_block, "start block")?,
            end_block: row.end_block.map(|value| from_i64(value, "end block")).transpose()?,
            cursor: Cursor(row.cursor.map(|value| from_i64(value, "cursor")).transpose()?),
            completed: row.completed,
            enabled: row.enabled,
            filter,
        })
    }

    fn canonical(row: CanonicalBlockDocument) -> AppResult<CanonicalBlock> {
        let finality = match row.finality.as_str() {
            "provisional" => Finality::Provisional,
            "finalized" => Finality::Finalized,
            value => anyhow::bail!("invalid canonical block finality {value}"),
        };
        Ok(CanonicalBlock {
            chain: Chain::new(from_i64(row.chain_id, "chain id")?),
            metadata: BlockMetadata {
                number: from_i64(row.block_number, "block number")?,
                hash: B256::from_str(&row.block_hash)?,
                parent_hash: B256::from_str(&row.parent_hash)?,
                timestamp: from_i64(row.block_timestamp, "block timestamp")?,
            },
            finality,
        })
    }

    fn chain_record(row: ChainDocument) -> AppResult<ChainRecord> {
        Ok(ChainRecord {
            chain: Chain::new(from_i64(row.chain_id, "chain id")?),
            rpc_url: row.rpc_url.parse()?,
            enabled: row.enabled,
            created_at: chrono(row.created_at)?,
            updated_at: chrono(row.updated_at)?,
        })
    }

    fn monitor_record(row: MonitorDocument) -> AppResult<MonitorRecord> {
        let filter = match (row.filter_ast.clone(), row.filter_version) {
            (None, None) => None,
            (Some(expression), Some(version)) => Some(FilterDefinition { version, expression }),
            _ => anyhow::bail!("monitor {} has incomplete filter state", row.id),
        };
        Ok(MonitorRecord {
            id: MonitorId::new(from_i64(row.id, "monitor id")?)?,
            chain: Chain::new(from_i64(row.chain_id, "chain id")?),
            target: Self::target(&row)?,
            start_block: from_i64(row.start_block, "start block")?,
            end_block: row.end_block.map(|value| from_i64(value, "end block")).transpose()?,
            cursor: row.cursor.map(|value| from_i64(value, "cursor")).transpose()?,
            completed: row.completed,
            enabled: row.enabled,
            filter,
            created_at: chrono(row.created_at)?,
            updated_at: chrono(row.updated_at)?,
        })
    }
}

#[async_trait]
impl IndexStorage for MongoStorage {
    async fn load_monitors(&self, chain: Chain) -> AppResult<Vec<Monitor>> {
        let mut rows = self
            .monitors()
            .find(doc! { "chain_id": to_i64(chain.id, "chain id")? })
            .sort(doc! { "id": 1 })
            .await?;
        let mut monitors = Vec::new();
        while let Some(row) = rows.try_next().await? {
            monitors.push(Self::monitor(&row)?);
        }
        Ok(monitors)
    }

    async fn canonical_tip(&self, chain: Chain) -> AppResult<Option<CanonicalBlock>> {
        Ok(self
            .blocks()
            .find_one(doc! { "chain_id": to_i64(chain.id, "chain id")? })
            .sort(doc! { "block_number": -1 })
            .await?
            .map(Self::canonical)
            .transpose()?)
    }

    async fn canonical_block(
        &self,
        chain: Chain,
        block_number: u64,
    ) -> AppResult<Option<CanonicalBlock>> {
        Ok(self
            .blocks()
            .find_one(doc! {
                "chain_id": to_i64(chain.id, "chain id")?,
                "block_number": to_i64(block_number, "block number")?
            })
            .await?
            .map(Self::canonical)
            .transpose()?)
    }

    async fn commit_block(&self, commit: &BlockCommit) -> AppResult<()> {
        commit.validate()?;
        let chain_id = to_i64(commit.chain.id, "chain id")?;
        let block_number = to_i64(commit.metadata.number, "block number")?;
        let mut ids = commit
            .monitors
            .iter()
            .map(|monitor| to_i64(monitor.id.get(), "monitor id"))
            .collect::<AppResult<Vec<_>>>()?;
        ids.sort_unstable();
        ids.dedup();
        let monitors = commit
            .monitors
            .iter()
            .map(|monitor| (monitor.id, monitor.as_ref()))
            .collect::<HashMap<_, _>>();
        let documents = commit
            .results
            .iter()
            .map(|result| result_document(chain_id, commit.finality, &monitors, result))
            .collect::<AppResult<Vec<_>>>()?;
        let collection = self.monitors();
        let results = self.results();
        let blocks = self.blocks();
        let block = CanonicalBlockDocument {
            chain_id,
            block_number,
            block_hash: format!("{:#x}", commit.metadata.hash),
            parent_hash: format!("{:#x}", commit.metadata.parent_hash),
            block_timestamp: to_i64(commit.metadata.timestamp, "block timestamp")?,
            finality: commit.finality.as_str().into(),
        };
        let commit_finality = commit.finality;
        self.transaction(move |session| {
            let collection = collection.clone();
            let results = results.clone();
            let blocks = blocks.clone();
            let block = block.clone();
            let ids = ids.clone();
            let mut documents = documents.clone();
            Box::pin(async move {
                let existing = blocks
                    .find_one(doc! { "chain_id": chain_id, "block_number": block_number })
                    .session(&mut *session)
                    .await?;
                let effective_finality = if let Some(existing) = existing {
                    anyhow::ensure!(
                        existing.block_hash == block.block_hash
                            && existing.parent_hash == block.parent_hash,
                        "canonical block {} hash changed without rollback",
                        block_number
                    );
                    anyhow::ensure!(
                        existing.finality == Finality::Provisional.as_str()
                            || existing.finality == Finality::Finalized.as_str(),
                        "canonical block {} has invalid finality",
                        block_number
                    );
                    let effective = existing.finality == Finality::Finalized.as_str()
                        || block.finality == Finality::Finalized.as_str();
                    if effective && existing.finality == Finality::Provisional.as_str() {
                        blocks
                            .update_one(
                                doc! { "chain_id": chain_id, "block_number": block_number },
                                doc! { "$set": { "finality": Finality::Finalized.as_str() } },
                            )
                            .session(&mut *session)
                            .await?;
                    }
                    effective
                } else {
                    drop(blocks.insert_one(block).session(&mut *session).await?);
                    matches!(commit_finality, Finality::Finalized)
                };
                if effective_finality {
                    for document in &mut documents {
                        document.finality = Finality::Finalized.as_str().into();
                    }
                }
                let updated = collection
                    .update_many(
                        doc! { "id": { "$in": &ids }, "chain_id": chain_id },
                        vec![doc! { "$set": {
                            "cursor": block_number,
                            "completed": { "$and": [
                                { "$ne": ["$end_block", null] },
                                { "$lte": ["$end_block", block_number] }
                            ] },
                            "updated_at": bson::DateTime::now()
                        } }],
                    )
                    .session(&mut *session)
                    .await?;
                anyhow::ensure!(
                    updated.matched_count == ids.len() as u64,
                    "monitor set changed before block {block_number} could be committed"
                );
                if !documents.is_empty() {
                    drop(results.insert_many(documents).session(&mut *session).await?);
                }
                Ok(())
            })
        })
        .await?;
        Ok(())
    }

    async fn rollback_to(&self, chain: Chain, ancestor: u64) -> AppResult<()> {
        let chain_id = to_i64(chain.id, "chain id")?;
        let ancestor = to_i64(ancestor, "ancestor block")?;
        let blocks = self.blocks();
        let monitors = self.monitors();
        let results = self.results();
        self.transaction(move |session| {
            let blocks = blocks.clone();
            let monitors = monitors.clone();
            let results = results.clone();
            Box::pin(async move {
                let finalized = blocks
                    .find_one(doc! {
                        "chain_id": chain_id,
                        "block_number": { "$gt": ancestor },
                        "finality": "finalized"
                    })
                    .session(&mut *session)
                    .await?;
                anyhow::ensure!(finalized.is_none(), "rollback crosses promoted finalized boundary");
                results
                    .delete_many(doc! { "chain_id": chain_id, "block_number": { "$gt": ancestor } })
                    .session(&mut *session)
                    .await?;
                monitors
                    .update_many(
                        doc! { "chain_id": chain_id },
                        vec![doc! { "$set": {
                            "cursor": { "$cond": [
                                { "$gt": ["$start_block", ancestor] },
                                null,
                                { "$cond": [
                                    { "$or": [ { "$eq": ["$cursor", null] }, { "$lte": ["$cursor", ancestor] } ] },
                                    "$cursor", ancestor
                                ] }
                            ] },
                            "completed": { "$cond": [
                                { "$eq": ["$end_block", null] }, false, { "$lte": ["$end_block", ancestor] }
                            ] },
                            "updated_at": bson::DateTime::now()
                        } }],
                    )
                    .session(&mut *session)
                    .await?;
                blocks
                    .delete_many(doc! { "chain_id": chain_id, "block_number": { "$gt": ancestor } })
                    .session(&mut *session)
                    .await?;
                Ok(())
            })
        })
        .await
    }

    async fn promote_finalized(
        &self,
        chain: Chain,
        finalized_head: u64,
    ) -> AppResult<Vec<parseon_core::ports::SinkBatch>> {
        let chain_id = to_i64(chain.id, "chain id")?;
        let head = to_i64(finalized_head, "finalized head")?;
        let blocks = self.blocks();
        let monitors = self.monitors();
        let results = self.results();
        self.transaction(move |session| {
            let blocks = blocks.clone();
            let monitors = monitors.clone();
            let results = results.clone();
            Box::pin(async move {
                let mut cursor = blocks
                    .find(doc! { "chain_id": chain_id, "finality": "provisional", "block_number": { "$lte": head } })
                    .sort(doc! { "block_number": 1 })
                    .session(&mut *session)
                    .await?;
                let mut promoted = Vec::new();
                while let Some(block) = cursor.next(&mut *session).await {
                    promoted.push(block?);
                }
                if promoted.is_empty() {
                    return Ok(Vec::new());
                }
                blocks
                    .update_many(
                        doc! { "chain_id": chain_id, "finality": "provisional", "block_number": { "$lte": head } },
                        doc! { "$set": { "finality": "finalized" } },
                    )
                    .session(&mut *session)
                    .await?;
                results
                    .update_many(
                        doc! { "chain_id": chain_id, "finality": "provisional", "block_number": { "$lte": head } },
                        doc! { "$set": { "finality": "finalized" } },
                    )
                    .session(&mut *session)
                    .await?;
                let mut monitor_cursor = monitors
                    .find(doc! { "chain_id": chain_id })
                    .session(&mut *session)
                    .await?;
                let mut monitor_map = HashMap::new();
                while let Some(monitor) = monitor_cursor.next(&mut *session).await {
                    let monitor = monitor?;
                    monitor_map.insert(monitor.id, monitor);
                }
                let mut batches = Vec::new();
                for block in promoted {
                    let mut rows = results
                        .find(doc! { "chain_id": chain_id, "block_hash": &block.block_hash, "block_number": block.block_number, "finality": "finalized" })
                        .session(&mut *session)
                        .await?;
                    let mut sink_results = Vec::new();
                    while let Some(row) = rows.next(&mut *session).await {
                        let row = row?;
                        let monitor = monitor_map.get(&row.monitor_id).ok_or_else(|| anyhow::anyhow!("result references missing monitor {}", row.monitor_id))?;
                        let params = bson_params_to_json(row.params)?;
                        let tx_hash = TxHash::from_str(&row.tx_hash)?;
                        if row.kind == "call" {
                            sink_results.push(parseon_core::ports::SinkResult::Call {
                                monitor_id: u64::try_from(row.monitor_id)?,
                                tx_hash,
                                from: Address::from_str(row.from.as_deref().ok_or_else(|| anyhow::anyhow!("missing call sender"))?)?,
                                to: Address::from_str(row.to.as_deref().ok_or_else(|| anyhow::anyhow!("missing call recipient"))?)?,
                                params,
                            });
                        } else {
                            sink_results.push(parseon_core::ports::SinkResult::Event {
                                monitor_id: u64::try_from(row.monitor_id)?,
                                tx_hash,
                                emitter: Address::from_str(&monitor.address)?,
                                log_index: from_i64(row.log_index.ok_or_else(|| anyhow::anyhow!("missing log index"))?, "log index")?,
                                params,
                            });
                        }
                    }
                    if !sink_results.is_empty() {
                        batches.push(parseon_core::ports::SinkBatch {
                            version: 1,
                            chain_id: chain.id,
                            block_number: from_i64(block.block_number, "block number")?,
                            results: sink_results,
                        });
                    }
                }
                Ok(batches)
            })
        })
        .await
    }
}

#[async_trait]
impl ChainRepository for MongoStorage {
    async fn list_registered_chains(&self) -> AppResult<Vec<RegisteredChain>> {
        let mut rows = self.chains().find(doc! {}).sort(doc! { "chain_id": 1 }).await?;
        let mut result = Vec::new();
        while let Some(row) = rows.try_next().await? {
            result.push(RegisteredChain {
                chain: Chain::new(from_i64(row.chain_id, "chain id")?),
                rpc_url: row.rpc_url.parse()?,
                enabled: row.enabled,
            });
        }
        Ok(result)
    }

    async fn create_chain(&self, input: NewChain) -> AppResult<ChainRecord> {
        let now = bson::DateTime::now();
        let row = ChainDocument {
            chain_id: to_i64(input.chain.id, "chain id")?,
            rpc_url: input.rpc_url.to_string(),
            enabled: input.enabled,
            created_at: now,
            updated_at: now,
        };
        drop(self.chains().insert_one(&row).await?);
        Self::chain_record(row)
    }

    async fn list_chains(&self) -> AppResult<Vec<ChainRecord>> {
        let mut rows = self.chains().find(doc! {}).sort(doc! { "chain_id": 1 }).await?;
        let mut result = Vec::new();
        while let Some(row) = rows.try_next().await? {
            result.push(Self::chain_record(row)?);
        }
        Ok(result)
    }

    async fn get_chain(&self, chain: Chain) -> AppResult<ChainRecord> {
        let id = to_i64(chain.id, "chain id")?;
        let row = self
            .chains()
            .find_one(doc! { "chain_id": id })
            .await?
            .ok_or_else(|| anyhow::anyhow!("chain {id} not found"))?;
        Self::chain_record(row)
    }

    async fn update_chain(&self, chain: Chain, update: ChainUpdate) -> AppResult<ChainRecord> {
        let id = to_i64(chain.id, "chain id")?;
        let mut set = doc! { "updated_at": bson::DateTime::now() };
        if let Some(url) = update.rpc_url {
            set.insert("rpc_url", url.to_string());
        }
        if let Some(enabled) = update.enabled {
            set.insert("enabled", enabled);
        }
        let row = self
            .chains()
            .find_one_and_update(doc! { "chain_id": id }, doc! { "$set": set })
            .return_document(ReturnDocument::After)
            .await?
            .ok_or_else(|| anyhow::anyhow!("chain {id} not found"))?;
        Self::chain_record(row)
    }

    async fn delete_chain(&self, chain: Chain) -> AppResult<()> {
        let chain_id = to_i64(chain.id, "chain id")?;
        let chains = self.chains();
        let monitors = self.monitors();
        let results = self.results();
        let blocks = self.blocks();
        self.transaction(move |session| {
            let chains = chains.clone();
            let monitors = monitors.clone();
            let results = results.clone();
            let blocks = blocks.clone();
            Box::pin(async move {
                let deleted =
                    chains.delete_one(doc! { "chain_id": chain_id }).session(&mut *session).await?;
                anyhow::ensure!(deleted.deleted_count == 1, "chain {chain_id} not found");
                let _ = monitors
                    .delete_many(doc! { "chain_id": chain_id })
                    .session(&mut *session)
                    .await?;
                let _ = results
                    .delete_many(doc! { "chain_id": chain_id })
                    .session(&mut *session)
                    .await?;
                let _ = blocks
                    .delete_many(doc! { "chain_id": chain_id })
                    .session(&mut *session)
                    .await?;
                Ok(())
            })
        })
        .await
    }
}

#[async_trait]
impl MonitorRepository for MongoStorage {
    async fn count_monitors(&self) -> AppResult<usize> {
        Ok(usize::try_from(self.monitors().count_documents(doc! {}).await?)?)
    }

    async fn create_monitor(&self, input: NewMonitor) -> AppResult<MonitorRecord> {
        let (address, kind, signature_hash, params) = match &input.target {
            Target::Call(target) => (
                format!("{:#x}", target.address),
                "call",
                format!("{:#x}", target.selector),
                &target.inputs,
            ),
            Target::Event(target) => (
                format!("{:#x}", target.address),
                "event",
                format!("{:#x}", target.topic0),
                &target.params,
            ),
        };
        let now = bson::DateTime::now();
        let template = MonitorDocument {
            id: 1,
            chain_id: to_i64(input.chain.id, "chain id")?,
            address,
            kind: kind.into(),
            signature_hash,
            param_schema: params
                .iter()
                .map(|param| StoredParam {
                    name: param.name.clone(),
                    sol_type: param.sol_type(),
                    indexed: param.indexed,
                })
                .collect(),
            start_block: to_i64(input.start_block, "start block")?,
            end_block: input.end_block.map(|value| to_i64(value, "end block")).transpose()?,
            cursor: None,
            completed: false,
            enabled: true,
            filter_ast: input.filter.as_ref().map(|filter| filter.expression.clone()),
            filter_version: input.filter.as_ref().map(|filter| filter.version),
            created_at: now,
            updated_at: now,
        };
        let chains = self.chains();
        let counters = self.counters();
        let monitors = self.monitors();
        let row = self
            .transaction(move |session| {
                let chains = chains.clone();
                let counters = counters.clone();
                let monitors = monitors.clone();
                let mut row = template.clone();
                Box::pin(async move {
                    let owner = chains
                        .find_one(doc! { "chain_id": row.chain_id })
                        .session(&mut *session)
                        .await?;
                    anyhow::ensure!(owner.is_some(), "chain {} not found", row.chain_id);
                    let counter = counters
                        .find_one_and_update(
                            doc! { "_id": "monitors" },
                            doc! { "$inc": { "value": 1_i64 } },
                        )
                        .upsert(true)
                        .return_document(ReturnDocument::After)
                        .session(&mut *session)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("monitor counter update returned no row"))?;
                    row.id = counter.get_i64("value")?;
                    drop(monitors.insert_one(&row).session(&mut *session).await?);
                    Ok(row)
                })
            })
            .await?;
        Self::monitor_record(row)
    }

    async fn list_monitors(&self, chain: Option<Chain>) -> AppResult<Vec<MonitorRecord>> {
        let filter = match chain {
            Some(chain) => doc! { "chain_id": to_i64(chain.id, "chain id")? },
            None => doc! {},
        };
        let mut rows = self.monitors().find(filter).sort(doc! { "id": 1 }).await?;
        let mut result = Vec::new();
        while let Some(row) = rows.try_next().await? {
            result.push(Self::monitor_record(row)?);
        }
        Ok(result)
    }

    async fn get_monitor(&self, id: MonitorId) -> AppResult<MonitorRecord> {
        let id = to_i64(id.get(), "monitor id")?;
        let row = self
            .monitors()
            .find_one(doc! { "id": id })
            .await?
            .ok_or_else(|| anyhow::anyhow!("monitor {id} not found"))?;
        Self::monitor_record(row)
    }

    async fn set_monitor_enabled(&self, id: MonitorId, enabled: bool) -> AppResult<MonitorRecord> {
        let id = to_i64(id.get(), "monitor id")?;
        let row = self
            .monitors()
            .find_one_and_update(
                doc! { "id": id },
                doc! { "$set": { "enabled": enabled, "updated_at": bson::DateTime::now() } },
            )
            .return_document(ReturnDocument::After)
            .await?
            .ok_or_else(|| anyhow::anyhow!("monitor {id} not found"))?;
        Self::monitor_record(row)
    }

    async fn delete_monitor(&self, id: MonitorId) -> AppResult<()> {
        let id = to_i64(id.get(), "monitor id")?;
        let monitors = self.monitors();
        let results = self.results();
        self.transaction(move |session| {
            let monitors = monitors.clone();
            let results = results.clone();
            Box::pin(async move {
                let deleted = monitors.delete_one(doc! { "id": id }).session(&mut *session).await?;
                anyhow::ensure!(deleted.deleted_count == 1, "monitor {id} not found");
                let _ =
                    results.delete_many(doc! { "monitor_id": id }).session(&mut *session).await?;
                Ok(())
            })
        })
        .await
    }
}

#[async_trait]
impl ResultRepository for MongoStorage {
    async fn query_results(
        &self,
        monitor: &MonitorRecord,
        query: ResultQuery,
    ) -> AppResult<Vec<ResultRecord>> {
        let kind = match monitor.target {
            Target::Call(_) => "call",
            Target::Event(_) => "event",
        };
        let (mut filter, sort) = result_query(to_i64(monitor.id.get(), "monitor id")?, kind)?;
        if let Some(finality) = query.finality {
            filter.insert("finality", finality.as_str());
        }
        let mut rows = self
            .results()
            .find(filter)
            .sort(sort)
            .skip(query.offset)
            .limit(i64::from(query.limit.get()))
            .await?;
        let emitter = match &monitor.target {
            Target::Event(target) => target.address,
            Target::Call(_) => Address::ZERO,
        };
        let mut result = Vec::new();
        while let Some(row) = rows.try_next().await? {
            let tx_hash = TxHash::from_str(&row.tx_hash)?;
            let block_number = from_i64(row.block_number, "block number")?;
            let block_hash = B256::from_str(&row.block_hash)?;
            let finality = finality_from_str(&row.finality)?;
            let params = bson_params_to_json(row.params)?;
            result.push(if kind == "call" {
                ResultRecord::Call {
                    tx_hash,
                    block_hash,
                    block_number,
                    from: Address::from_str(
                        row.from
                            .as_deref()
                            .ok_or_else(|| anyhow::anyhow!("missing call sender"))?,
                    )?,
                    to: Address::from_str(
                        row.to
                            .as_deref()
                            .ok_or_else(|| anyhow::anyhow!("missing call recipient"))?,
                    )?,
                    finality,
                    params,
                }
            } else {
                ResultRecord::Event {
                    tx_hash,
                    block_hash,
                    log_index: from_i64(
                        row.log_index.ok_or_else(|| anyhow::anyhow!("missing log index"))?,
                        "log index",
                    )?,
                    block_number,
                    emitter,
                    finality,
                    params,
                }
            });
        }
        Ok(result)
    }
}

fn result_document(
    chain_id: i64,
    finality: Finality,
    monitors: &HashMap<MonitorId, &Monitor>,
    result: &DecodedResult,
) -> AppResult<ResultDocument> {
    match result {
        DecodedResult::Call(call) => {
            let monitor = monitors
                .get(&call.monitor_id)
                .ok_or_else(|| anyhow::anyhow!("monitor {} was not locked", call.monitor_id))?;
            let Target::Call(target) = &monitor.target else {
                anyhow::bail!("call result references event monitor")
            };
            Ok(ResultDocument {
                chain_id,
                monitor_id: to_i64(call.monitor_id.get(), "monitor id")?,
                kind: "call".into(),
                tx_hash: format!("{:#x}", call.transaction_hash),
                block_hash: format!("{:#x}", call.block_hash),
                log_index: None,
                block_number: to_i64(call.block_number, "block number")?,
                from: Some(format!("{:#x}", call.from)),
                to: Some(format!("{:#x}", call.to)),
                finality: finality.as_str().into(),
                params: bson_params(&target.inputs, &call.params)?,
            })
        }
        DecodedResult::Event(event) => {
            let monitor = monitors
                .get(&event.monitor_id)
                .ok_or_else(|| anyhow::anyhow!("monitor {} was not locked", event.monitor_id))?;
            let Target::Event(target) = &monitor.target else {
                anyhow::bail!("event result references call monitor")
            };
            Ok(ResultDocument {
                chain_id,
                monitor_id: to_i64(event.monitor_id.get(), "monitor id")?,
                kind: "event".into(),
                tx_hash: format!("{:#x}", event.transaction_hash),
                block_hash: format!("{:#x}", event.block_hash),
                log_index: Some(to_i64(event.log_index, "log index")?),
                block_number: to_i64(event.block_number, "block number")?,
                from: None,
                to: None,
                finality: finality.as_str().into(),
                params: bson_params(&target.params, &event.params)?,
            })
        }
    }
}

fn bson_params(schema: &[AbiParam], values: &[DecodedValue]) -> AppResult<Document> {
    anyhow::ensure!(schema.len() == values.len(), "parameter count mismatch");
    Ok(schema
        .iter()
        .zip(values)
        .map(|(param, value)| {
            let value = match value {
                DecodedValue::Uint(value) => Bson::String(value.to_string()),
                DecodedValue::Int(value) => Bson::String(value.to_string()),
                DecodedValue::Bool(value) => Bson::Boolean(*value),
                DecodedValue::Address(value) => Bson::String(format!("{value:#x}")),
                DecodedValue::String(value) => Bson::String(value.clone()),
                DecodedValue::Bytes(value) => Bson::Binary(Binary {
                    subtype: bson::spec::BinarySubtype::Generic,
                    bytes: value.to_vec(),
                }),
            };
            (param.name.clone(), value)
        })
        .collect())
}

fn bson_params_to_json(params: Document) -> AppResult<serde_json::Value> {
    Ok(serde_json::Value::Object(
        params
            .into_iter()
            .map(|(name, value)| {
                let value = match value {
                    Bson::String(value) => serde_json::Value::String(value),
                    Bson::Boolean(value) => serde_json::Value::Bool(value),
                    Bson::Binary(value) => {
                        serde_json::Value::String(format!("0x{}", alloy::hex::encode(value.bytes)))
                    }
                    value => anyhow::bail!("invalid stored parameter value {value:?}"),
                };
                Ok((name, value))
            })
            .collect::<AppResult<_>>()?,
    ))
}

fn indexes() -> Vec<(&'static str, IndexModel)> {
    let index =
        |keys: Document, name: &str, unique: bool, partial_filter_expression: Option<Document>| {
            IndexModel::builder()
                .keys(keys)
                .options(Some(
                    IndexOptions::builder()
                        .name(Some(name.into()))
                        .unique(unique.then_some(true))
                        .partial_filter_expression(partial_filter_expression)
                        .build(),
                ))
                .build()
        };
    vec![
        ("chains", index(doc! { "chain_id": 1 }, "chains_chain_id", true, None)),
        ("monitors", index(doc! { "id": 1 }, "monitors_id", true, None)),
        (
            "monitors",
            index(
                doc! { "chain_id": 1, "kind": 1, "address": 1, "signature_hash": 1 },
                MONITOR_TARGET_LOOKUP_INDEX,
                false,
                None,
            ),
        ),
        ("monitors", index(doc! { "chain_id": 1, "id": 1 }, "monitors_chain_order", false, None)),
        (
            "canonical_blocks",
            index(doc! { "chain_id": 1, "block_number": 1 }, "blocks_chain_number", true, None),
        ),
        (
            "canonical_blocks",
            index(doc! { "chain_id": 1, "block_hash": 1 }, "blocks_chain_hash", true, None),
        ),
        (
            "results",
            index(
                doc! { "monitor_id": 1, "tx_hash": 1, "block_hash": 1 },
                "results_call_identity",
                true,
                Some(doc! { "kind": "call" }),
            ),
        ),
        (
            "results",
            index(
                doc! { "monitor_id": 1, "tx_hash": 1, "block_hash": 1, "log_index": 1 },
                "results_event_identity",
                true,
                Some(doc! { "kind": "event" }),
            ),
        ),
        (
            "results",
            index(
                doc! { "monitor_id": 1, "block_number": -1, "block_hash": -1, "tx_hash": -1 },
                "results_call_order",
                false,
                Some(doc! { "kind": "call" }),
            ),
        ),
        (
            "results",
            index(
                doc! { "monitor_id": 1, "block_number": -1, "block_hash": -1, "log_index": -1 },
                "results_event_order",
                false,
                Some(doc! { "kind": "event" }),
            ),
        ),
    ]
}

fn missing_index(error: &mongodb::error::Error) -> bool {
    matches!(
        error.kind.as_ref(),
        ErrorKind::Command(error)
            if matches!(error.code, NAMESPACE_NOT_FOUND | INDEX_NOT_FOUND)
    )
}

fn finality_from_str(value: &str) -> AppResult<Finality> {
    match value {
        "provisional" => Ok(Finality::Provisional),
        "finalized" => Ok(Finality::Finalized),
        value => anyhow::bail!("invalid result finality {value}"),
    }
}

fn result_query(monitor_id: i64, kind: &str) -> AppResult<(Document, Document)> {
    let sort = match kind {
        "call" => doc! { "block_number": -1, "block_hash": -1, "tx_hash": -1 },
        "event" => doc! { "block_number": -1, "block_hash": -1, "log_index": -1 },
        _ => anyhow::bail!("invalid monitor kind {kind}"),
    };
    Ok((doc! { "monitor_id": monitor_id, "kind": kind }, sort))
}

fn transient(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<mongodb::error::Error>()
        .is_some_and(|error| error.contains_label("TransientTransactionError"))
}

fn supports_transactions(hello: &Document) -> bool {
    hello.get_str("setName").is_ok() || hello.get_str("msg") == Ok("isdbgrid")
}

fn to_i64(value: u64, name: &str) -> AppResult<i64> {
    i64::try_from(value).map_err(|_| anyhow::anyhow!("{name} exceeds MongoDB integer range"))
}

fn from_i64(value: i64, name: &str) -> AppResult<u64> {
    u64::try_from(value).map_err(|_| anyhow::anyhow!("{name} must not be negative"))
}

fn chrono(value: bson::DateTime) -> AppResult<DateTime<Utc>> {
    DateTime::from_timestamp_millis(value.timestamp_millis())
        .ok_or_else(|| anyhow::anyhow!("invalid MongoDB timestamp"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy::primitives::{I256, U256};
    use parseon_core::commands::{PageLimit, ResultQuery};
    use parseon_core::{DecodedCall, DecodedEvent};

    use super::*;
    fn param(name: &str) -> AbiParam {
        AbiParam::new(name, parse_abi_type("uint256").unwrap()).unwrap()
    }

    fn metadata(number: u64) -> BlockMetadata {
        BlockMetadata {
            number,
            hash: B256::repeat_byte(number as u8),
            parent_hash: B256::ZERO,
            timestamp: 0,
        }
    }

    #[test]
    fn bson_values_round_trip_to_canonical_json() {
        let schema = [
            param("uint"),
            AbiParam::new("int", parse_abi_type("int256").unwrap()).unwrap(),
            AbiParam::new("flag", parse_abi_type("bool").unwrap()).unwrap(),
            AbiParam::new("owner", parse_abi_type("address").unwrap()).unwrap(),
            AbiParam::new("label", parse_abi_type("string").unwrap()).unwrap(),
            AbiParam::new("data", parse_abi_type("bytes").unwrap()).unwrap(),
        ];
        let values = [
            DecodedValue::Uint(U256::from(42)),
            DecodedValue::Int(I256::try_from(-7).unwrap()),
            DecodedValue::Bool(true),
            DecodedValue::Address(Address::repeat_byte(1)),
            DecodedValue::String("hello".into()),
            DecodedValue::Bytes(vec![0xde, 0xad].into()),
        ];
        let stored = bson_params(&schema, &values).unwrap();
        assert!(matches!(stored.get("data"), Some(Bson::Binary(_))));
        assert_eq!(
            bson_params_to_json(stored).unwrap(),
            serde_json::json!({
                "uint": "42",
                "int": "-7",
                "flag": true,
                "owner": format!("{:#x}", Address::repeat_byte(1)),
                "label": "hello",
                "data": "0xdead"
            })
        );
    }

    #[test]
    fn declares_non_unique_target_lookup_and_unique_result_indexes() {
        let indexes = indexes();
        assert_eq!(indexes.len(), 10);
        let names = indexes
            .iter()
            .map(|(_, index)| index.options.as_ref().unwrap().name.as_deref().unwrap())
            .collect::<Vec<_>>();
        assert!(!names.contains(&LEGACY_MONITOR_TARGET_INDEX));
        assert!(names.contains(&MONITOR_TARGET_LOOKUP_INDEX));
        assert!(names.contains(&"results_call_identity"));
        assert!(names.contains(&"results_event_identity"));
        assert!(names.contains(&"results_call_order"));
        assert!(names.contains(&"results_event_order"));
        let target_lookup = indexes
            .iter()
            .map(|(_, index)| index)
            .find(|index| {
                index.options.as_ref().unwrap().name.as_deref() == Some(MONITOR_TARGET_LOOKUP_INDEX)
            })
            .unwrap();
        assert_eq!(
            target_lookup.keys,
            doc! { "chain_id": 1, "kind": 1, "address": 1, "signature_hash": 1 }
        );
        assert_eq!(target_lookup.options.as_ref().unwrap().unique, None);
        for name in ["results_call_identity", "results_event_identity"] {
            let identity = indexes
                .iter()
                .map(|(_, index)| index)
                .find(|index| index.options.as_ref().unwrap().name.as_deref() == Some(name))
                .unwrap();
            assert_eq!(identity.options.as_ref().unwrap().unique, Some(true));
        }
        assert_eq!(
            result_query(7, "call").unwrap(),
            (
                doc! { "monitor_id": 7_i64, "kind": "call" },
                doc! { "block_number": -1, "block_hash": -1, "tx_hash": -1 }
            )
        );
        assert_eq!(
            result_query(8, "event").unwrap().1,
            doc! { "block_number": -1, "block_hash": -1, "log_index": -1 }
        );
        assert!(result_query(7, "unknown").is_err());
    }

    #[test]
    fn accepts_only_transaction_capable_topologies() {
        assert!(supports_transactions(&doc! { "setName": "rs0" }));
        assert!(supports_transactions(&doc! { "msg": "isdbgrid" }));
        assert!(!supports_transactions(&doc! { "isWritablePrimary": true }));
    }

    #[tokio::test]
    #[ignore = "requires `docker compose --profile mongodb up -d`"]
    async fn compose_crud_duplicate_targets_transactions_ordering_pagination_and_cascades() {
        let url: Url = std::env::var("MONGODB_TEST_URL")
            .unwrap_or_else(|_| "mongodb://localhost:27017/?replicaSet=rs0".into())
            .parse()
            .unwrap();
        let database = format!(
            "parseon_test_{}_{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        );
        let legacy_client = Client::with_uri_str(url.as_str()).await.unwrap();
        legacy_client
            .database(&database)
            .collection::<Document>("monitors")
            .create_index(
                IndexModel::builder()
                    .keys(doc! {
                        "chain_id": 1,
                        "kind": 1,
                        "address": 1,
                        "signature_hash": 1
                    })
                    .options(
                        IndexOptions::builder()
                            .name(Some(LEGACY_MONITOR_TARGET_INDEX.into()))
                            .unique(Some(true))
                            .build(),
                    )
                    .build(),
            )
            .await
            .unwrap();
        drop(legacy_client);
        let storage = MongoStorage::connect(&url, &database).await.unwrap();
        let index_names = storage.monitors().list_index_names().await.unwrap();
        assert!(!index_names.iter().any(|name| name == LEGACY_MONITOR_TARGET_INDEX));
        assert!(index_names.iter().any(|name| name == MONITOR_TARGET_LOOKUP_INDEX));
        let chain = Chain::new(8453);
        let rpc_url: Url = "http://localhost:4000/main/evm/8453".parse().unwrap();
        storage
            .create_chain(NewChain { chain, rpc_url: rpc_url.clone(), enabled: true })
            .await
            .unwrap();
        assert!(
            storage
                .create_chain(NewChain { chain, rpc_url: rpc_url.clone(), enabled: true })
                .await
                .is_err()
        );
        assert_eq!(storage.list_chains().await.unwrap().len(), 1);
        assert!(
            !storage
                .update_chain(chain, ChainUpdate { rpc_url: None, enabled: Some(false) })
                .await
                .unwrap()
                .enabled
        );

        let target = Target::Call(CallTarget {
            address: Address::repeat_byte(1),
            selector: Selector::repeat_byte(2),
            inputs: Vec::new(),
        });
        let input = NewMonitor {
            chain,
            target: target.clone(),
            start_block: 10,
            end_block: None,
            filter: None,
        };
        let monitor = storage.create_monitor(input.clone()).await.unwrap();
        assert_eq!(monitor.id.get(), 1);
        let duplicate_monitor = storage.create_monitor(input).await.unwrap();
        assert_eq!(duplicate_monitor.id.get(), 2);
        let event = storage
            .create_monitor(NewMonitor {
                chain,
                target: Target::Event(EventTarget {
                    address: Address::repeat_byte(3),
                    topic0: B256::repeat_byte(4),
                    params: Vec::new(),
                }),
                start_block: 10,
                end_block: None,
                filter: None,
            })
            .await
            .unwrap();
        assert_eq!(event.id.get(), 3);

        let mut runtime_monitors = storage
            .load_monitors(chain)
            .await
            .unwrap()
            .into_iter()
            .map(|monitor| (monitor.id, Arc::new(monitor)))
            .collect::<HashMap<_, _>>();
        let runtime_monitor = runtime_monitors.remove(&monitor.id).unwrap();
        let runtime_duplicate = runtime_monitors.remove(&duplicate_monitor.id).unwrap();
        let call = |monitor_id, hash: u8, block_number| {
            DecodedResult::Call(DecodedCall {
                monitor_id,
                block_hash: metadata(block_number).hash,
                block_number,
                transaction_hash: B256::repeat_byte(hash),
                from: Address::repeat_byte(5),
                to: Address::repeat_byte(1),
                params: Vec::new(),
            })
        };
        for (hash, block_number) in [(1, 10), (2, 11), (3, 12)] {
            let mut monitors = vec![Arc::clone(&runtime_monitor)];
            let mut results = vec![call(monitor.id, hash, block_number)];
            if block_number == 10 {
                monitors.push(Arc::clone(&runtime_duplicate));
                results.push(call(duplicate_monitor.id, hash, block_number));
            }
            let commit = BlockCommit {
                chain,
                metadata: metadata(block_number),
                finality: Finality::Provisional,
                monitors,
                results,
            };
            storage.commit_block(&commit).await.unwrap();
        }
        assert_eq!(storage.get_monitor(monitor.id).await.unwrap().cursor, Some(12));
        assert_eq!(storage.get_monitor(duplicate_monitor.id).await.unwrap().cursor, Some(10));
        let duplicate_commit = BlockCommit {
            chain,
            metadata: metadata(13),
            finality: Finality::Provisional,
            monitors: vec![Arc::clone(&runtime_monitor)],
            results: vec![call(monitor.id, 3, 12)],
        };
        assert!(storage.commit_block(&duplicate_commit).await.is_err());
        assert_eq!(
            storage.get_monitor(monitor.id).await.unwrap().cursor,
            Some(12),
            "duplicate result must roll back cursor advancement"
        );
        let page = storage
            .query_results(
                &monitor,
                ResultQuery { limit: PageLimit::new(1), offset: 1, finality: None },
            )
            .await
            .unwrap();
        assert!(matches!(page.as_slice(), [ResultRecord::Call { block_number: 11, .. }]));
        assert!(matches!(
            storage
                .query_results(
                    &duplicate_monitor,
                    ResultQuery { limit: PageLimit::new(1), offset: 0, finality: None }
                )
                .await
                .unwrap()
                .as_slice(),
            [ResultRecord::Call { block_number: 10, .. }]
        ));
        assert!(!storage.set_monitor_enabled(duplicate_monitor.id, false).await.unwrap().enabled);
        assert!(storage.get_monitor(monitor.id).await.unwrap().enabled);
        assert!(
            storage
                .load_monitors(chain)
                .await
                .unwrap()
                .iter()
                .any(|monitor| monitor.id == duplicate_monitor.id && !monitor.enabled)
        );
        assert!(storage.set_monitor_enabled(duplicate_monitor.id, true).await.unwrap().enabled);
        let cross_chain_commit = BlockCommit {
            chain: Chain::new(1),
            metadata: metadata(13),
            finality: Finality::Provisional,
            monitors: vec![Arc::clone(&runtime_monitor)],
            results: Vec::new(),
        };
        assert!(storage.commit_block(&cross_chain_commit).await.is_err());

        let deleting = storage.clone();
        let committing = storage.clone();
        let concurrent_commit = BlockCommit {
            chain,
            metadata: metadata(13),
            finality: Finality::Provisional,
            monitors: vec![Arc::clone(&runtime_monitor)],
            results: vec![call(monitor.id, 4, 13)],
        };
        let (deleted, committed) = tokio::join!(
            deleting.delete_monitor(monitor.id),
            committing.commit_block(&concurrent_commit)
        );
        deleted.unwrap();
        drop(committed);
        assert_eq!(
            storage
                .results()
                .count_documents(doc! { "monitor_id": i64::try_from(monitor.id.get()).unwrap() })
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            storage
                .results()
                .count_documents(
                    doc! { "monitor_id": i64::try_from(duplicate_monitor.id.get()).unwrap() }
                )
                .await
                .unwrap(),
            1
        );
        assert!(storage.get_monitor(duplicate_monitor.id).await.is_ok());

        let runtime_event = Arc::new(
            storage
                .load_monitors(chain)
                .await
                .unwrap()
                .into_iter()
                .find(|row| row.id == event.id)
                .unwrap(),
        );
        let event_commit = BlockCommit {
            chain,
            metadata: metadata(14),
            finality: Finality::Provisional,
            monitors: vec![runtime_event],
            results: [1, 2]
                .into_iter()
                .map(|log_index| {
                    DecodedResult::Event(DecodedEvent {
                        monitor_id: event.id,
                        block_hash: metadata(14).hash,
                        block_number: 14,
                        transaction_hash: B256::repeat_byte(9),
                        log_index,
                        params: Vec::new(),
                    })
                })
                .collect(),
        };
        storage.commit_block(&event_commit).await.unwrap();
        assert!(matches!(
            storage
                .query_results(
                    &event,
                    ResultQuery { limit: PageLimit::new(1), offset: 0, finality: None }
                )
                .await
                .unwrap()
                .as_slice(),
            [ResultRecord::Event { log_index: 2, .. }]
        ));
        assert_eq!(storage.results().count_documents(doc! {}).await.unwrap(), 3);

        let promoted = storage.promote_finalized(chain, 11).await.unwrap();
        assert!(!promoted.is_empty(), "promotion must reconstruct finalized sink batches");
        assert!(
            storage
                .query_results(
                    &duplicate_monitor,
                    ResultQuery {
                        limit: PageLimit::new(10),
                        offset: 0,
                        finality: Some(Finality::Finalized),
                    },
                )
                .await
                .unwrap()
                .iter()
                .all(|result| matches!(
                    result,
                    ResultRecord::Call { finality: Finality::Finalized, .. }
                ))
        );

        let before_rejected_rollback = storage.results().count_documents(doc! {}).await.unwrap();
        assert!(storage.rollback_to(chain, 10).await.is_err());
        assert_eq!(
            storage.results().count_documents(doc! {}).await.unwrap(),
            before_rejected_rollback,
            "rollback across finalized data must be atomic"
        );
        storage.rollback_to(chain, 12).await.unwrap();
        assert_eq!(
            storage
                .results()
                .count_documents(doc! { "block_number": { "$gt": 12 } })
                .await
                .unwrap(),
            0,
            "rollback must remove provisional fork results"
        );
        storage.delete_chain(chain).await.unwrap();
        assert_eq!(storage.count_monitors().await.unwrap(), 0);
        assert_eq!(storage.results().count_documents(doc! {}).await.unwrap(), 0);
        storage.database.drop().await.unwrap();
    }
}
