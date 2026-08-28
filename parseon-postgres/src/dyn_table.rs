use std::fmt::{self, Display};
use std::str::FromStr;

use alloy::dyn_abi::DynSolType;
use sqlx::postgres::PgRow;
use sqlx::types::BigDecimal;
use sqlx::{PgConnection, PgPool, QueryBuilder, Row, Transaction};

use crate::{monitor_repo::StoredParam, pg_types};
use parseon_core::abi::parse_abi_type;
use parseon_core::commands::PageLimit;
use parseon_core::ports::{CanonicalBlock, SinkBatch, SinkResult};
use parseon_core::{Address, B256, BlockNumber, DecodedValue, Finality, TxHash};
type AppResult<T> = anyhow::Result<T>;
const BIND_LIMIT: usize = u16::MAX as usize;

const CALL_COLUMNS: &[(&str, &str)] = &[
    ("tx_hash", "BYTEA NOT NULL CHECK (octet_length(tx_hash) = 32)"),
    ("block_hash", "BYTEA NOT NULL CHECK (octet_length(block_hash) = 32)"),
    ("block_number", "BIGINT NOT NULL CHECK (block_number >= 0)"),
    ("from_addr", "BYTEA NOT NULL CHECK (octet_length(from_addr) = 20)"),
    ("to_addr", "BYTEA NOT NULL CHECK (octet_length(to_addr) = 20)"),
    ("finality", "TEXT NOT NULL CHECK (finality IN ('provisional', 'finalized'))"),
    ("PRIMARY KEY", "(tx_hash, block_hash)"),
];
const EVENT_COLUMNS: &[(&str, &str)] = &[
    ("tx_hash", "BYTEA NOT NULL CHECK (octet_length(tx_hash) = 32)"),
    ("block_hash", "BYTEA NOT NULL CHECK (octet_length(block_hash) = 32)"),
    ("log_index", "BIGINT NOT NULL CHECK (log_index >= 0)"),
    ("block_number", "BIGINT NOT NULL CHECK (block_number >= 0)"),
    ("finality", "TEXT NOT NULL CHECK (finality IN ('provisional', 'finalized'))"),
    ("PRIMARY KEY", "(tx_hash, block_hash, log_index)"),
];
const CALL_RESERVED: &[&str] =
    &["tx_hash", "block_hash", "block_number", "from_addr", "to_addr", "finality"];
const EVENT_RESERVED: &[&str] = &["tx_hash", "block_hash", "log_index", "block_number", "finality"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgColumnType {
    Numeric,
    Bool,
    Text,
    Bytea,
}
impl PgColumnType {
    fn from_param(param: &StoredParam) -> AppResult<Self> {
        let ty = parse_abi_type(&param.sol_type)?;
        if param.indexed && matches!(ty, DynSolType::String | DynSolType::Bytes) {
            return Ok(Self::Bytea);
        }
        Ok(match ty {
            DynSolType::Uint(_) | DynSolType::Int(_) => Self::Numeric,
            DynSolType::Bool => Self::Bool,
            DynSolType::Address => Self::Bytea,
            DynSolType::String => Self::Text,
            DynSolType::Bytes | DynSolType::FixedBytes(_) | DynSolType::Function => Self::Bytea,
            _ => unreachable!(),
        })
    }
    fn ddl(self) -> &'static str {
        match self {
            Self::Numeric => "NUMERIC",
            Self::Bool => "BOOLEAN",
            Self::Text => "TEXT",
            Self::Bytea => "BYTEA",
        }
    }
}
#[derive(Debug, Clone)]
struct PgParam {
    name: String,
    column: String,
    kind: PgColumnType,
}
fn postgres_params(kind: &str, params: &[StoredParam]) -> AppResult<Vec<PgParam>> {
    let reserved = match kind {
        "call" => CALL_RESERVED,
        "event" => EVENT_RESERVED,
        _ => anyhow::bail!("invalid monitor kind"),
    };
    let mut used = std::collections::HashSet::new();
    params
        .iter()
        .map(|p| {
            let base = if reserved.contains(&p.name.as_str()) {
                format!("{}_param", p.name)
            } else {
                p.name.clone()
            };
            let mut col = base.clone();
            let mut n = 1;
            while !used.insert(col.clone()) {
                col = format!("{base}_{n}");
                n += 1;
            }
            Ok(PgParam { name: p.name.clone(), column: col, kind: PgColumnType::from_param(p)? })
        })
        .collect()
}

pub(crate) fn result_table_name(id: i64) -> AppResult<String> {
    if id <= 0 {
        anyhow::bail!("invalid monitor id");
    }
    Ok(format!("monitor_{id}_results"))
}

pub(crate) async fn create_result_table(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    id: i64,
    kind: &str,
    params: &[StoredParam],
) -> AppResult<()> {
    let table = Identifier::new(result_table_name(id)?)?;
    let params = postgres_params(kind, params)?;
    let cols = match kind {
        "call" => CALL_COLUMNS,
        "event" => EVENT_COLUMNS,
        _ => anyhow::bail!("invalid monitor kind"),
    };
    let mut qb = QueryBuilder::new("CREATE TABLE ");
    qb.push(table.clone()).push(" (");
    let mut separated = qb.separated(", ");
    for (name, ty) in cols {
        separated.push(*name).push_unseparated(" ").push_unseparated(*ty);
    }
    for p in &params {
        separated
            .push(Identifier::new(&p.column)?)
            .push_unseparated(" ")
            .push_unseparated(p.kind.ddl());
    }
    qb.push(")");
    qb.build().execute(&mut **tx).await?;
    QueryBuilder::new("CREATE INDEX ")
        .push(Identifier::new(format!("monitor_{id}_block_idx"))?)
        .push(" ON ")
        .push(table)
        .push(" (block_number)")
        .build()
        .execute(&mut **tx)
        .await?;
    Ok(())
}
pub(crate) async fn drop_result_table(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    id: i64,
) -> AppResult<()> {
    QueryBuilder::new("DROP TABLE IF EXISTS ")
        .push(Identifier::new(result_table_name(id)?)?)
        .push(" CASCADE")
        .build()
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(crate) struct CallResultInput<'a> {
    pub tx_hash: TxHash,
    pub block_hash: TxHash,
    pub block_number: BlockNumber,
    pub from: parseon_core::Address,
    pub to: parseon_core::Address,
    pub finality: Finality,
    pub params: &'a [DecodedValue],
}
pub(crate) struct EventResultInput<'a> {
    pub tx_hash: TxHash,
    pub block_hash: TxHash,
    pub log_index: u64,
    pub block_number: BlockNumber,
    pub finality: Finality,
    pub params: &'a [DecodedValue],
}

fn push_values(qb: &mut QueryBuilder<sqlx::Postgres>, values: &[DecodedValue]) -> AppResult<()> {
    for v in values {
        qb.push(", ");
        match v {
            DecodedValue::Uint(v) => {
                qb.push_bind(BigDecimal::from_str(&v.to_string()).map_err(anyhow::Error::from)?)
            }
            DecodedValue::Int(v) => {
                qb.push_bind(BigDecimal::from_str(&v.to_string()).map_err(anyhow::Error::from)?)
            }
            DecodedValue::Bool(v) => qb.push_bind(*v),
            DecodedValue::Address(v) => qb.push_bind(v.as_slice()),
            DecodedValue::String(v) => qb.push_bind(v),
            DecodedValue::Bytes(v) => qb.push_bind(v.as_ref()),
        };
    }
    Ok(())
}
fn push_param_columns(qb: &mut QueryBuilder<sqlx::Postgres>, params: &[PgParam]) -> AppResult<()> {
    for p in params {
        qb.push(", ").push(Identifier::new(&p.column)?);
    }
    Ok(())
}

pub(crate) async fn insert_calls(
    conn: &mut PgConnection,
    id: i64,
    schema: &[StoredParam],
    inputs: &[CallResultInput<'_>],
) -> AppResult<()> {
    let params = postgres_params("call", schema)?;
    anyhow::ensure!(
        inputs.iter().all(|input| params.len() == input.params.len()),
        "parameter count mismatch"
    );
    let width = params.len() + 6;
    anyhow::ensure!(width <= BIND_LIMIT, "result row exceeds PostgreSQL bind limit");
    let table = Identifier::new(result_table_name(id)?)?;
    for chunk in inputs.chunks(BIND_LIMIT / width) {
        let mut qb = QueryBuilder::new("INSERT INTO ");
        qb.push(table.clone()).push(" (tx_hash,block_hash,block_number,from_addr,to_addr,finality");
        push_param_columns(&mut qb, &params)?;
        qb.push(") VALUES ");
        for (index, input) in chunk.iter().enumerate() {
            if index > 0 {
                qb.push(",");
            }
            qb.push("(")
                .push_bind(input.tx_hash.as_slice())
                .push(",")
                .push_bind(input.block_hash.as_slice())
                .push(",")
                .push_bind(pg_types::to_i64(input.block_number, "block number")?)
                .push(",")
                .push_bind(input.from.as_slice())
                .push(",")
                .push_bind(input.to.as_slice())
                .push(",")
                .push_bind(input.finality.as_str());
            push_values(&mut qb, input.params)?;
            qb.push(")");
        }
        let inserted = qb.build().persistent(false).execute(&mut *conn).await?.rows_affected();
        anyhow::ensure!(inserted == u64::try_from(chunk.len())?, "result row count mismatch");
    }
    Ok(())
}
pub(crate) async fn insert_events(
    conn: &mut PgConnection,
    id: i64,
    schema: &[StoredParam],
    inputs: &[EventResultInput<'_>],
) -> AppResult<()> {
    let params = postgres_params("event", schema)?;
    anyhow::ensure!(
        inputs.iter().all(|input| params.len() == input.params.len()),
        "parameter count mismatch"
    );
    let width = params.len() + 5;
    anyhow::ensure!(width <= BIND_LIMIT, "result row exceeds PostgreSQL bind limit");
    let table = Identifier::new(result_table_name(id)?)?;
    for chunk in inputs.chunks(BIND_LIMIT / width) {
        let mut qb = QueryBuilder::new("INSERT INTO ");
        qb.push(table.clone()).push(" (tx_hash,block_hash,log_index,block_number,finality");
        push_param_columns(&mut qb, &params)?;
        qb.push(") VALUES ");
        for (index, input) in chunk.iter().enumerate() {
            if index > 0 {
                qb.push(",");
            }
            qb.push("(")
                .push_bind(input.tx_hash.as_slice())
                .push(",")
                .push_bind(input.block_hash.as_slice())
                .push(",")
                .push_bind(pg_types::to_i64(input.log_index, "log index")?)
                .push(",")
                .push_bind(pg_types::to_i64(input.block_number, "block number")?)
                .push(",")
                .push_bind(input.finality.as_str());
            push_values(&mut qb, input.params)?;
            qb.push(")");
        }
        let inserted = qb.build().persistent(false).execute(&mut *conn).await?.rows_affected();
        anyhow::ensure!(inserted == u64::try_from(chunk.len())?, "result row count mismatch");
    }
    Ok(())
}

pub(crate) struct SearchParams {
    pub limit: PageLimit,
    pub offset: u64,
    pub finality: Option<Finality>,
}
#[derive(Debug)]
pub(crate) struct CallResultRecord {
    pub tx_hash: TxHash,
    pub block_hash: B256,
    pub block_number: BlockNumber,
    pub from: Address,
    pub to: Address,
    pub finality: Finality,
    pub params: serde_json::Value,
}
#[derive(Debug)]
pub(crate) struct EventResultRecord {
    pub tx_hash: TxHash,
    pub block_hash: B256,
    pub log_index: u64,
    pub block_number: BlockNumber,
    pub emitter: Address,
    pub finality: Finality,
    pub params: serde_json::Value,
}
#[derive(Debug)]
pub(crate) enum ResultRecord {
    Call(CallResultRecord),
    Event(EventResultRecord),
}

fn read_params(row: &PgRow, params: &[PgParam]) -> AppResult<serde_json::Value> {
    let mut map = serde_json::Map::new();
    for p in params {
        let value = match p.kind {
            PgColumnType::Numeric => row
                .try_get::<Option<BigDecimal>, _>(p.column.as_str())?
                .map(|v| serde_json::Value::String(v.to_plain_string()))
                .unwrap_or_default(),
            PgColumnType::Bool => {
                serde_json::json!(row.try_get::<Option<bool>, _>(p.column.as_str())?)
            }
            PgColumnType::Text => {
                serde_json::json!(row.try_get::<Option<String>, _>(p.column.as_str())?)
            }
            PgColumnType::Bytea => row
                .try_get::<Option<Vec<u8>>, _>(p.column.as_str())?
                .map(|v| serde_json::Value::String(format!("0x{}", alloy::hex::encode(v))))
                .unwrap_or_default(),
        };
        map.insert(p.name.clone(), value);
    }
    Ok(serde_json::Value::Object(map))
}
pub(crate) async fn query_results(
    pool: &PgPool,
    id: i64,
    kind: &str,
    schema: &[StoredParam],
    search: &SearchParams,
) -> AppResult<Vec<ResultRecord>> {
    let params = postgres_params(kind, schema)?;
    let mut qb = QueryBuilder::new("SELECT * FROM ");
    qb.push(Identifier::new(result_table_name(id)?)?);
    if let Some(finality) = search.finality {
        qb.push(" WHERE finality = ").push_bind(finality.as_str());
    }
    if kind == "call" {
        qb.push(" ORDER BY block_number DESC, tx_hash DESC");
    } else {
        qb.push(" ORDER BY block_number DESC, log_index DESC");
    }
    qb.push(" LIMIT ")
        .push_bind(i64::from(search.limit.get()))
        .push(" OFFSET ")
        .push_bind(pg_types::to_i64(search.offset, "result offset")?);
    let rows = qb.build().fetch_all(pool).await?;
    rows.into_iter()
        .map(|row| {
            let decoded = read_params(&row, &params)?;
            let finality = match row.try_get::<String, _>("finality")?.as_str() {
                "provisional" => Finality::Provisional,
                "finalized" => Finality::Finalized,
                value => anyhow::bail!("invalid result finality {value}"),
            };
            Ok(if kind == "call" {
                ResultRecord::Call(CallResultRecord {
                    tx_hash: pg_types::b256(
                        &row.try_get::<Vec<u8>, _>("tx_hash")?,
                        "transaction hash",
                    )?,
                    block_hash: pg_types::b256(
                        &row.try_get::<Vec<u8>, _>("block_hash")?,
                        "block hash",
                    )?,
                    block_number: pg_types::from_i64(row.try_get("block_number")?, "block number")?,
                    from: pg_types::address(&row.try_get::<Vec<u8>, _>("from_addr")?)?,
                    to: pg_types::address(&row.try_get::<Vec<u8>, _>("to_addr")?)?,
                    finality,
                    params: decoded,
                })
            } else {
                ResultRecord::Event(EventResultRecord {
                    tx_hash: pg_types::b256(
                        &row.try_get::<Vec<u8>, _>("tx_hash")?,
                        "transaction hash",
                    )?,
                    block_hash: pg_types::b256(
                        &row.try_get::<Vec<u8>, _>("block_hash")?,
                        "block hash",
                    )?,
                    log_index: pg_types::from_i64(row.try_get("log_index")?, "log index")?,
                    block_number: pg_types::from_i64(row.try_get("block_number")?, "block number")?,
                    emitter: Address::ZERO,
                    finality,
                    params: decoded,
                })
            })
        })
        .collect()
}

pub(crate) async fn delete_results_after(
    conn: &mut PgConnection,
    id: i64,
    block_number: BlockNumber,
) -> AppResult<()> {
    QueryBuilder::new("DELETE FROM ")
        .push(Identifier::new(result_table_name(id)?)?)
        .push(" WHERE block_number > ")
        .push_bind(pg_types::to_i64(block_number, "block number")?)
        .build()
        .execute(&mut *conn)
        .await?;
    Ok(())
}

pub(crate) async fn promote_results(
    conn: &mut PgConnection,
    id: i64,
    finalized_head: BlockNumber,
) -> AppResult<()> {
    QueryBuilder::new("UPDATE ")
        .push(Identifier::new(result_table_name(id)?)?)
        .push(" SET finality = 'finalized' WHERE finality = 'provisional' AND block_number <= ")
        .push_bind(pg_types::to_i64(finalized_head, "finalized head")?)
        .build()
        .execute(&mut *conn)
        .await?;
    Ok(())
}

pub(crate) async fn sink_batch_for_block(
    conn: &mut PgConnection,
    id: i64,
    kind: &str,
    schema: &[StoredParam],
    block: &CanonicalBlock,
    emitter: Address,
) -> AppResult<Option<SinkBatch>> {
    let params = postgres_params(kind, schema)?;
    let table = Identifier::new(result_table_name(id)?)?;
    let mut qb = QueryBuilder::new("SELECT * FROM ");
    qb.push(table)
        .push(" WHERE block_hash = ")
        .push_bind(block.metadata.hash.as_slice())
        .push(" AND block_number = ")
        .push_bind(pg_types::to_i64(block.metadata.number, "block number")?)
        .push(" AND finality = 'finalized'");
    let rows = qb.build().fetch_all(&mut *conn).await?;
    if rows.is_empty() {
        return Ok(None);
    }
    let results = rows
        .into_iter()
        .map(|row| {
            let params = read_params(&row, &params)?;
            let tx_hash =
                pg_types::b256(&row.try_get::<Vec<u8>, _>("tx_hash")?, "transaction hash")?;
            if kind == "call" {
                Ok(SinkResult::Call {
                    monitor_id: u64::try_from(id)?,
                    tx_hash,
                    from: pg_types::address(&row.try_get::<Vec<u8>, _>("from_addr")?)?,
                    to: pg_types::address(&row.try_get::<Vec<u8>, _>("to_addr")?)?,
                    params,
                })
            } else {
                Ok(SinkResult::Event {
                    monitor_id: u64::try_from(id)?,
                    tx_hash,
                    emitter,
                    log_index: pg_types::from_i64(row.try_get("log_index")?, "log index")?,
                    params,
                })
            }
        })
        .collect::<AppResult<Vec<_>>>()?;
    Ok(Some(SinkBatch {
        version: 1,
        chain_id: block.chain.id,
        block_number: block.metadata.number,
        results,
    }))
}

#[derive(Debug, Clone)]
struct Identifier(String);
impl Identifier {
    fn new(v: impl Into<String>) -> AppResult<Self> {
        let v = v.into();
        if v.is_empty() || v.contains('\0') {
            anyhow::bail!("invalid SQL identifier");
        }
        Ok(Self(v))
    }
}
impl Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"{}\"", self.0.replace('"', "\"\""))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn table_name_uses_monitor_id() {
        assert_eq!(result_table_name(42).unwrap(), "monitor_42_results");
        assert!(result_table_name(0).is_err());
    }

    #[test]
    fn indexed_dynamic_params_store_topic_hashes_as_bytes() {
        let params = postgres_params(
            "event",
            &[StoredParam { name: "label".into(), sol_type: "string".into(), indexed: true }],
        )
        .unwrap();
        assert_eq!(params[0].kind, PgColumnType::Bytea);
    }

    #[test]
    fn reserves_only_identity_columns_for_each_result_kind() {
        let param = |name: &str| StoredParam {
            name: name.into(),
            sol_type: "uint256".into(),
            indexed: false,
        };
        let call = postgres_params("call", &[param("log_index"), param("from_addr")]).unwrap();
        assert_eq!(call[0].column, "log_index");
        assert_eq!(call[1].column, "from_addr_param");

        let event = postgres_params("event", &[param("log_index"), param("topics_raw")]).unwrap();
        assert_eq!(event[0].column, "log_index_param");
        assert_eq!(event[1].column, "topics_raw");
    }
}
