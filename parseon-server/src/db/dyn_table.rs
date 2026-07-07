use std::fmt::{self, Display};
use std::str::FromStr;

use alloy::dyn_abi::DynSolType;
use sqlx::postgres::PgRow;
use sqlx::types::BigDecimal;
use sqlx::{PgConnection, PgPool, QueryBuilder, Row, Transaction};

use parseon_core::DecodedValue;
use parseon_core::abi::parse_abi_type;
use crate::db::monitor_repo::StoredParam;
use crate::error::{AppError, AppResult};

const CALL_COLUMNS: &[(&str, &str)] = &[
    ("tx_hash", "TEXT NOT NULL PRIMARY KEY"),
    ("block_number", "BIGINT NOT NULL"),
];
const EVENT_COLUMNS: &[(&str, &str)] = &[
    ("tx_hash", "TEXT NOT NULL"),
    ("log_index", "BIGINT NOT NULL"),
    ("block_number", "BIGINT NOT NULL"),
    ("PRIMARY KEY", "(tx_hash, log_index)"),
];
const CALL_RESERVED: &[&str] = &["tx_hash", "block_number"];
const EVENT_RESERVED: &[&str] = &["tx_hash", "log_index", "block_number"];

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
            DynSolType::Address | DynSolType::String => Self::Text,
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
        _ => return Err(AppError::Internal(anyhow::anyhow!("invalid monitor kind"))),
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
            Ok(PgParam {
                name: p.name.clone(),
                column: col,
                kind: PgColumnType::from_param(p)?,
            })
        })
        .collect()
}

pub fn result_table_name(id: i64) -> AppResult<String> {
    if id <= 0 {
        return Err(AppError::Internal(anyhow::anyhow!("invalid monitor id")));
    }
    Ok(format!("monitor_{id}_results"))
}

pub async fn create_result_table(
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
        _ => return Err(AppError::Internal(anyhow::anyhow!("invalid monitor kind"))),
    };
    let mut qb = QueryBuilder::new("CREATE TABLE ");
    qb.push(table.clone()).push(" (");
    let mut separated = qb.separated(", ");
    for (name, ty) in cols {
        separated
            .push(*name)
            .push_unseparated(" ")
            .push_unseparated(*ty);
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
pub async fn drop_result_table(tx: &mut Transaction<'_, sqlx::Postgres>, id: i64) -> AppResult<()> {
    QueryBuilder::new("DROP TABLE IF EXISTS ")
        .push(Identifier::new(result_table_name(id)?)?)
        .push(" CASCADE")
        .build()
        .execute(&mut **tx)
        .await?;
    Ok(())
}
pub async fn truncate_result_table(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    id: i64,
) -> AppResult<()> {
    QueryBuilder::new("TRUNCATE TABLE ")
        .push(Identifier::new(result_table_name(id)?)?)
        .build()
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub struct CallResultInput {
    pub tx_hash: String,
    pub block_number: i64,
    pub params: Vec<DecodedValue>,
}
pub struct EventResultInput {
    pub tx_hash: String,
    pub log_index: i64,
    pub block_number: i64,
    pub params: Vec<DecodedValue>,
}

fn push_values(qb: &mut QueryBuilder<sqlx::Postgres>, values: &[DecodedValue]) -> AppResult<()> {
    for v in values {
        qb.push(", ");
        match v {
            DecodedValue::Uint(v) => qb.push_bind(
                BigDecimal::from_str(&v.to_string()).map_err(|e| AppError::Internal(e.into()))?,
            ),
            DecodedValue::Int(v) => qb.push_bind(
                BigDecimal::from_str(&v.to_string()).map_err(|e| AppError::Internal(e.into()))?,
            ),
            DecodedValue::Bool(v) => qb.push_bind(*v),
            DecodedValue::Address(v) => qb.push_bind(v.to_string()),
            DecodedValue::String(v) => qb.push_bind(v),
            DecodedValue::Bytes(v) => qb.push_bind(v),
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

pub async fn insert_call(
    conn: &mut PgConnection,
    id: i64,
    schema: &[StoredParam],
    input: &CallResultInput,
) -> AppResult<()> {
    let params = postgres_params("call", schema)?;
    if params.len() != input.params.len() {
        return Err(AppError::Internal(anyhow::anyhow!(
            "parameter count mismatch"
        )));
    }
    let mut qb = QueryBuilder::new("INSERT INTO ");
    qb.push(Identifier::new(result_table_name(id)?)?)
        .push(" (tx_hash,block_number");
    push_param_columns(&mut qb, &params)?;
    qb.push(") VALUES (")
        .push_bind(&input.tx_hash)
        .push(",")
        .push_bind(input.block_number);
    push_values(&mut qb, &input.params)?;
    qb.push(") ON CONFLICT DO NOTHING");
    qb.build().execute(conn).await?;
    Ok(())
}
pub async fn insert_event(
    conn: &mut PgConnection,
    id: i64,
    schema: &[StoredParam],
    input: &EventResultInput,
) -> AppResult<()> {
    let params = postgres_params("event", schema)?;
    if params.len() != input.params.len() {
        return Err(AppError::Internal(anyhow::anyhow!(
            "parameter count mismatch"
        )));
    }
    let mut qb = QueryBuilder::new("INSERT INTO ");
    qb.push(Identifier::new(result_table_name(id)?)?)
        .push(" (tx_hash,log_index,block_number");
    push_param_columns(&mut qb, &params)?;
    qb.push(") VALUES (")
        .push_bind(&input.tx_hash)
        .push(",")
        .push_bind(input.log_index)
        .push(",")
        .push_bind(input.block_number);
    push_values(&mut qb, &input.params)?;
    qb.push(") ON CONFLICT DO NOTHING");
    qb.build().execute(conn).await?;
    Ok(())
}

pub struct SearchParams {
    pub limit: i64,
    pub offset: i64,
}
#[derive(Debug)]
pub struct CallResultRecord {
    pub tx_hash: String,
    pub block_number: i64,
    pub params: serde_json::Value,
}
#[derive(Debug)]
pub struct EventResultRecord {
    pub tx_hash: String,
    pub log_index: i64,
    pub block_number: i64,
    pub params: serde_json::Value,
}
#[derive(Debug)]
pub enum ResultRecord {
    Call(CallResultRecord),
    Event(EventResultRecord),
}

fn read_params(row: &PgRow, params: &[PgParam]) -> AppResult<serde_json::Value> {
    let mut map = serde_json::Map::new();
    for p in params {
        let value = match p.kind {
            PgColumnType::Numeric => row
                .try_get::<Option<BigDecimal>, _>(&p.column[..])?
                .map(|v| serde_json::Value::String(v.to_plain_string()))
                .unwrap_or_default(),
            PgColumnType::Bool => serde_json::json!(row.try_get::<Option<bool>, _>(&p.column[..])?),
            PgColumnType::Text => {
                serde_json::json!(row.try_get::<Option<String>, _>(&p.column[..])?)
            }
            PgColumnType::Bytea => row
                .try_get::<Option<Vec<u8>>, _>(&p.column[..])?
                .map(|v| serde_json::Value::String(format!("0x{}", alloy::hex::encode(v))))
                .unwrap_or_default(),
        };
        map.insert(p.name.clone(), value);
    }
    Ok(serde_json::Value::Object(map))
}
pub async fn query_results(
    pool: &PgPool,
    id: i64,
    kind: &str,
    schema: &[StoredParam],
    search: &SearchParams,
) -> AppResult<Vec<ResultRecord>> {
    let params = postgres_params(kind, schema)?;
    let mut qb = QueryBuilder::new("SELECT * FROM ");
    qb.push(Identifier::new(result_table_name(id)?)?);
    if kind == "call" {
        qb.push(" ORDER BY block_number DESC, tx_hash DESC");
    } else {
        qb.push(" ORDER BY block_number DESC, log_index DESC");
    }
    qb.push(" LIMIT ")
        .push_bind(search.limit)
        .push(" OFFSET ")
        .push_bind(search.offset);
    let rows = qb.build().fetch_all(pool).await?;
    rows.into_iter()
        .map(|row| {
            let decoded = read_params(&row, &params)?;
            Ok(if kind == "call" {
                ResultRecord::Call(CallResultRecord {
                    tx_hash: row.try_get("tx_hash")?,
                    block_number: row.try_get("block_number")?,
                    params: decoded,
                })
            } else {
                ResultRecord::Event(EventResultRecord {
                    tx_hash: row.try_get("tx_hash")?,
                    log_index: row.try_get("log_index")?,
                    block_number: row.try_get("block_number")?,
                    params: decoded,
                })
            })
        })
        .collect()
}

#[derive(Debug, Clone)]
struct Identifier(String);
impl Identifier {
    fn new(v: impl Into<String>) -> AppResult<Self> {
        let v = v.into();
        if v.is_empty() || v.contains('\0') {
            return Err(AppError::Internal(anyhow::anyhow!(
                "invalid SQL identifier"
            )));
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
            &[StoredParam {
                name: "label".into(),
                sol_type: "string".into(),
                indexed: true,
            }],
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
        assert_eq!(call[1].column, "from_addr");

        let event = postgres_params("event", &[param("log_index"), param("topics_raw")]).unwrap();
        assert_eq!(event[0].column, "log_index_param");
        assert_eq!(event[1].column, "topics_raw");
    }
}
