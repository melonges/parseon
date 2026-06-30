use alloy::hex;
use chrono::{DateTime, Utc};
use sqlx::postgres::PgRow;
use sqlx::types::BigDecimal;
use sqlx::{PgConnection, PgPool, QueryBuilder, Row, Transaction};
use std::str::FromStr;

use crate::abi::{ParamSpec, SqlKind};
use crate::core::DecodedValue;
use crate::error::{AppError, AppResult};

/// Standard transaction-metadata columns every result table carries alongside
/// the decoded ABI parameter columns.
const STANDARD_COLUMNS: &[(&str, &str)] = &[
    ("tx_hash", "TEXT NOT NULL PRIMARY KEY"),
    ("block_number", "BIGINT NOT NULL"),
    ("block_hash", "TEXT NOT NULL"),
    ("from_addr", "TEXT NOT NULL"),
    ("to_addr", "TEXT NOT NULL"),
    ("value", "NUMERIC NOT NULL"),
    ("gas_used", "NUMERIC NOT NULL"),
    ("gas_price", "NUMERIC NOT NULL"),
    ("status", "SMALLINT NOT NULL"),
    ("input_raw", "BYTEA NOT NULL"),
    ("created_at", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"),
];

/// Reserved column names owned by the standard transaction metadata. Decoded
/// ABI params that collide with one of these are renamed so they still appear
/// in the result table (and in API output under their original ABI name).
const RESERVED_COLUMNS: &[&str] = &[
    "tx_hash",
    "block_number",
    "block_hash",
    "from_addr",
    "to_addr",
    "value",
    "gas_used",
    "gas_price",
    "status",
    "input_raw",
    "created_at",
];

/// Return the result table name for a normalized address and selector.
pub fn result_table_name(address: &str, selector: &str) -> AppResult<String> {
    let address = address.strip_prefix("0x").unwrap_or(address);
    let selector = selector.strip_prefix("0x").unwrap_or(selector);
    if address.len() != 40 || !address.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(AppError::Internal(anyhow::anyhow!(
            "invalid normalized monitor address"
        )));
    }
    if selector.len() != 8 || !selector.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(AppError::Internal(anyhow::anyhow!(
            "invalid normalized monitor selector"
        )));
    }
    Ok(format!(
        "{}_{}",
        address.to_ascii_lowercase(),
        selector.to_ascii_lowercase()
    ))
}

/// Rename any decoded-parameter column that collides with a reserved standard
/// column (e.g. `transfer(address to, uint256 value)` → `value` becomes
/// `value_param`). The ABI `name` is preserved so API output is unaffected.
pub fn dedupe_param_columns(params: &mut [ParamSpec]) {
    use std::collections::HashSet;

    let reserved: HashSet<&str> = RESERVED_COLUMNS.iter().copied().collect();
    // Columns already taken by non-colliding params.
    let mut used: HashSet<String> = params
        .iter()
        .filter_map(|p| {
            if reserved.contains(p.column.as_str()) {
                None
            } else {
                Some(p.column.clone())
            }
        })
        .collect();

    for p in params.iter_mut() {
        if !reserved.contains(p.column.as_str()) {
            continue;
        }
        let base = format!("{}_param", p.column);
        let mut final_col = base.clone();
        let mut n = 1;
        while used.contains(&final_col) {
            final_col = format!("{base}_{n}");
            n += 1;
        }
        used.insert(final_col.clone());
        p.column = final_col;
    }
}

/// Create the per-monitor result table: standard transaction metadata columns
/// followed by the decoded ABI parameter columns, plus indexes on block_number
/// and from_addr for the search endpoint.
pub async fn create_result_table(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    address: &str,
    selector: &str,
    params: &[ParamSpec],
) -> AppResult<()> {
    let table = result_table_name(address, selector)?;
    let table_ident = Identifier::new(table.as_str())?;

    let mut qb: QueryBuilder<sqlx::Postgres> = QueryBuilder::new("CREATE TABLE IF NOT EXISTS ");
    qb.push(table_ident.clone()).push(" (");
    {
        // `separated(", ")` prepends the separator before every push except
        // the first; `push_unseparated` keeps a "name type" pair together.
        let mut cols = qb.separated(", ");
        for (name, ty) in STANDARD_COLUMNS {
            cols.push(*name).push_unseparated(" ").push_unseparated(*ty);
        }
        for p in params {
            cols.push(Identifier::new(p.column.as_str())?)
                .push_unseparated(" ")
                .push_unseparated(p.sql_kind.ddl_type());
        }
    }
    qb.push(")");
    qb.build().execute(&mut **tx).await?;

    // Search-supporting indexes. Executed as separate single statements.
    for (suffix, column) in [("block_idx", "block_number"), ("from_idx", "from_addr")] {
        QueryBuilder::new("CREATE INDEX IF NOT EXISTS ")
            .push(Identifier::new(format!("{table}_{suffix}"))?)
            .push(" ON ")
            .push(table_ident.clone())
            .push(" (")
            .push(column)
            .push(")")
            .build()
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

/// Drop the per-monitor result table.
pub async fn drop_result_table(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    address: &str,
    selector: &str,
) -> AppResult<()> {
    let table = Identifier::new(result_table_name(address, selector)?)?;
    QueryBuilder::new("DROP TABLE IF EXISTS ")
        .push(table)
        .push(" CASCADE")
        .build()
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Remove all decoded results before resetting a monitor for reindexing.
pub async fn truncate_result_table(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    address: &str,
    selector: &str,
) -> AppResult<()> {
    let table = Identifier::new(result_table_name(address, selector)?)?;
    QueryBuilder::new("TRUNCATE TABLE ")
        .push(table)
        .build()
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Transaction metadata + decoded parameters for a single matched tx.
pub struct ResultInput {
    pub tx_hash: String,
    pub block_number: i64,
    pub block_hash: String,
    pub from_addr: String,
    pub to_addr: String,
    pub value: BigDecimal,
    pub gas_used: BigDecimal,
    pub gas_price: BigDecimal,
    pub status: i16,
    pub input_raw: Vec<u8>,
    pub params: Vec<DecodedValue>,
}

/// Insert a matched transaction's metadata and decoded params into the
/// monitor's result table. Idempotent on `tx_hash`.
pub async fn insert_result(
    conn: &mut PgConnection,
    address: &str,
    selector: &str,
    params: &[ParamSpec],
    input: &ResultInput,
) -> AppResult<()> {
    let table = Identifier::new(result_table_name(address, selector)?)?;

    let mut qb: QueryBuilder<sqlx::Postgres> = QueryBuilder::new("INSERT INTO ");
    qb.push(table)
        .push(" (tx_hash, block_number, block_hash, from_addr, to_addr, value, gas_used, gas_price, status, input_raw");
    for p in params {
        qb.push(", ").push(Identifier::new(p.column.as_str())?);
    }
    qb.push(") VALUES (")
        .push_bind(input.tx_hash.as_str())
        .push(", ")
        .push_bind(input.block_number)
        .push(", ")
        .push_bind(input.block_hash.as_str())
        .push(", ")
        .push_bind(input.from_addr.as_str())
        .push(", ")
        .push_bind(input.to_addr.as_str())
        .push(", ")
        .push_bind(&input.value)
        .push(", ")
        .push_bind(&input.gas_used)
        .push(", ")
        .push_bind(&input.gas_price)
        .push(", ")
        .push_bind(input.status)
        .push(", ")
        .push_bind(&input.input_raw);
    for val in &input.params {
        qb.push(", ");
        match val {
            DecodedValue::Uint(v) => {
                qb.push_bind(BigDecimal::from_str(&v.to_string()).map_err(|error| {
                    AppError::Internal(anyhow::anyhow!("uint to numeric: {error}"))
                })?)
            }
            DecodedValue::Int(v) => {
                qb.push_bind(BigDecimal::from_str(&v.to_string()).map_err(|error| {
                    AppError::Internal(anyhow::anyhow!("int to numeric: {error}"))
                })?)
            }
            DecodedValue::Bool(v) => qb.push_bind(*v),
            DecodedValue::Address(v) => qb.push_bind(v.to_string()),
            DecodedValue::String(v) => qb.push_bind(v.as_str()),
            DecodedValue::Bytes(v) => qb.push_bind(v),
        };
    }
    qb.push(") ON CONFLICT (tx_hash) DO NOTHING");

    qb.build().execute(conn).await?;
    Ok(())
}

/// Search filters for a monitor's decoded results.
pub struct SearchParams {
    pub from_addr: Option<String>,
    pub status: Option<i16>,
    pub limit: i64,
    pub offset: i64,
}

/// A single decoded transaction result returned by the search endpoint.
#[derive(Debug)]
pub struct ResultRecord {
    pub tx_hash: String,
    pub block_number: i64,
    pub block_hash: String,
    pub from_addr: String,
    pub to_addr: String,
    /// Decimal string to preserve full NUMERIC precision (u256/u128).
    pub value: String,
    pub gas_used: String,
    pub gas_price: String,
    /// 1 = success, 0 = reverted.
    pub status: i16,
    pub created_at: DateTime<Utc>,
    /// Decoded ABI parameters keyed by their Solidity name.
    pub params: serde_json::Value,
}

/// Query a monitor's result table, ordered by block_number descending.
///
/// Numeric fields are returned as decimal strings to avoid losing precision on
/// 256-bit values; byte fields are returned as `0x`-prefixed hex.
pub async fn query_results(
    pool: &PgPool,
    address: &str,
    selector: &str,
    params: &[ParamSpec],
    search: &SearchParams,
) -> AppResult<Vec<ResultRecord>> {
    let table = Identifier::new(result_table_name(address, selector)?)?;

    let mut qb: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
        "SELECT tx_hash, block_number, block_hash, from_addr, to_addr, value, gas_used, gas_price, status, created_at",
    );
    for p in params {
        qb.push(", ").push(Identifier::new(p.column.as_str())?);
    }
    qb.push(" FROM ").push(table);

    let mut has_cond = false;
    if let Some(ref from) = search.from_addr {
        qb.push(" WHERE from_addr = ").push_bind(from.as_str());
        has_cond = true;
    }
    if let Some(status) = search.status {
        if has_cond {
            qb.push(" AND ");
        } else {
            qb.push(" WHERE ");
        }
        qb.push("status = ").push_bind(status);
    }

    qb.push(" ORDER BY block_number DESC LIMIT ")
        .push_bind(search.limit)
        .push(" OFFSET ")
        .push_bind(search.offset);

    let rows = qb.build().fetch_all(pool).await?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let mut param_map = serde_json::Map::new();
        for p in params {
            param_map.insert(p.name.clone(), read_param(&row, &p.column, p.sql_kind)?);
        }
        results.push(ResultRecord {
            tx_hash: row.try_get("tx_hash")?,
            block_number: row.try_get("block_number")?,
            block_hash: row.try_get("block_hash")?,
            from_addr: row.try_get("from_addr")?,
            to_addr: row.try_get("to_addr")?,
            value: row.try_get::<BigDecimal, _>("value")?.to_string(),
            gas_used: row.try_get::<BigDecimal, _>("gas_used")?.to_string(),
            gas_price: row.try_get::<BigDecimal, _>("gas_price")?.to_string(),
            status: row.try_get("status")?,
            created_at: row.try_get("created_at")?,
            params: serde_json::Value::Object(param_map),
        });
    }
    Ok(results)
}

/// Read a decoded parameter column as JSON, coercing by its storage kind.
fn read_param(row: &PgRow, column: &str, kind: SqlKind) -> AppResult<serde_json::Value> {
    Ok(match kind {
        SqlKind::Numeric => match row.try_get::<Option<BigDecimal>, _>(column)? {
            Some(d) => serde_json::Value::String(d.to_string()),
            None => serde_json::Value::Null,
        },
        SqlKind::Bool => serde_json::json!(row.try_get::<Option<bool>, _>(column)?),
        SqlKind::Text => serde_json::json!(row.try_get::<Option<String>, _>(column)?),
        SqlKind::Bytea => match row.try_get::<Option<Vec<u8>>, _>(column)? {
            Some(b) => serde_json::Value::String(format!("0x{}", hex::encode(b))),
            None => serde_json::Value::Null,
        },
    })
}

/// A validated SQL identifier rendered as a double-quoted Postgres identifier
/// with any embedded `"` doubled.
///
/// Table and column names cannot be bound as query parameters, so they are
/// validated up front and emitted as literal SQL through [`Display`] when
/// pushed into a [`QueryBuilder`]. The validator is intentionally permissive:
/// it rejects only empty strings and NUL bytes (both rejected by Postgres even
/// inside quoted identifiers). Spelling constraints live elsewhere — table
/// names come from [`result_table_name`] (strict 40+8 lowercase hex), column
/// names from parsed Solidity identifiers — so this type guarantees safe
/// quoting, not that a name is a legal *unquoted* identifier.
#[derive(Debug, Clone)]
struct Identifier(String);

impl Identifier {
    fn new(raw: impl Into<String>) -> AppResult<Self> {
        let raw = raw.into();
        if raw.is_empty() {
            return Err(AppError::Internal(anyhow::anyhow!(
                "SQL identifier must be non-empty"
            )));
        }
        if raw.contains('\0') {
            return Err(AppError::Internal(anyhow::anyhow!(
                "SQL identifier must not contain NUL bytes"
            )));
        }
        Ok(Self(raw))
    }
}

impl std::fmt::Display for Identifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Always quote and double any embedded quotes, matching the previous
        // `quote_identifier` output byte-for-byte.
        write!(f, "\"{}\"", self.0.replace('"', "\"\""))
    }
}

#[cfg(test)]
mod tests {
    use super::{Identifier, dedupe_param_columns, result_table_name};
    use crate::abi::ParamSpec;

    fn spec(name: &str, sql_kind: crate::abi::SqlKind) -> ParamSpec {
        ParamSpec {
            name: name.to_string(),
            sol_type: name.to_string(),
            sql_kind,
            column: name.to_string(),
        }
    }

    #[test]
    fn names_tables_from_address_and_selector() {
        assert_eq!(
            result_table_name("0xF36FED68017F6E84D2EB1D4BD35AB56AE0CD914A", "0x6E553F65").unwrap(),
            "f36fed68017f6e84d2eb1d4bd35ab56ae0cd914a_6e553f65"
        );
    }

    #[test]
    fn rejects_invalid_table_components() {
        assert!(result_table_name("not-an-address", "0x6e553f65").is_err());
        assert!(result_table_name("0xf36fed68017f6e84d2eb1d4bd35ab56ae0cd914a", "bad").is_err());
    }

    #[test]
    fn dedupes_only_colliding_param_columns() {
        let mut params = vec![
            spec("to", crate::abi::SqlKind::Text),
            spec("value", crate::abi::SqlKind::Numeric),
        ];
        dedupe_param_columns(&mut params);
        assert_eq!(params[0].name, "to");
        assert_eq!(params[0].column, "to"); // not reserved
        assert_eq!(params[1].name, "value"); // ABI name preserved
        assert_eq!(params[1].column, "value_param"); // renamed
    }

    #[test]
    fn dedupe_avoids_secondary_collision() {
        // `value` collides with reserved, and `value_param` is already taken
        // by another explicit param.
        let mut params = vec![
            spec("value", crate::abi::SqlKind::Numeric),
            spec("value_param", crate::abi::SqlKind::Numeric),
        ];
        dedupe_param_columns(&mut params);
        assert_eq!(params[0].column, "value_param_1");
        assert_eq!(params[1].column, "value_param");
    }

    #[test]
    fn identifier_renders_simple() {
        assert_eq!(Identifier::new("value").unwrap().to_string(), "\"value\"");
    }

    #[test]
    fn identifier_doubles_embedded_quotes() {
        assert_eq!(Identifier::new("a\"b").unwrap().to_string(), "\"a\"\"b\"");
    }

    #[test]
    fn identifier_quotes_reserved_keyword() {
        // Unquoted, `order` would parse as a keyword; quoting makes it an identifier.
        assert_eq!(Identifier::new("order").unwrap().to_string(), "\"order\"");
    }

    #[test]
    fn identifier_rejects_empty() {
        assert!(Identifier::new("").is_err());
    }

    #[test]
    fn identifier_rejects_nul() {
        assert!(Identifier::new("a\0b").is_err());
    }
}
