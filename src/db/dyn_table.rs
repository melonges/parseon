use sqlx::{AssertSqlSafe, PgConnection, QueryBuilder, Transaction};

use crate::abi::{ParamSpec, SqlValue};
use crate::error::{AppError, AppResult};

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

/// Create the per-monitor result table with typed columns derived from the
/// method's parameter schema.
///
/// Solidity parameter names are identifiers by grammar and are always quoted.
pub async fn create_result_table(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    address: &str,
    selector: &str,
    params: &[ParamSpec],
) -> AppResult<()> {
    let table = result_table_name(address, selector)?;
    let mut ddl = format!("CREATE TABLE IF NOT EXISTS \"{table}\" (\n");
    for (index, p) in params.iter().enumerate() {
        if index > 0 {
            ddl.push_str(",\n");
        }
        ddl.push_str(&format!(
            "    {} {}",
            quote_identifier(&p.column),
            p.sql_kind.ddl_type()
        ));
    }
    ddl.push_str("\n);");

    sqlx::query(AssertSqlSafe(ddl)).execute(&mut **tx).await?;
    Ok(())
}

/// Drop the per-monitor result table.
pub async fn drop_result_table(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    address: &str,
    selector: &str,
) -> AppResult<()> {
    let table = result_table_name(address, selector)?;
    let stmt = format!("DROP TABLE IF EXISTS \"{table}\" CASCADE");
    sqlx::query(AssertSqlSafe(stmt)).execute(&mut **tx).await?;
    Ok(())
}

/// Remove all decoded results before resetting a monitor for reindexing.
pub async fn truncate_result_table(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    address: &str,
    selector: &str,
) -> AppResult<()> {
    let table = result_table_name(address, selector)?;
    let stmt = format!("TRUNCATE TABLE \"{table}\"");
    sqlx::query(AssertSqlSafe(stmt)).execute(&mut **tx).await?;
    Ok(())
}

/// Insert a decoded transaction's params into the monitor's result table.
///
/// Each value is bound via sqlx's native `Encode<Postgres>` matching its
/// `SqlValue` variant — no per-column `::cast` is needed because the params
/// table columns already carry the corresponding SqlKind DDL type.
pub async fn insert_params(
    conn: &mut PgConnection,
    address: &str,
    selector: &str,
    params: &[ParamSpec],
    values: &[SqlValue],
) -> AppResult<()> {
    let table = result_table_name(address, selector)?;

    let mut qb: QueryBuilder<sqlx::Postgres> = QueryBuilder::new("INSERT INTO \"");
    qb.push(table).push("\" (");
    for (index, p) in params.iter().enumerate() {
        if index > 0 {
            qb.push(", ");
        }
        qb.push(quote_identifier(&p.column));
    }
    qb.push(") VALUES (");
    for (index, val) in values.iter().enumerate() {
        if index > 0 {
            qb.push(", ");
        }
        match val {
            SqlValue::Numeric(v) => qb.push_bind(v.clone()),
            SqlValue::Bool(v) => qb.push_bind(*v),
            SqlValue::Text(v) => qb.push_bind(v.clone()),
            SqlValue::Bytea(v) => qb.push_bind(v.clone()),
        };
    }
    qb.push(")");

    qb.build().execute(conn).await?;
    Ok(())
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::result_table_name;

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
}
