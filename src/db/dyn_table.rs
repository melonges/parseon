use sqlx::{AssertSqlSafe, PgConnection, PgPool, QueryBuilder};

use crate::abi::{ParamSpec, SqlValue};

/// Return the params table name for a monitor id.
pub fn params_table_name(monitor_id: i64) -> String {
    format!("params_{monitor_id}")
}

/// Create the per-monitor params table with typed columns derived from the
/// method's parameter schema.
///
/// Identifiers (table name, column names) are produced internally from a
/// numeric id and sanitized param names, so this DDL is injection-safe.
pub async fn create_params_table(
    pool: &PgPool,
    monitor_id: i64,
    params: &[ParamSpec],
) -> Result<(), sqlx::Error> {
    let table = params_table_name(monitor_id);
    let mut ddl = format!(
        "CREATE TABLE IF NOT EXISTS \"{table}\" (\n    \
         tx_hash TEXT NOT NULL PRIMARY KEY REFERENCES transactions(tx_hash) ON DELETE CASCADE"
    );
    for p in params {
        ddl.push_str(&format!(
            ",\n    \"{}\" {}",
            p.column,
            p.sql_kind.ddl_type()
        ));
    }
    ddl.push_str("\n);");

    sqlx::query(AssertSqlSafe(ddl)).execute(pool).await?;
    Ok(())
}

/// Drop the per-monitor params table. `monitor_id` is an internal integer,
/// so the table name is not user-controlled.
pub async fn drop_params_table(pool: &PgPool, monitor_id: i64) -> Result<(), sqlx::Error> {
    let table = params_table_name(monitor_id);
    let stmt = format!("DROP TABLE IF EXISTS \"{table}\" CASCADE");
    sqlx::query(AssertSqlSafe(stmt)).execute(pool).await?;
    Ok(())
}

/// Insert a decoded transaction's params into the monitor's params table.
///
/// Each value is bound via sqlx's native `Encode<Postgres>` matching its
/// `SqlValue` variant — no per-column `::cast` is needed because the params
/// table columns already carry the corresponding SqlKind DDL type.
pub async fn insert_params(
    conn: &mut PgConnection,
    monitor_id: i64,
    params: &[ParamSpec],
    tx_hash: &str,
    values: &[SqlValue],
) -> Result<(), sqlx::Error> {
    let table = params_table_name(monitor_id);

    let mut qb: QueryBuilder<sqlx::Postgres> = QueryBuilder::new("INSERT INTO \"");
    qb.push(table).push("\" (tx_hash");
    for p in params {
        qb.push(", \"").push(&p.column).push("\"");
    }
    qb.push(") VALUES (");
    qb.push_bind(tx_hash.to_string());
    for val in values.iter() {
        qb.push(", ");
        match val {
            SqlValue::Numeric(v) => qb.push_bind(v.clone()),
            SqlValue::Bool(v) => qb.push_bind(*v),
            SqlValue::Text(v) => qb.push_bind(v.clone()),
            SqlValue::Bytea(v) => qb.push_bind(v.clone()),
        };
    }
    qb.push(") ON CONFLICT (tx_hash) DO NOTHING");

    qb.build().execute(conn).await?;
    Ok(())
}
