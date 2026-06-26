use serde::{Deserialize, Serialize};

/// SQL storage kind for a decoded Solidity parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SqlKind {
    Bigint,
    Numeric,
    Bool,
    Text,
    Bytea,
    Jsonb,
}

impl SqlKind {
    /// DDL column type used when creating the params table.
    pub fn ddl_type(self) -> &'static str {
        match self {
            SqlKind::Bigint => "BIGINT",
            SqlKind::Numeric => "NUMERIC",
            SqlKind::Bool => "BOOLEAN",
            SqlKind::Text => "TEXT",
            SqlKind::Bytea => "BYTEA",
            SqlKind::Jsonb => "JSONB",
        }
    }
}

/// Map a (possibly composite) Solidity type string to a SQL storage kind.
pub fn sol_type_to_sql_kind(sol: &str) -> SqlKind {
    let s = sol.trim();
    // Arrays and tuples collapse into JSONB.
    if s.starts_with('(') || s.contains('[') {
        return SqlKind::Jsonb;
    }
    // Strip a leading tuple-less base type.
    let base = s.split('(').next().unwrap_or(s).trim();
    match base {
        "bool" => SqlKind::Bool,
        "address" => SqlKind::Text,
        "string" => SqlKind::Text,
        "bytes" => SqlKind::Bytea,
        b if b.starts_with("bytes") => SqlKind::Bytea,
        u if u.starts_with("uint") => {
            let bits = parse_bits(&u["uint".len()..]);
            if bits <= 64 {
                SqlKind::Bigint
            } else {
                SqlKind::Numeric
            }
        }
        i if i.starts_with("int") => {
            let bits = parse_bits(&i["int".len()..]);
            if bits <= 64 {
                SqlKind::Bigint
            } else {
                SqlKind::Numeric
            }
        }
        _ => SqlKind::Jsonb,
    }
}

fn parse_bits(rest: &str) -> u32 {
    if rest.is_empty() {
        return 256;
    }
    rest.parse::<u32>().unwrap_or(256)
}

/// Sanitize a Solidity parameter name into a safe SQL column identifier.
/// Resulting columns are prefixed with `param_` to avoid collisions with
/// reserved names (e.g. `tx_hash`).
pub fn sanitize_column(name: &str) -> String {
    let mut out = String::from("param_");
    for c in name.chars() {
        if c.is_ascii_alphanumeric() || c == '_' {
            out.push(c);
        } else {
            out.push('_');
        }
    }
    if out == "param_" {
        out.push_str("arg");
    }
    out
}
