use alloy::dyn_abi::DynSolType;
use serde::{Deserialize, Serialize};

use super::parser::AbiError;

/// SQL storage kind for a decoded Solidity parameter.
///
/// Only primitive Solidity types are supported; arrays and tuples are rejected
/// at monitor-creation time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SqlKind {
    Numeric,
    Bool,
    Text,
    Bytea,
}

impl SqlKind {
    /// DDL column type used when creating the params table.
    pub fn ddl_type(self) -> &'static str {
        match self {
            SqlKind::Numeric => "NUMERIC",
            SqlKind::Bool => "BOOLEAN",
            SqlKind::Text => "TEXT",
            SqlKind::Bytea => "BYTEA",
        }
    }
}

/// Map a resolved `DynSolType` to a SQL storage kind.
///
/// Returns `Err` for composite types (arrays, tuples, custom structs) so the
/// caller (parser) can reject the signature up-front.
pub fn sol_type_to_sql_kind(ty: &DynSolType) -> Result<SqlKind, AbiError> {
    match ty {
        DynSolType::Bool => Ok(SqlKind::Bool),
        DynSolType::Address | DynSolType::String => Ok(SqlKind::Text),
        DynSolType::Bytes | DynSolType::FixedBytes(_) | DynSolType::Function => {
            Ok(SqlKind::Bytea)
        }
        DynSolType::Uint(_) | DynSolType::Int(_) => Ok(SqlKind::Numeric),
        DynSolType::Array(_)
        | DynSolType::FixedArray(_, _)
        | DynSolType::Tuple(_) => Err(AbiError::Type(format!(
            "composite type not supported: {ty}"
        ))),
    }
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