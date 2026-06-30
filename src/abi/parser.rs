use alloy::dyn_abi::{DynSolType, Specifier};
use alloy::json_abi::Function;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use super::types::{SqlKind, sol_type_to_sql_kind};

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ParamSpec {
    pub name: String,
    pub sol_type: String,
    pub sql_kind: SqlKind,
    pub column: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MethodSpec {
    pub name: String,
    pub canonical_signature: String,
    pub selector: String,
    pub params: Vec<ParamSpec>,
}

#[derive(Debug, thiserror::Error)]
pub enum AbiError {
    #[error("failed to parse signature: {0}")]
    Parse(String),
    #[error("invalid solidity type: {0}")]
    Type(String),
    #[error("decode error: {0}")]
    Decode(String),
}

/// Parse a human-readable function signature into a typed `MethodSpec`.
///
/// Accepts forms like:
///   `function transfer(address to, uint256 value) returns (bool)`
///   `transfer(address to, uint256 value)`
///
/// Each input's Solidity type is resolved to a `DynSolType` via alloy's
/// `Specifier` trait, so composite types (arrays, tuples) are rejected here
/// using the library's own type discriminant — no hand-rolled string matching.
pub fn parse_func_signature(signature: &str) -> Result<MethodSpec, AbiError> {
    let func = Function::parse(signature)
        .map_err(|e| AbiError::Parse(format!("failed to parse signature: {e}")))?;

    let mut columns = HashSet::new();
    let params = func
        .inputs
        .iter()
        .enumerate()
        .map(|(index, p)| -> Result<ParamSpec, AbiError> {
            let ty = Specifier::<DynSolType>::resolve(p)
                .map_err(|e| AbiError::Type(format!("param `{}`: {e}", p.name)))?;
            let sql_kind = sol_type_to_sql_kind(&ty)?;
            let name = if p.name.is_empty() {
                format!("arg_{index}")
            } else {
                p.name.clone()
            };
            if !columns.insert(name.clone()) {
                return Err(AbiError::Parse(format!(
                    "duplicate parameter name `{name}`"
                )));
            }
            Ok(ParamSpec {
                name: name.clone(),
                sol_type: format!("{ty}"),
                sql_kind,
                column: name,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    if params.is_empty() {
        return Err(AbiError::Parse(
            "functions with no inputs are not supported".to_string(),
        ));
    }

    Ok(MethodSpec {
        name: func.name.clone(),
        canonical_signature: func.signature(),
        selector: func.selector().to_string(),
        params,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_transfer() {
        let spec =
            parse_func_signature("function transfer(address to, uint256 value) returns (bool)")
                .unwrap();
        assert_eq!(spec.name, "transfer");
        assert_eq!(spec.canonical_signature, "transfer(address,uint256)");
        assert_eq!(spec.selector, "0xa9059cbb");
        assert_eq!(spec.params.len(), 2);
        assert_eq!(spec.params[0].column, "to");
        assert_eq!(spec.params[1].column, "value");
        assert_eq!(spec.params[0].sol_type, "address");
        assert_eq!(spec.params[1].sol_type, "uint256");
    }

    #[test]
    fn parses_approve() {
        let spec = parse_func_signature("approve(address spender, uint256 amount) returns (bool)")
            .unwrap();
        assert_eq!(spec.canonical_signature, "approve(address,uint256)");
        assert_eq!(spec.selector, "0x095ea7b3");
    }

    #[test]
    fn parses_no_name_params() {
        let spec = parse_func_signature("transfer(address,uint256)").unwrap();
        assert_eq!(spec.canonical_signature, "transfer(address,uint256)");
        assert_eq!(spec.params[0].column, "arg_0");
        assert_eq!(spec.params[1].column, "arg_1");
    }

    #[test]
    fn rejects_duplicate_param_names() {
        let err = parse_func_signature("transfer(address value,uint256 value)").unwrap_err();
        assert!(err.to_string().contains("duplicate parameter name"));
    }

    #[test]
    fn preserves_exact_named_columns() {
        let spec = parse_func_signature("deposit(uint256 assets,address receiver)").unwrap();
        assert_eq!(spec.params[0].column, "assets");
        assert_eq!(spec.params[1].column, "receiver");
    }

    #[test]
    fn rejects_generated_and_explicit_duplicate_names() {
        let err = parse_func_signature("transfer(address,uint256 arg_0)").unwrap_err();
        assert!(err.to_string().contains("duplicate parameter name"));
    }

    #[test]
    fn rejects_zero_input_functions() {
        let err = parse_func_signature("totalSupply()").unwrap_err();
        assert!(err.to_string().contains("no inputs"));
    }

    #[test]
    fn rejects_array_param() {
        let err = parse_func_signature("doThing(uint256[] ids, address[] users)").unwrap_err();
        assert!(
            err.to_string().contains("composite type not supported"),
            "got: {err}"
        );
    }

    #[test]
    fn parses_uint_alias_as_numeric() {
        let spec = parse_func_signature("setUint(uint v)").unwrap();
        assert_eq!(spec.canonical_signature, "setUint(uint256)");
        assert_eq!(spec.params[0].sql_kind, SqlKind::Numeric);
        assert_eq!(spec.params[0].sol_type, "uint256");
    }
}
