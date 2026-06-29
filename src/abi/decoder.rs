use alloy::dyn_abi::{DynSolType, DynSolValue};
use sqlx::types::BigDecimal;
use std::str::FromStr;

use super::parser::AbiError;

/// Rust-native SQL bind target. The abi layer owns all Solidity-to-Rust
/// conversion; the db layer binds variants directly via sqlx `Encode<Postgres>`
/// with no per-column `::cast`.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Numeric(BigDecimal),
    Bool(bool),
    Text(String),
    Bytea(Vec<u8>),
}

/// Decode calldata (after the 4-byte selector) into native Rust values
/// suitable for direct sqlx binding.
///
/// Returns one `SqlValue` per parameter in declaration order.
pub fn decode_calldata(
    input_types: &str,
    data: &[u8],
) -> Result<Vec<SqlValue>, AbiError> {
    let ty = DynSolType::from_str(input_types)
        .map_err(|e| AbiError::Type(e.to_string()))?;
    let decoded = ty
        .abi_decode_params(data)
        .map_err(|e| AbiError::Decode(e.to_string()))?;
    let values = match decoded {
        DynSolValue::Tuple(v) | DynSolValue::Array(v) => v,
        single => vec![single],
    };
    values.into_iter().map(value_to_sql).collect()
}

/// Convert a decoded `DynSolValue` into the Rust-native `SqlValue`.
///
/// Integers always become `Numeric(BigDecimal)` (arbitrary-precision), so any
/// Solidity bit width is supported without overflow guards. Composite variants
/// (`Array`/`FixedArray`/`Tuple`) are unreachable here: the parser rejects
/// composite params at monitor-creation time.
pub fn value_to_sql(v: DynSolValue) -> Result<SqlValue, AbiError> {
    match v {
        DynSolValue::Bool(b) => Ok(SqlValue::Bool(b)),
        DynSolValue::Address(a) => Ok(SqlValue::Text(format!("{a:?}"))),
        DynSolValue::String(s) => Ok(SqlValue::Text(s)),
        DynSolValue::Bytes(b) => Ok(SqlValue::Bytea(b.to_vec())),
        DynSolValue::FixedBytes(word, size) => {
            Ok(SqlValue::Bytea(word[..size].to_vec()))
        }
        DynSolValue::Function(f) => Ok(SqlValue::Bytea(f.as_slice().to_vec())),
        DynSolValue::Uint(u, _) => BigDecimal::from_str(&u.to_string())
            .map(SqlValue::Numeric)
            .map_err(|e| AbiError::Decode(format!("uint->BigDecimal: {e}"))),
        DynSolValue::Int(i, _) => BigDecimal::from_str(&i.to_string())
            .map(SqlValue::Numeric)
            .map_err(|e| AbiError::Decode(format!("int->BigDecimal: {e}"))),
        DynSolValue::Array(_) | DynSolValue::FixedArray(_) | DynSolValue::Tuple(_) => {
            Err(AbiError::Decode(
                "composite types not supported at decode time".into(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{U256, address};
    use alloy::sol_types::SolCall;

    alloy::sol! {
        function transfer(address to, uint256 value) external returns (bool);
        function f_uint64(uint64 x) external returns (bool);
        function f_bytes32(bytes32 w) external returns (bool);
        function f_bool(bool b) external returns (bool);
    }

    #[test]
    fn decodes_transfer_calldata() {
        let call = transferCall {
            to: address!("d8da6bf26964af9d7eed66e0db1c02b9c4a5b0e9"),
            value: U256::from(1_000_000_000_000u64),
        };
        let data = call.abi_encode();
        let args = &data[4..];

        let values = decode_calldata("(address,uint256)", args).unwrap();
        assert_eq!(values.len(), 2);
        match &values[0] {
            SqlValue::Text(s) => assert_eq!(
                s,
                "0xd8da6bf26964af9d7eed66e0db1c02b9c4a5b0e9"
            ),
            v => panic!("address: {v:?}"),
        }
        match &values[1] {
            SqlValue::Numeric(n) => {
                assert_eq!(n.to_string(), "1000000000000")
            }
            v => panic!("uint256: {v:?}"),
        }
    }

    #[test]
    fn decodes_uint64_as_numeric() {
        let call = f_uint64Call { x: u64::MAX };
        let data = call.abi_encode();
        let values = decode_calldata("(uint64)", &data[4..]).unwrap();
        match &values[0] {
            SqlValue::Numeric(n) => assert_eq!(n.to_string(), u64::MAX.to_string()),
            v => panic!("uint64: {v:?}"),
        }
    }

    #[test]
    fn decodes_bytes32_as_bytea() {
        let mut word = [0u8; 32];
        word[0] = 0xde;
        word[1] = 0xad;
        let call = f_bytes32Call { w: word.into() };
        let data = call.abi_encode();
        let values = decode_calldata("(bytes32)", &data[4..]).unwrap();
        match &values[0] {
            SqlValue::Bytea(b) => assert_eq!(b.as_slice(), &word[..]),
            v => panic!("bytes32: {v:?}"),
        }
    }

    #[test]
    fn decodes_bool() {
        let call = f_boolCall { b: true };
        let data = call.abi_encode();
        let values = decode_calldata("(bool)", &data[4..]).unwrap();
        match &values[0] {
            SqlValue::Bool(true) => {}
            v => panic!("bool: {v:?}"),
        }
    }
}