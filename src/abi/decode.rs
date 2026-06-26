use alloy::dyn_abi::{DynSolType, DynSolValue};
use serde_json::{Value, json};

use super::parse::{AbiError, hex_encode};

/// Decode calldata (after the 4-byte selector) into text representations
/// suitable for SQL binding with per-column casts.
///
/// Returns one `Option<String>` per parameter in declaration order. `None`
/// indicates a null (only possible for dynamically-shaped values that decoded
/// as such).
pub fn decode_calldata(input_types: &str, data: &[u8]) -> Result<Vec<Option<String>>, AbiError> {
    let ty = DynSolType::parse(input_types)
        .map_err(|e| AbiError::Type(format!("{input_types}: {e}")))?;
    let value = ty
        .abi_decode_params(data)
        .map_err(|e| AbiError::Decode(format!("{e}")))?;
    let elements = match value {
        DynSolValue::Tuple(v) | DynSolValue::Array(v) | DynSolValue::FixedArray(v) => v,
        other => vec![other],
    };
    Ok(elements.into_iter().map(value_to_text).collect())
}

fn addr_lowercase(a: &alloy::primitives::Address) -> String {
    format!("0x{}", hex_encode(a.as_ref()))
}

fn value_to_text(v: DynSolValue) -> Option<String> {
    match v {
        DynSolValue::Bool(b) => Some(if b { "true".into() } else { "false".into() }),
        DynSolValue::Address(a) => Some(addr_lowercase(&a)),
        DynSolValue::String(s) => Some(s),
        DynSolValue::Bytes(b) => Some(hex_encode(&b)),
        DynSolValue::FixedBytes(word, size) => Some(hex_encode(&word[..size])),
        DynSolValue::Uint(u, _) => Some(u.to_string()),
        DynSolValue::Int(i, _) => Some(i.to_string()),
        DynSolValue::Function(f) => Some(hex_encode(f.as_ref())),
        DynSolValue::Array(_) | DynSolValue::FixedArray(_) | DynSolValue::Tuple(_) => {
            Some(dyn_to_json(v).to_string())
        }
    }
}

/// Recursively convert a `DynSolValue` into a JSON value for JSONB storage.
pub fn dyn_to_json(v: DynSolValue) -> Value {
    match v {
        DynSolValue::Bool(b) => json!(b),
        DynSolValue::Address(a) => json!(addr_lowercase(&a)),
        DynSolValue::String(s) => json!(s),
        DynSolValue::Bytes(b) => json!(format!("0x{}", hex_encode(&b))),
        DynSolValue::FixedBytes(word, size) => json!(format!("0x{}", hex_encode(&word[..size]))),
        DynSolValue::Uint(u, _) => json!(u.to_string()),
        DynSolValue::Int(i, _) => json!(i.to_string()),
        DynSolValue::Function(f) => json!(format!("0x{}", hex_encode(f.as_ref()))),
        DynSolValue::Array(v) | DynSolValue::FixedArray(v) | DynSolValue::Tuple(v) => {
            json!(v.into_iter().map(dyn_to_json).collect::<Vec<_>>())
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
    }

    #[test]
    fn decodes_transfer_calldata() {
        let call = transferCall {
            to: address!("d8da6bf26964af9d7eed66e0db1c02b9c4a5b0e9"),
            value: U256::from(1_000_000_000_000u64),
        };
        let data = call.abi_encode();
        let selector = &data[..4];
        let args = &data[4..];
        assert_eq!(hex_encode(selector), "a9059cbb");

        let texts = decode_calldata("(address,uint256)", args).unwrap();
        assert_eq!(texts.len(), 2);
        assert_eq!(
            texts[0].as_deref().unwrap(),
            "0xd8da6bf26964af9d7eed66e0db1c02b9c4a5b0e9"
        );
        assert_eq!(texts[1].as_deref().unwrap(), "1000000000000");
    }
}
