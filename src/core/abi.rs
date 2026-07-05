use std::collections::HashSet;
use std::str::FromStr;

use alloy::dyn_abi::{DynSolEvent, DynSolType, DynSolValue, Specifier};
use alloy::json_abi::{Event, Function};
use alloy::primitives::B256;

use super::DecodedValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbiParam {
    pub name: String,
    pub ty: DynSolType,
    pub indexed: bool,
}

impl AbiParam {
    pub fn new(name: impl Into<String>, ty: DynSolType) -> Result<Self, AbiError> {
        ensure_supported_type(&ty)?;
        Ok(Self {
            name: name.into(),
            ty,
            indexed: false,
        })
    }

    pub fn sol_type(&self) -> String {
        self.ty.to_string()
    }
}

impl AbiParam {
    pub fn with_indexed(mut self, indexed: bool) -> Self {
        self.indexed = indexed;
        self
    }
}

#[derive(Debug, Clone)]
pub struct MethodSpec {
    pub selector: [u8; 4],
    pub params: Vec<AbiParam>,
}

#[derive(Debug, Clone)]
pub struct EventSpec {
    pub topic0: B256,
    pub params: Vec<AbiParam>,
}

#[derive(Debug, Clone)]
pub enum TargetSpec {
    Call(MethodSpec),
    Event(EventSpec),
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

pub fn parse_abi_type(value: &str) -> Result<DynSolType, AbiError> {
    let ty = DynSolType::from_str(value).map_err(|error| AbiError::Type(error.to_string()))?;
    ensure_supported_type(&ty)?;
    Ok(ty)
}

pub fn parse_selector(value: &str) -> Result<[u8; 4], AbiError> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    let bytes = alloy::hex::decode(value)
        .map_err(|error| AbiError::Parse(format!("invalid selector {value}: {error}")))?;
    bytes
        .try_into()
        .map_err(|_| AbiError::Parse(format!("selector must contain exactly 4 bytes: {value}")))
}

fn ensure_supported_type(ty: &DynSolType) -> Result<(), AbiError> {
    match ty {
        DynSolType::Bool
        | DynSolType::Int(_)
        | DynSolType::Uint(_)
        | DynSolType::FixedBytes(_)
        | DynSolType::Address
        | DynSolType::Function
        | DynSolType::Bytes
        | DynSolType::String => Ok(()),
        _ => Err(AbiError::Type(format!(
            "composite type not supported: {ty}"
        ))),
    }
}

pub fn parse_func_signature(signature: &str) -> Result<MethodSpec, AbiError> {
    let func = Function::parse(signature)
        .map_err(|error| AbiError::Parse(format!("failed to parse signature: {error}")))?;

    let mut names = HashSet::new();
    let params = func
        .inputs
        .iter()
        .enumerate()
        .map(|(index, param)| -> Result<AbiParam, AbiError> {
            let ty = Specifier::<DynSolType>::resolve(param)
                .map_err(|error| AbiError::Type(format!("param `{}`: {error}", param.name)))?;
            let name = if param.name.is_empty() {
                format!("arg_{index}")
            } else {
                param.name.clone()
            };
            if !names.insert(name.clone()) {
                return Err(AbiError::Parse(format!(
                    "duplicate parameter name `{name}`"
                )));
            }
            AbiParam::new(name, ty)
        })
        .collect::<Result<Vec<_>, _>>()?;

    if params.is_empty() {
        return Err(AbiError::Parse(
            "functions with no inputs are not supported".to_string(),
        ));
    }

    Ok(MethodSpec {
        selector: func.selector().into(),
        params,
    })
}

pub fn parse_target_signature(signature: &str) -> Result<TargetSpec, AbiError> {
    if signature.trim_start().starts_with("event ") {
        parse_event_signature(signature).map(TargetSpec::Event)
    } else {
        parse_func_signature(signature).map(TargetSpec::Call)
    }
}

pub fn parse_event_signature(signature: &str) -> Result<EventSpec, AbiError> {
    let event = Event::parse(signature)
        .map_err(|error| AbiError::Parse(format!("failed to parse event signature: {error}")))?;
    if event.anonymous {
        return Err(AbiError::Parse("anonymous events are not supported".into()));
    }
    let mut names = HashSet::new();
    let params = event
        .inputs
        .iter()
        .enumerate()
        .map(|(index, param)| {
            let ty = Specifier::<DynSolType>::resolve(param)
                .map_err(|error| AbiError::Type(format!("param `{}`: {error}", param.name)))?;
            let name = if param.name.is_empty() {
                format!("arg_{index}")
            } else {
                param.name.clone()
            };
            if !names.insert(name.clone()) {
                return Err(AbiError::Parse(format!(
                    "duplicate parameter name `{name}`"
                )));
            }
            Ok(AbiParam::new(name, ty)?.with_indexed(param.indexed))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;
    Ok(EventSpec {
        topic0: event.selector(),
        params,
    })
}

pub fn decode_event(
    params: &[AbiParam],
    topic0: B256,
    topics: &[B256],
    data: &[u8],
) -> Result<Vec<DecodedValue>, AbiError> {
    let indexed = params
        .iter()
        .filter(|p| p.indexed)
        .map(|p| p.ty.clone())
        .collect();
    let body = DynSolType::Tuple(
        params
            .iter()
            .filter(|p| !p.indexed)
            .map(|p| p.ty.clone())
            .collect(),
    );
    let event = DynSolEvent::new(Some(topic0), indexed, body)
        .ok_or_else(|| AbiError::Decode("event has too many indexed parameters".into()))?;
    let decoded = event
        .decode_log_parts(topics.iter().copied(), data)
        .map_err(|error| AbiError::Decode(error.to_string()))?;
    let mut indexed = decoded.indexed.into_iter();
    let mut body = decoded.body.into_iter();
    params
        .iter()
        .map(|param| {
            decode_value(
                if param.indexed {
                    indexed.next()
                } else {
                    body.next()
                }
                .ok_or_else(|| AbiError::Decode("decoded parameter count mismatch".into()))?,
            )
        })
        .collect()
}

pub fn decode_calldata(params: &[AbiParam], data: &[u8]) -> Result<Vec<DecodedValue>, AbiError> {
    let ty = DynSolType::Tuple(params.iter().map(|param| param.ty.clone()).collect());
    let decoded = ty
        .abi_decode_params(data)
        .map_err(|error| AbiError::Decode(error.to_string()))?;
    let values = match decoded {
        DynSolValue::Tuple(values) => values,
        single => vec![single],
    };
    values.into_iter().map(decode_value).collect()
}

fn decode_value(value: DynSolValue) -> Result<DecodedValue, AbiError> {
    match value {
        DynSolValue::Bool(value) => Ok(DecodedValue::Bool(value)),
        DynSolValue::Address(value) => Ok(DecodedValue::Address(value)),
        DynSolValue::String(value) => Ok(DecodedValue::String(value)),
        DynSolValue::Bytes(value) => Ok(DecodedValue::Bytes(value)),
        DynSolValue::FixedBytes(word, size) => Ok(DecodedValue::Bytes(word[..size].to_vec())),
        DynSolValue::Function(value) => Ok(DecodedValue::Bytes(value.as_slice().to_vec())),
        DynSolValue::Uint(value, _) => Ok(DecodedValue::Uint(value)),
        DynSolValue::Int(value, _) => Ok(DecodedValue::Int(value)),
        _ => Err(AbiError::Decode(
            "composite types not supported at decode time".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, U256, address, keccak256};
    use alloy::sol_types::SolCall;

    use super::*;

    alloy::sol! {
        function transfer(address to, uint256 value) external returns (bool);
        function f_bytes32(bytes32 word) external returns (bool);
    }

    #[test]
    fn parses_a_typed_method() {
        let spec =
            parse_func_signature("function transfer(address to, uint256 value) returns (bool)")
                .unwrap();
        assert_eq!(spec.selector, transferCall::SELECTOR);
        assert_eq!(spec.params[0].name, "to");
        assert_eq!(spec.params[0].ty, DynSolType::Address);
        assert_eq!(spec.params[1].ty, DynSolType::Uint(256));
    }

    #[test]
    fn generates_names_and_rejects_duplicates() {
        let spec = parse_func_signature("transfer(address,uint256)").unwrap();
        assert_eq!(spec.params[0].name, "arg_0");
        assert_eq!(spec.params[1].name, "arg_1");
        assert!(parse_func_signature("transfer(address value,uint256 value)").is_err());
        assert!(parse_func_signature("transfer(address,uint256 arg_0)").is_err());
    }

    #[test]
    fn rejects_unsupported_signatures() {
        assert!(parse_func_signature("totalSupply()").is_err());
        assert!(parse_func_signature("doThing(uint256[] ids)").is_err());
    }

    #[test]
    fn decodes_typed_calldata() {
        let call = transferCall {
            to: address!("d8da6bf26964af9d7eed66e0db1c02b9c4a5b0e9"),
            value: U256::from(42),
        };
        let data = call.abi_encode();
        let params = parse_func_signature("transfer(address to,uint256 value)")
            .unwrap()
            .params;
        let values = decode_calldata(&params, &data[4..]).unwrap();
        assert_eq!(values[0], DecodedValue::Address(call.to));
        assert_eq!(values[1], DecodedValue::Uint(call.value));
    }

    #[test]
    fn decodes_fixed_bytes() {
        let mut word = [0u8; 32];
        word[..2].copy_from_slice(&[0xde, 0xad]);
        let call = f_bytes32Call { word: word.into() };
        let data = call.abi_encode();
        let params = parse_func_signature("f_bytes32(bytes32 word)")
            .unwrap()
            .params;
        let values = decode_calldata(&params, &data[4..]).unwrap();
        assert_eq!(values[0], DecodedValue::Bytes(word.to_vec()));
    }

    #[test]
    fn address_type_is_semantic_not_storage_specific() {
        let param = AbiParam::new("owner", DynSolType::Address).unwrap();
        assert_eq!(param.sol_type(), "address");
        assert_eq!(Address::ZERO.to_string().len(), 42);
    }

    #[test]
    fn infers_event_kind_and_preserves_indexed_metadata() {
        let TargetSpec::Event(spec) = parse_target_signature(
            "event Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap() else {
            panic!("expected event")
        };
        assert_eq!(spec.topic0, keccak256("Transfer(address,address,uint256)"));
        assert!(spec.params[0].indexed);
        assert!(!spec.params[2].indexed);
        assert!(parse_event_signature("event Hidden(uint256 value) anonymous").is_err());
        assert!(parse_event_signature("event Batch(uint256[] values)").is_err());
    }

    #[test]
    fn decodes_indexed_and_body_event_values() {
        let spec = parse_event_signature(
            "event Message(address indexed sender, string indexed label, uint256 value)",
        )
        .unwrap();
        let sender = address!("0000000000000000000000000000000000000001");
        let mut sender_topic = [0u8; 32];
        sender_topic[12..].copy_from_slice(sender.as_slice());
        let label_hash = keccak256("hello");
        let topics = [spec.topic0, B256::from(sender_topic), label_hash];
        let data =
            DynSolValue::Tuple(vec![DynSolValue::Uint(U256::from(7), 256)]).abi_encode_params();
        let values = decode_event(&spec.params, spec.topic0, &topics, &data).unwrap();
        assert_eq!(values[0], DecodedValue::Address(sender));
        assert_eq!(values[1], DecodedValue::Bytes(label_hash.to_vec()));
        assert_eq!(values[2], DecodedValue::Uint(U256::from(7)));
    }
}
