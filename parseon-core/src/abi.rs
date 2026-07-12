use std::collections::HashSet;
use std::str::FromStr;

use alloy::dyn_abi::{DynSolEvent, DynSolType, DynSolValue, Specifier};
use alloy::json_abi::{AbiItem, Event, Function};
use alloy::primitives::{B256, Selector};

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
        Ok(Self { name: name.into(), ty, indexed: false })
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
    pub selector: Selector,
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
        _ => Err(AbiError::Type(format!("composite type not supported: {ty}"))),
    }
}

fn method_spec(func: &Function) -> Result<MethodSpec, AbiError> {
    let mut names = HashSet::new();
    let params = func
        .inputs
        .iter()
        .enumerate()
        .map(|(index, param)| -> Result<AbiParam, AbiError> {
            let ty = Specifier::<DynSolType>::resolve(param)
                .map_err(|error| AbiError::Type(format!("param `{}`: {error}", param.name)))?;
            let name =
                if param.name.is_empty() { format!("arg_{index}") } else { param.name.clone() };
            if !names.insert(name.clone()) {
                return Err(AbiError::Parse(format!("duplicate parameter name `{name}`")));
            }
            AbiParam::new(name, ty)
        })
        .collect::<Result<Vec<_>, _>>()?;

    if params.is_empty() {
        return Err(AbiError::Parse("functions with no inputs are not supported".to_string()));
    }

    Ok(MethodSpec { selector: func.selector(), params })
}

pub fn parse_target_signature(signature: &str) -> Result<TargetSpec, AbiError> {
    match AbiItem::parse(signature)
        .map_err(|error| AbiError::Parse(format!("failed to parse target signature: {error}")))?
    {
        AbiItem::Function(func) => method_spec(&func).map(TargetSpec::Call),
        AbiItem::Event(event) => event_spec(&event).map(TargetSpec::Event),
        _ => Err(AbiError::Parse(format!("invalid target signature: {signature}"))),
    }
}

fn event_spec(event: &Event) -> Result<EventSpec, AbiError> {
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
            let name =
                if param.name.is_empty() { format!("arg_{index}") } else { param.name.clone() };
            if !names.insert(name.clone()) {
                return Err(AbiError::Parse(format!("duplicate parameter name `{name}`")));
            }
            Ok(AbiParam::new(name, ty)?.with_indexed(param.indexed))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;
    Ok(EventSpec { topic0: event.selector(), params })
}

pub fn decode_event(
    params: &[AbiParam],
    topic0: B256,
    topics: &[B256],
    data: &[u8],
) -> Result<Vec<DecodedValue>, AbiError> {
    let indexed = params.iter().filter(|p| p.indexed).map(|p| p.ty.clone()).collect();
    let body =
        DynSolType::Tuple(params.iter().filter(|p| !p.indexed).map(|p| p.ty.clone()).collect());
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
                if param.indexed { indexed.next() } else { body.next() }
                    .ok_or_else(|| AbiError::Decode("decoded parameter count mismatch".into()))?,
            )
        })
        .collect()
}

pub fn decode_calldata(params: &[AbiParam], data: &[u8]) -> Result<Vec<DecodedValue>, AbiError> {
    let ty = DynSolType::Tuple(params.iter().map(|param| param.ty.clone()).collect());
    let decoded =
        ty.abi_decode_params(data).map_err(|error| AbiError::Decode(error.to_string()))?;
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
        _ => Err(AbiError::Decode("composite types not supported at decode time".into())),
    }
}

#[cfg(test)]
mod tests {
    use alloy::sol_types::SolCall;

    use super::*;

    alloy::sol! {
        function transfer(address to, uint256 value) external returns (bool);
        function f_bytes32(bytes32 word) external returns (bool);
    }

    #[test]
    fn infers_function_kind_and_rejects_other_abi_items() {
        let TargetSpec::Call(spec) =
            parse_target_signature("function transfer(address to, uint256 value)").unwrap()
        else {
            panic!("expected call")
        };
        assert_eq!(spec.selector, transferCall::SELECTOR);
        assert!(parse_target_signature("error Unauthorized(address caller)").is_err());
    }
}
