//! ABI parameter types and reusable decoders for function calls and events.
//!
//! Parseon accepts human-readable Solidity signatures at monitor creation time
//! and resolves them to fixed selectors and topic0 hashes via
//! [`parse_target_signature`]. The resulting [`AbiParam`] schema feeds
//! [`CallDecoder`] and [`EventDecoder`], which are reused per monitor snapshot
//! so the indexing hot path does not re-parse ABI types on every block.
//!
//! Only scalar ABI types are supported (uint, int, bool, address, string,
//! bytes, fixed bytes, function). Composite types (arrays, tuples, structs)
//! are rejected at parse time so storage adapters can map every parameter to
//! a flat column or BSON field.

use std::collections::HashSet;
use std::str::FromStr;

use alloy::dyn_abi::{DynSolEvent, DynSolType, DynSolValue, Specifier};
use alloy::json_abi::{AbiItem, Event, Function};
use alloy::primitives::{B256, Selector};

use super::DecodedValue;

/// One ABI parameter: name, scalar type, and indexed flag (for events).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbiParam {
    /// Parameter name. Unnamed parameters in the source signature are
    /// renamed to `arg0`, `arg1`, … so filter fields and storage columns
    /// always have a stable name.
    pub name: String,
    /// Scalar ABI type. Composite types are rejected.
    pub ty: DynSolType,
    /// Whether this parameter is indexed (events only). Indexed parameters
    /// appear in log topics; non-indexed parameters appear in log data.
    pub indexed: bool,
}

impl AbiParam {
    /// Creates a non-indexed parameter named `name` with type `ty`. Returns
    /// an error if `ty` is a composite type.
    pub fn new(name: impl Into<String>, ty: DynSolType) -> Result<Self, AbiError> {
        ensure_supported_type(&ty)?;
        Ok(Self { name: name.into(), ty, indexed: false })
    }

    /// Returns the canonical Solidity type string for this parameter (e.g.
    /// `"uint256"`, `"address"`).
    pub fn sol_type(&self) -> String {
        self.ty.to_string()
    }

    /// Sets the `indexed` flag (for event parameters). Builder-style.
    pub fn with_indexed(mut self, indexed: bool) -> Self {
        self.indexed = indexed;
        self
    }
}

/// A resolved function-call specification: four-byte selector and input
/// parameters.
#[derive(Debug, Clone)]
pub struct MethodSpec {
    /// Function selector (first four bytes of `keccak256(signature)`).
    pub selector: Selector,
    /// Input parameter schema, in ABI order.
    pub params: Vec<AbiParam>,
}

/// A resolved event specification: topic0 signature hash and parameters.
#[derive(Debug, Clone)]
pub struct EventSpec {
    /// Event signature hash (`keccak256(canonical_signature)`), stored as
    /// `topics[0]` in emitted logs.
    pub topic0: B256,
    /// Parameter schema, in ABI order (indexed and non-indexed interleaved).
    pub params: Vec<AbiParam>,
}

/// The kind of target signature parsed by [`parse_target_signature`].
#[derive(Debug, Clone)]
pub enum TargetSpec {
    /// A function call.
    Call(MethodSpec),
    /// An event log.
    Event(EventSpec),
}

/// ABI parse, type-resolution, or decode error.
#[derive(Debug, thiserror::Error)]
pub enum AbiError {
    /// Signature parsing failed.
    #[error("failed to parse signature: {0}")]
    Parse(String),
    /// A type is unsupported or unresolvable.
    #[error("invalid solidity type: {0}")]
    Type(String),
    /// Decoding calldata or event data failed.
    #[error("decode error: {0}")]
    Decode(String),
}

/// A reusable ABI decoder for one function's input tuple.
///
/// Construct once from a parameter schema and call [`CallDecoder::decode`] for
/// every matching transaction. The underlying `DynSolType::Tuple` is compiled
/// at construction so the hot path only walks the decoded value tree.
#[derive(Debug, Clone)]
pub struct CallDecoder {
    params: DynSolType,
}

impl CallDecoder {
    /// Compiles a decoder for the given parameter schema.
    pub fn new(params: &[AbiParam]) -> Self {
        Self { params: DynSolType::Tuple(params.iter().map(|param| param.ty.clone()).collect()) }
    }

    /// Decodes calldata (without the four-byte selector) into a vector of
    /// scalar [`DecodedValue`]s, in ABI order.
    pub fn decode(&self, data: &[u8]) -> Result<Vec<DecodedValue>, AbiError> {
        let decoded = self
            .params
            .abi_decode_params(data)
            .map_err(|error| AbiError::Decode(error.to_string()))?;
        let values = match decoded {
            DynSolValue::Tuple(values) => values,
            single => vec![single],
        };
        values.into_iter().map(decode_value).collect()
    }
}

/// A reusable ABI decoder for one non-anonymous event definition.
///
/// Construct once from a parameter schema and the event's topic0, then call
/// [`EventDecoder::decode`] for every matching log. The decoder caches the
/// indexed/non-indexed partition so the hot path only walks the decoded value
/// tree.
#[derive(Debug, Clone)]
pub struct EventDecoder {
    event: DynSolEvent,
    indexed: Box<[bool]>,
}

impl EventDecoder {
    pub fn new(params: &[AbiParam], topic0: B256) -> Result<Self, AbiError> {
        let event = DynSolEvent::new(
            Some(topic0),
            params.iter().filter(|param| param.indexed).map(|param| param.ty.clone()).collect(),
            DynSolType::Tuple(
                params
                    .iter()
                    .filter(|param| !param.indexed)
                    .map(|param| param.ty.clone())
                    .collect(),
            ),
        )
        .ok_or_else(|| AbiError::Decode("event has too many indexed parameters".into()))?;
        let indexed = params.iter().map(|param| param.indexed).collect();
        Ok(Self { event, indexed })
    }

    pub fn decode(&self, topics: &[B256], data: &[u8]) -> Result<Vec<DecodedValue>, AbiError> {
        let decoded = self
            .event
            .decode_log_parts(topics.iter().copied(), data)
            .map_err(|error| AbiError::Decode(error.to_string()))?;
        let mut indexed_values = decoded.indexed.into_iter();
        let mut body_values = decoded.body.into_iter();
        self.indexed
            .iter()
            .map(|indexed| {
                decode_value(
                    if *indexed { indexed_values.next() } else { body_values.next() }.ok_or_else(
                        || AbiError::Decode("decoded parameter count mismatch".into()),
                    )?,
                )
            })
            .collect()
    }
}

/// Parses a single ABI type string (e.g. `"uint256"`, `"address"`) and
/// rejects composite types.
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
        .map(|(idx, param)| -> Result<AbiParam, AbiError> {
            let ty = Specifier::<DynSolType>::resolve(param)
                .map_err(|error| AbiError::Type(format!("param `{}`: {error}", param.name)))?;
            let name = if param.name.is_empty() { format!("arg{idx}") } else { param.name.clone() };
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

/// Parses a human-readable Solidity function or event signature into a
/// [`TargetSpec`] (selector/topic0 + parameter schema).
///
/// Rejects anonymous events, events with more than three indexed parameters,
/// functions with no inputs, duplicate parameter names, and composite
/// parameter types.
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
    if event.inputs.iter().filter(|param| param.indexed).count() > 3 {
        return Err(AbiError::Parse(
            "non-anonymous events support at most three indexed parameters".into(),
        ));
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
                if param.name.is_empty() { format!("arg{index}") } else { param.name.clone() };
            if !names.insert(name.clone()) {
                return Err(AbiError::Parse(format!("duplicate parameter name `{name}`")));
            }

            Ok(AbiParam::new(name, ty)?.with_indexed(param.indexed))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;
    Ok(EventSpec { topic0: event.selector(), params })
}

/// One-shot event decoder: constructs an [`EventDecoder`] and decodes one
/// log. Prefer reusing an [`EventDecoder`] in the indexing hot path.
pub fn decode_event(
    params: &[AbiParam],
    topic0: B256,
    topics: &[B256],
    data: &[u8],
) -> Result<Vec<DecodedValue>, AbiError> {
    EventDecoder::new(params, topic0)?.decode(topics, data)
}

/// One-shot calldata decoder: constructs a [`CallDecoder`] and decodes one
/// call. Prefer reusing a [`CallDecoder`] in the indexing hot path.
pub fn decode_calldata(params: &[AbiParam], data: &[u8]) -> Result<Vec<DecodedValue>, AbiError> {
    CallDecoder::new(params).decode(data)
}

fn decode_value(value: DynSolValue) -> Result<DecodedValue, AbiError> {
    match value {
        DynSolValue::Bool(value) => Ok(DecodedValue::Bool(value)),
        DynSolValue::Address(value) => Ok(DecodedValue::Address(value)),
        DynSolValue::String(value) => Ok(DecodedValue::String(value)),
        DynSolValue::Bytes(value) => Ok(DecodedValue::Bytes(value.into())),
        DynSolValue::FixedBytes(word, size) => {
            Ok(DecodedValue::Bytes(word[..size].to_vec().into()))
        }
        DynSolValue::Function(value) => Ok(DecodedValue::Bytes(value.as_slice().to_vec().into())),
        DynSolValue::Uint(value, _) => Ok(DecodedValue::Uint(value)),
        DynSolValue::Int(value, _) => Ok(DecodedValue::Int(value)),
        _ => Err(AbiError::Decode("composite types not supported at decode time".into())),
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{U256, address, keccak256};
    use alloy::sol_types::{SolCall, SolEvent};

    use super::*;

    alloy::sol! {
        function transfer(address to, uint256 value) external returns (bool);
        function f_bytes32(bytes32 word) external returns (bool);

        event Transfer(address indexed from, address indexed to, uint256 value);
        event Message(string indexed key, string value);
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

    #[test]
    fn reuses_compiled_call_decoder() {
        let TargetSpec::Call(spec) =
            parse_target_signature("function transfer(address to, uint256 value)").unwrap()
        else {
            panic!("expected call")
        };
        let call = transferCall {
            to: address!("0000000000000000000000000000000000000001"),
            value: U256::from(42),
        };
        let calldata = call.abi_encode();
        let params = &calldata[transferCall::SELECTOR.len()..];
        let expected = vec![DecodedValue::Address(call.to), DecodedValue::Uint(call.value)];
        let decoder = CallDecoder::new(&spec.params);

        assert_eq!(decoder.decode(params).unwrap(), expected);
        assert_eq!(decoder.decode(params).unwrap(), expected);
        assert_eq!(decode_calldata(&spec.params, params).unwrap(), expected);
        assert!(decoder.decode(&[0; 31]).is_err());
    }

    #[test]
    fn reuses_compiled_event_decoder_and_preserves_parameter_order() {
        let TargetSpec::Event(spec) = parse_target_signature(
            "event Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap() else {
            panic!("expected event")
        };
        let event = Transfer {
            from: address!("0000000000000000000000000000000000000001"),
            to: address!("0000000000000000000000000000000000000002"),
            value: U256::from(42),
        };
        let topics = event.encode_topics().into_iter().map(Into::into).collect::<Vec<B256>>();
        let data = event.encode_data();
        let expected = vec![
            DecodedValue::Address(event.from),
            DecodedValue::Address(event.to),
            DecodedValue::Uint(event.value),
        ];
        let decoder = EventDecoder::new(&spec.params, spec.topic0).unwrap();

        assert_eq!(decoder.decode(&topics, &data).unwrap(), expected);
        assert_eq!(decoder.decode(&topics, &data).unwrap(), expected);
        assert_eq!(decode_event(&spec.params, spec.topic0, &topics, &data).unwrap(), expected);

        let mut wrong_topics = topics;
        wrong_topics[0] = B256::ZERO;
        assert!(decoder.decode(&wrong_topics, &data).is_err());
    }

    #[test]
    fn decodes_dynamic_indexed_values_as_topic_hashes() {
        let TargetSpec::Event(spec) =
            parse_target_signature("event Message(string indexed key, string value)").unwrap()
        else {
            panic!("expected event")
        };
        let key = keccak256("indexed key");
        let event = Message { key, value: "body value".to_string() };
        let topics = event.encode_topics().into_iter().map(Into::into).collect::<Vec<B256>>();
        let decoder = EventDecoder::new(&spec.params, spec.topic0).unwrap();

        assert_eq!(
            decoder.decode(&topics, &event.encode_data()).unwrap(),
            vec![
                DecodedValue::Bytes(key.as_slice().to_vec().into()),
                DecodedValue::String(event.value),
            ]
        );
    }

    #[test]
    fn rejects_events_with_more_than_three_indexed_parameters() {
        let params = (0..4)
            .map(|index| {
                AbiParam::new(format!("arg{index}"), DynSolType::Address)
                    .unwrap()
                    .with_indexed(true)
            })
            .collect::<Vec<_>>();

        assert!(EventDecoder::new(&params, B256::ZERO).is_err());
        assert!(
            parse_target_signature(
                "event Invalid(address indexed a, address indexed b, address indexed c, address indexed d)"
            )
            .is_err()
        );
    }
}
