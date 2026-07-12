use std::fmt;

use alloy::dyn_abi::{DynSolType, DynSolValue};
use serde::{Deserialize, Serialize};

use crate::abi::AbiParam;
use crate::{Address, B256, BlockNumber, DecodedValue, I256, Target, U256};

pub const FILTER_VERSION: i16 = 1;
const MAX_BYTES: usize = 16 * 1024;
const MAX_NODES: usize = 128;
const MAX_DEPTH: usize = 16;
const MAX_CHILDREN: usize = 32;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FilterExpression {
    And(AndExpression),
    Or(OrExpression),
    Not(NotExpression),
    Compare(Comparison),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AndExpression {
    pub and: Vec<FilterExpression>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrExpression {
    pub or: Vec<FilterExpression>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotExpression {
    pub not: Box<FilterExpression>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Comparison {
    pub field: String,
    pub op: ComparisonOperator,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonOperator {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl ComparisonOperator {
    const fn ordered(self) -> bool {
        matches!(self, Self::Gt | Self::Gte | Self::Lt | Self::Lte)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilterDefinition {
    pub version: i16,
    pub expression: FilterExpression,
}

impl FilterDefinition {
    pub fn prepare(
        expression: FilterExpression,
        target: &Target,
    ) -> Result<(Self, Filter), FilterError> {
        validate_limits(&expression)?;
        let (expression, compiled) = compile_expression(expression, target, "/filter")?;
        Ok((
            Self {
                version: FILTER_VERSION,
                expression,
            },
            Filter::Expr(compiled),
        ))
    }

    pub fn compile(&self, target: &Target) -> Result<Filter, FilterError> {
        if self.version != FILTER_VERSION {
            return Err(FilterError::at(
                "/filter_version",
                format!("unsupported filter version {}", self.version),
            ));
        }
        validate_limits(&self.expression)?;
        Ok(Filter::Expr(
            compile_expression(self.expression.clone(), target, "/filter")?.1,
        ))
    }
}

#[derive(Debug, Clone, Default)]
pub enum Filter {
    #[default]
    All,
    Expr(CompiledExpression),
}

impl Filter {
    pub fn evaluate(&self, context: FilterContext<'_>) -> Result<bool, FilterError> {
        match self {
            Self::All => Ok(true),
            Self::Expr(expression) => expression.evaluate(context),
        }
    }
}

#[derive(Debug, Clone)]
pub enum FilterContext<'a> {
    Call {
        block_number: BlockNumber,
        tx_hash: B256,
        from: Address,
        to: Address,
        params: &'a [DecodedValue],
    },
    Event {
        block_number: BlockNumber,
        tx_hash: B256,
        emitter: Address,
        log_index: u64,
        params: &'a [DecodedValue],
    },
}

impl<'a> FilterContext<'a> {
    fn params(&self) -> &'a [DecodedValue] {
        match self {
            Self::Call { params, .. } | Self::Event { params, .. } => params,
        }
    }
}

#[derive(Debug, Clone)]
pub enum FilterSample {
    Call {
        block_number: BlockNumber,
        tx_hash: B256,
        from: Address,
        to: Address,
        params: serde_json::Map<String, serde_json::Value>,
    },
    Event {
        block_number: BlockNumber,
        tx_hash: B256,
        emitter: Address,
        log_index: u64,
        params: serde_json::Map<String, serde_json::Value>,
    },
}

#[derive(Debug, Clone)]
pub struct FilterPreview {
    pub filter: FilterExpression,
    pub matches: bool,
}

pub fn preview(
    target: &Target,
    expression: FilterExpression,
    sample: FilterSample,
) -> Result<FilterPreview, FilterError> {
    let (definition, filter) = FilterDefinition::prepare(expression, target)?;
    let matches = match (target, sample) {
        (
            Target::Call(target),
            FilterSample::Call {
                block_number,
                tx_hash,
                from,
                to,
                params,
            },
        ) => {
            let values = decode_sample_params(&target.inputs, false, params)?;
            filter.evaluate(FilterContext::Call {
                block_number,
                tx_hash,
                from,
                to,
                params: &values,
            })?
        }
        (
            Target::Event(target),
            FilterSample::Event {
                block_number,
                tx_hash,
                emitter,
                log_index,
                params,
            },
        ) => {
            let values = decode_sample_params(&target.params, true, params)?;
            filter.evaluate(FilterContext::Event {
                block_number,
                tx_hash,
                emitter,
                log_index,
                params: &values,
            })?
        }
        _ => {
            return Err(FilterError::at(
                "/sample/kind",
                "sample kind does not match signature",
            ));
        }
    };
    Ok(FilterPreview {
        filter: definition.expression,
        matches,
    })
}

#[derive(Debug, Clone)]
pub enum CompiledExpression {
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Compare {
        field: ResolvedField,
        op: ComparisonOperator,
        value: FilterValue,
    },
}

impl CompiledExpression {
    fn evaluate(&self, context: FilterContext<'_>) -> Result<bool, FilterError> {
        match self {
            Self::And(expressions) => {
                for expression in expressions {
                    if !expression.evaluate(context.clone())? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Self::Or(expressions) => {
                for expression in expressions {
                    if expression.evaluate(context.clone())? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Self::Not(expression) => Ok(!expression.evaluate(context)?),
            Self::Compare { field, op, value } => {
                compare(resolve_value(field, &context)?, *op, value)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum ResolvedField {
    BlockNumber,
    TransactionHash,
    CallFrom,
    CallTo,
    EventEmitter,
    EventLogIndex,
    Parameter(usize),
}

#[derive(Debug, Clone)]
pub enum FilterValue {
    Uint(U256),
    Int(I256),
    Bool(bool),
    Address(Address),
    String(String),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone)]
enum ValueType {
    Uint(usize),
    Int(usize),
    Bool,
    Address,
    String,
    Bytes(Option<usize>),
}

#[derive(Clone, Copy)]
enum RuntimeValue<'a> {
    Uint(U256),
    Int(I256),
    Bool(bool),
    Address(Address),
    String(&'a str),
    Bytes32(B256),
    Bytes(&'a [u8]),
}

fn validate_limits(expression: &FilterExpression) -> Result<(), FilterError> {
    let bytes = serde_json::to_vec(expression)
        .map_err(|error| FilterError::at("/filter", error.to_string()))?
        .len();
    if bytes > MAX_BYTES {
        return Err(FilterError::at(
            "/filter",
            format!("canonical filter exceeds {MAX_BYTES} bytes"),
        ));
    }
    fn visit(
        expression: &FilterExpression,
        depth: usize,
        nodes: &mut usize,
        path: &str,
    ) -> Result<(), FilterError> {
        *nodes += 1;
        if *nodes > MAX_NODES {
            return Err(FilterError::at(
                path,
                format!("filter exceeds {MAX_NODES} nodes"),
            ));
        }
        if depth > MAX_DEPTH {
            return Err(FilterError::at(
                path,
                format!("filter exceeds depth {MAX_DEPTH}"),
            ));
        }
        let children = match expression {
            FilterExpression::And(value) => Some((&value.and, "and")),
            FilterExpression::Or(value) => Some((&value.or, "or")),
            FilterExpression::Not(value) => {
                return visit(&value.not, depth + 1, nodes, &format!("{path}/not"));
            }
            FilterExpression::Compare(_) => None,
        };
        if let Some((children, key)) = children {
            if !(2..=MAX_CHILDREN).contains(&children.len()) {
                return Err(FilterError::at(
                    format!("{path}/{key}"),
                    format!("expected 2 to {MAX_CHILDREN} expressions"),
                ));
            }
            for (index, child) in children.iter().enumerate() {
                visit(child, depth + 1, nodes, &format!("{path}/{key}/{index}"))?;
            }
        }
        Ok(())
    }
    visit(expression, 1, &mut 0, "/filter")
}

fn compile_expression(
    expression: FilterExpression,
    target: &Target,
    path: &str,
) -> Result<(FilterExpression, CompiledExpression), FilterError> {
    Ok(match expression {
        FilterExpression::And(value) => {
            let values = value
                .and
                .into_iter()
                .enumerate()
                .map(|(index, expression)| {
                    compile_expression(expression, target, &format!("{path}/and/{index}"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            (
                FilterExpression::And(AndExpression {
                    and: values.iter().map(|value| value.0.clone()).collect(),
                }),
                CompiledExpression::And(values.into_iter().map(|value| value.1).collect()),
            )
        }
        FilterExpression::Or(value) => {
            let values = value
                .or
                .into_iter()
                .enumerate()
                .map(|(index, expression)| {
                    compile_expression(expression, target, &format!("{path}/or/{index}"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            (
                FilterExpression::Or(OrExpression {
                    or: values.iter().map(|value| value.0.clone()).collect(),
                }),
                CompiledExpression::Or(values.into_iter().map(|value| value.1).collect()),
            )
        }
        FilterExpression::Not(value) => {
            let (source, compiled) =
                compile_expression(*value.not, target, &format!("{path}/not"))?;
            (
                FilterExpression::Not(NotExpression {
                    not: Box::new(source),
                }),
                CompiledExpression::Not(Box::new(compiled)),
            )
        }
        FilterExpression::Compare(value) => {
            let (field, ty) = resolve_field(target, &value.field, &format!("{path}/field"))?;
            if value.op.ordered() && !matches!(&ty, ValueType::Uint(_) | ValueType::Int(_)) {
                return Err(FilterError::at(
                    format!("{path}/op"),
                    format!("operator {:?} requires an integer field", value.op)
                        .to_ascii_lowercase(),
                ));
            }
            let (literal, canonical) = parse_value(&ty, value.value, &format!("{path}/value"))?;
            let source = FilterExpression::Compare(Comparison {
                field: value.field,
                op: value.op,
                value: canonical,
            });
            (
                source,
                CompiledExpression::Compare {
                    field,
                    op: value.op,
                    value: literal,
                },
            )
        }
    })
}

fn resolve_field(
    target: &Target,
    field: &str,
    path: &str,
) -> Result<(ResolvedField, ValueType), FilterError> {
    let common = match field {
        "block.number" => Some((ResolvedField::BlockNumber, ValueType::Uint(64))),
        "tx.hash" => Some((ResolvedField::TransactionHash, ValueType::Bytes(Some(32)))),
        _ => None,
    };
    if let Some(value) = common {
        return Ok(value);
    }
    match (target, field) {
        (Target::Call(_), "tx.from") => return Ok((ResolvedField::CallFrom, ValueType::Address)),
        (Target::Call(_), "tx.to") => return Ok((ResolvedField::CallTo, ValueType::Address)),
        (Target::Event(_), "event.emitter") => {
            return Ok((ResolvedField::EventEmitter, ValueType::Address));
        }
        (Target::Event(_), "event.log_index") => {
            return Ok((ResolvedField::EventLogIndex, ValueType::Uint(64)));
        }
        _ => {}
    }
    let name = field
        .strip_prefix("params.")
        .ok_or_else(|| FilterError::at(path, format!("unknown field `{field}`")))?;
    let params = match target {
        Target::Call(target) => &target.inputs,
        Target::Event(target) => &target.params,
    };
    let (index, param) = params
        .iter()
        .enumerate()
        .find(|(_, param)| param.name == name)
        .ok_or_else(|| FilterError::at(path, format!("unknown parameter `{name}`")))?;
    Ok((
        ResolvedField::Parameter(index),
        value_type(param, matches!(target, Target::Event(_)))?,
    ))
}

fn value_type(param: &AbiParam, event: bool) -> Result<ValueType, FilterError> {
    if event && param.indexed && matches!(&param.ty, DynSolType::String | DynSolType::Bytes) {
        return Ok(ValueType::Bytes(Some(32)));
    }
    Ok(match &param.ty {
        DynSolType::Uint(bits) => ValueType::Uint(*bits),
        DynSolType::Int(bits) => ValueType::Int(*bits),
        DynSolType::Bool => ValueType::Bool,
        DynSolType::Address => ValueType::Address,
        DynSolType::String => ValueType::String,
        DynSolType::Bytes => ValueType::Bytes(None),
        DynSolType::FixedBytes(size) => ValueType::Bytes(Some(*size)),
        DynSolType::Function => ValueType::Bytes(Some(24)),
        ty => {
            return Err(FilterError::at(
                "/filter",
                format!("unsupported parameter type `{ty}`"),
            ));
        }
    })
}

fn parse_value(
    ty: &ValueType,
    value: serde_json::Value,
    path: &str,
) -> Result<(FilterValue, serde_json::Value), FilterError> {
    let string = |value: serde_json::Value, expected: &str| {
        value
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| FilterError::at(path, format!("expected {expected}")))
    };
    Ok(match ty {
        ValueType::Uint(bits) => {
            let value = string(value, "a decimal string")?;
            if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(FilterError::at(path, "expected an unsigned decimal string"));
            }
            let DynSolValue::Uint(value, _) = DynSolType::Uint(*bits)
                .coerce_str(&value)
                .map_err(|error| FilterError::at(path, error.to_string()))?
            else {
                unreachable!()
            };
            (
                FilterValue::Uint(value),
                serde_json::Value::String(value.to_string()),
            )
        }
        ValueType::Int(bits) => {
            let value = string(value, "a signed decimal string")?;
            let digits = value.strip_prefix('-').unwrap_or(&value);
            if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(FilterError::at(path, "expected a signed decimal string"));
            }
            let DynSolValue::Int(value, _) = DynSolType::Int(*bits)
                .coerce_str(&value)
                .map_err(|error| FilterError::at(path, error.to_string()))?
            else {
                unreachable!()
            };
            (
                FilterValue::Int(value),
                serde_json::Value::String(value.to_string()),
            )
        }
        ValueType::Bool => {
            let value = value
                .as_bool()
                .ok_or_else(|| FilterError::at(path, "expected a boolean"))?;
            (FilterValue::Bool(value), serde_json::Value::Bool(value))
        }
        ValueType::Address => {
            let value = string(value, "a 20-byte 0x-prefixed address")?;
            if value.len() != 42
                || !value.starts_with("0x")
                || !value[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(FilterError::at(
                    path,
                    "expected a 20-byte 0x-prefixed address",
                ));
            }
            let address = value
                .parse::<Address>()
                .map_err(|error| FilterError::at(path, error.to_string()))?;
            (
                FilterValue::Address(address),
                serde_json::Value::String(format!("{address:#x}")),
            )
        }
        ValueType::String => {
            let value = string(value, "a string")?;
            (
                FilterValue::String(value.clone()),
                serde_json::Value::String(value),
            )
        }
        ValueType::Bytes(size) => {
            let value = string(value, "0x-prefixed hexadecimal bytes")?;
            let hex = value
                .strip_prefix("0x")
                .ok_or_else(|| FilterError::at(path, "expected 0x-prefixed hexadecimal bytes"))?;
            if hex.len() % 2 != 0 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(FilterError::at(
                    path,
                    "expected even-length hexadecimal bytes",
                ));
            }
            let bytes = alloy::hex::decode(hex)
                .map_err(|error| FilterError::at(path, error.to_string()))?;
            if let Some(size) = size
                && bytes.len() != *size
            {
                return Err(FilterError::at(path, format!("expected {size} bytes")));
            }
            let canonical = format!("0x{}", alloy::hex::encode(&bytes));
            (
                FilterValue::Bytes(bytes),
                serde_json::Value::String(canonical),
            )
        }
    })
}

fn decode_sample_params(
    params: &[AbiParam],
    event: bool,
    mut values: serde_json::Map<String, serde_json::Value>,
) -> Result<Vec<DecodedValue>, FilterError> {
    if values.len() != params.len() {
        return Err(FilterError::at(
            "/sample/params",
            "sample parameters must exactly match the ABI schema",
        ));
    }
    params
        .iter()
        .map(|param| {
            let path = format!("/sample/params/{}", param.name);
            let value = values
                .remove(&param.name)
                .ok_or_else(|| FilterError::at(&path, "missing ABI parameter"))?;
            Ok(
                match parse_value(&value_type(param, event)?, value, &path)?.0 {
                    FilterValue::Uint(value) => DecodedValue::Uint(value),
                    FilterValue::Int(value) => DecodedValue::Int(value),
                    FilterValue::Bool(value) => DecodedValue::Bool(value),
                    FilterValue::Address(value) => DecodedValue::Address(value),
                    FilterValue::String(value) => DecodedValue::String(value),
                    FilterValue::Bytes(value) => DecodedValue::Bytes(value),
                },
            )
        })
        .collect()
}

fn resolve_value<'a>(
    field: &ResolvedField,
    context: &FilterContext<'a>,
) -> Result<RuntimeValue<'a>, FilterError> {
    Ok(match (field, context) {
        (
            ResolvedField::BlockNumber,
            FilterContext::Call { block_number, .. } | FilterContext::Event { block_number, .. },
        ) => RuntimeValue::Uint(U256::from(*block_number)),
        (
            ResolvedField::TransactionHash,
            FilterContext::Call { tx_hash, .. } | FilterContext::Event { tx_hash, .. },
        ) => RuntimeValue::Bytes32(*tx_hash),
        (ResolvedField::CallFrom, FilterContext::Call { from, .. }) => RuntimeValue::Address(*from),
        (ResolvedField::CallTo, FilterContext::Call { to, .. }) => RuntimeValue::Address(*to),
        (ResolvedField::EventEmitter, FilterContext::Event { emitter, .. }) => {
            RuntimeValue::Address(*emitter)
        }
        (ResolvedField::EventLogIndex, FilterContext::Event { log_index, .. }) => {
            RuntimeValue::Uint(U256::from(*log_index))
        }
        (ResolvedField::Parameter(index), context) => match context
            .params()
            .get(*index)
            .ok_or_else(|| FilterError::at("/filter", "decoded parameter index is missing"))?
        {
            DecodedValue::Uint(value) => RuntimeValue::Uint(*value),
            DecodedValue::Int(value) => RuntimeValue::Int(*value),
            DecodedValue::Bool(value) => RuntimeValue::Bool(*value),
            DecodedValue::Address(value) => RuntimeValue::Address(*value),
            DecodedValue::String(value) => RuntimeValue::String(value),
            DecodedValue::Bytes(value) => RuntimeValue::Bytes(value),
        },
        _ => {
            return Err(FilterError::at(
                "/filter",
                "field is unavailable in this evaluation context",
            ));
        }
    })
}

fn compare(
    left: RuntimeValue<'_>,
    op: ComparisonOperator,
    right: &FilterValue,
) -> Result<bool, FilterError> {
    fn apply<T: PartialEq + PartialOrd>(left: T, op: ComparisonOperator, right: T) -> bool {
        match op {
            ComparisonOperator::Eq => left == right,
            ComparisonOperator::Ne => left != right,
            ComparisonOperator::Gt => left > right,
            ComparisonOperator::Gte => left >= right,
            ComparisonOperator::Lt => left < right,
            ComparisonOperator::Lte => left <= right,
        }
    }
    Ok(match (left, right) {
        (RuntimeValue::Uint(left), FilterValue::Uint(right)) => apply(left, op, *right),
        (RuntimeValue::Int(left), FilterValue::Int(right)) => apply(left, op, *right),
        (RuntimeValue::Bool(left), FilterValue::Bool(right)) => apply(left, op, *right),
        (RuntimeValue::Address(left), FilterValue::Address(right)) => apply(left, op, *right),
        (RuntimeValue::String(left), FilterValue::String(right)) => apply(left, op, right.as_str()),
        (RuntimeValue::Bytes32(left), FilterValue::Bytes(right)) => {
            apply(left.as_slice(), op, right.as_slice())
        }
        (RuntimeValue::Bytes(left), FilterValue::Bytes(right)) => apply(left, op, right.as_slice()),
        _ => {
            return Err(FilterError::at(
                "/filter",
                "resolved field type does not match compiled literal",
            ));
        }
    })
}

#[derive(Debug, Clone)]
pub struct FilterError {
    path: String,
    message: String,
}

impl FilterError {
    fn at(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}

impl std::error::Error for FilterError {}
