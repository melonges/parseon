//! Immutable, ABI-aware JSON filter DSL.
//!
//! A filter is a bounded, versioned JSON AST compiled against a monitor's ABI
//! before indexing. It supports scalar equality, integer ordering, and
//! short-circuit boolean composition over decoded parameters and the metadata
//! already fetched for successful calls or events.
//!
//! ## Compilation
//!
//! [`FilterDefinition::prepare`] validates a [`FilterExpression`] against a
//! [`Target`], canonicalizes literal values, and produces a
//! [`Filter`] that the worker evaluates per decoded result. The compiled
//! representation is reused for every matching call or event in a monitor's
//! lifetime.
//!
//! ## Limits
//!
//! Filters are bounded to prevent untrusted input from exhausting the worker:
//! - `MAX_BYTES` (16 KiB): canonical JSON encoding size.
//! - `MAX_NODES` (128): total AST nodes.
//! - `MAX_DEPTH` (16): nesting depth.
//! - `MAX_CHILDREN` (32): `and`/`or` arity.
//!
//! ## Evaluation
//!
//! [`Filter::evaluate`] takes a borrowed [`FilterContext`] (which is [`Copy`])
//! and returns `Ok(bool)` or a [`FilterError`] if the compiled expression
//! references a field unavailable in the current context.

use std::fmt;

use alloy::dyn_abi::{DynSolType, DynSolValue};
use alloy::primitives::{I256, U256};
use serde::{Deserialize, Serialize};

use crate::abi::AbiParam;
use crate::{Address, B256, BlockNumber, Bytes, DecodedValue, Target};

/// The current filter DSL schema version. Bumped when the JSON shape changes
/// in a way that old compiled filters cannot handle.
pub const FILTER_VERSION: i16 = 1;
const MAX_BYTES: usize = 16 * 1024;
const MAX_NODES: usize = 128;
const MAX_DEPTH: usize = 16;
const MAX_CHILDREN: usize = 32;

/// The JSON filter AST root: a boolean combinator or a leaf comparison.
///
/// Serialized with `serde` as an untagged enum so callers can write
/// `{"and": [...]}`, `{"or": [...]}`, `{"not": {...}}`, or
/// `{"field": "...", "op": "...", "value": ...}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FilterExpression {
    /// Logical AND over two or more sub-expressions.
    And(AndExpression),
    /// Logical OR over two or more sub-expressions.
    Or(OrExpression),
    /// Logical NOT over one sub-expression.
    Not(NotExpression),
    /// A leaf comparison: field, operator, literal value.
    Compare(Comparison),
}

/// `{"and": [...]}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AndExpression {
    /// Sub-expressions, all of which must match.
    pub and: Vec<FilterExpression>,
}

/// `{"or": [...]}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrExpression {
    /// Sub-expressions, at least one of which must match.
    pub or: Vec<FilterExpression>,
}

/// `{"not": {...}}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotExpression {
    /// The negated sub-expression.
    pub not: Box<FilterExpression>,
}

/// `{"field": "...", "op": "...", "value": ...}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Comparison {
    /// Dotted field path (e.g. `"params.value"`, `"tx.from"`, `"block.number"`).
    pub field: String,
    /// Comparison operator.
    pub op: ComparisonOperator,
    /// Literal value to compare against. Typed and canonicalized during
    /// compilation.
    pub value: serde_json::Value,
}

/// Comparison operator for a [`Comparison`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonOperator {
    /// Equal.
    Eq,
    /// Not equal.
    Ne,
    /// Greater than (integers only).
    Gt,
    /// Greater than or equal (integers only).
    Gte,
    /// Less than (integers only).
    Lt,
    /// Less than or equal (integers only).
    Lte,
}

impl ComparisonOperator {
    const fn ordered(self) -> bool {
        matches!(self, Self::Gt | Self::Gte | Self::Lt | Self::Lte)
    }
}

/// A versioned, compiled filter definition persisted with a monitor.
///
/// `version` is checked on recompile so a future DSL change cannot silently
/// apply an old expression to a new evaluator.
#[derive(Debug, Clone, PartialEq)]
pub struct FilterDefinition {
    /// DSL schema version. Must equal [`FILTER_VERSION`].
    pub version: i16,
    /// The canonicalized (re-serialized) expression tree.
    pub expression: FilterExpression,
}

impl FilterDefinition {
    /// Validates, compiles, and canonicalizes `expression` against `target`.
    ///
    /// Returns both the persisted [`FilterDefinition`] (with canonicalized
    /// expression) and the runtime-evaluable [`Filter`].
    pub fn prepare(
        expression: FilterExpression,
        target: &Target,
    ) -> Result<(Self, Filter), FilterError> {
        validate_limits(&expression)?;
        let (expression, compiled) = compile_expression(expression, target, "/filter")?;
        Ok((Self { version: FILTER_VERSION, expression }, Filter::Expr(compiled)))
    }

    /// Recompiles a persisted definition against `target`, verifying the
    /// version and re-checking limits.
    pub fn compile(&self, target: &Target) -> Result<Filter, FilterError> {
        if self.version != FILTER_VERSION {
            return Err(FilterError::at(
                "/filter_version",
                format!("unsupported filter version {}", self.version),
            ));
        }
        validate_limits(&self.expression)?;
        Ok(Filter::Expr(compile_expression(self.expression.clone(), target, "/filter")?.1))
    }
}

/// A compiled filter ready for evaluation.
///
/// `All` matches every result; `Expr` is a compiled expression tree.
#[derive(Debug, Clone, Default)]
pub enum Filter {
    /// Matches every result. The default.
    #[default]
    All,
    /// Matches results satisfying the compiled expression.
    Expr(CompiledExpression),
}

impl Filter {
    /// Evaluates this filter against `context`. Returns `Ok(true)` if the
    /// result matches, `Ok(false)` if it does not, or an error if the
    /// compiled expression references an unavailable field.
    pub fn evaluate(&self, context: FilterContext<'_>) -> Result<bool, FilterError> {
        match self {
            Self::All => Ok(true),
            Self::Expr(expression) => expression.evaluate(context),
        }
    }
}

/// Borrowed view of one decoded result for filter evaluation.
///
/// All fields are `Copy` (including the `&[DecodedValue]` slice), so
/// [`Filter::evaluate`] can pass this by value without allocation.
#[derive(Debug, Clone, Copy)]
pub enum FilterContext<'a> {
    /// A decoded call context.
    Call {
        /// Block the call was included in.
        block_number: BlockNumber,
        /// Transaction hash.
        tx_hash: B256,
        /// Sender address.
        from: Address,
        /// Recipient address.
        to: Address,
        /// Decoded parameter values.
        params: &'a [DecodedValue],
    },
    /// A decoded event context.
    Event {
        /// Block the event was emitted in.
        block_number: BlockNumber,
        /// Transaction hash that emitted the log.
        tx_hash: B256,
        /// Emitter address.
        emitter: Address,
        /// Log index within the block.
        log_index: u64,
        /// Decoded parameter values.
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

/// A sample decoded result for filter preview (does not create a monitor).
///
/// Parameters are a JSON map keyed by parameter name; the preview path
/// decodes them into [`DecodedValue`]s using the target's ABI schema.
#[derive(Debug, Clone)]
pub enum FilterSample {
    /// A sample call result.
    Call {
        /// Block number.
        block_number: BlockNumber,
        /// Transaction hash.
        tx_hash: B256,
        /// Sender address.
        from: Address,
        /// Recipient address.
        to: Address,
        /// Sample parameter values keyed by parameter name.
        params: serde_json::Map<String, serde_json::Value>,
    },
    /// A sample event result.
    Event {
        /// Block number.
        block_number: BlockNumber,
        /// Transaction hash.
        tx_hash: B256,
        /// Emitter address.
        emitter: Address,
        /// Log index.
        log_index: u64,
        /// Sample parameter values keyed by parameter name.
        params: serde_json::Map<String, serde_json::Value>,
    },
}

/// Result of a filter preview: the canonicalized expression and whether the
/// sample matched.
#[derive(Debug, Clone)]
pub struct FilterPreview {
    /// Canonicalized filter expression.
    pub filter: FilterExpression,
    /// Whether the sample matched the filter.
    pub matches: bool,
}

/// Evaluates a filter expression against a sample without creating a monitor.
///
/// Compiles `expression` against the target parsed from `command.signature`,
/// decodes the sample parameters via the same ABI schema, and returns the
/// canonicalized expression together with the match result.
pub fn preview(
    target: &Target,
    expression: FilterExpression,
    sample: FilterSample,
) -> Result<FilterPreview, FilterError> {
    let (definition, filter) = FilterDefinition::prepare(expression, target)?;
    let matches = match (target, sample) {
        (Target::Call(target), FilterSample::Call { block_number, tx_hash, from, to, params }) => {
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
            FilterSample::Event { block_number, tx_hash, emitter, log_index, params },
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
            return Err(FilterError::at("/sample/kind", "sample kind does not match signature"));
        }
    };
    Ok(FilterPreview { filter: definition.expression, matches })
}

/// A compiled filter expression tree. Internal to [`Filter`].
#[derive(Debug, Clone)]
pub enum CompiledExpression {
    /// Short-circuit AND.
    And(Vec<Self>),
    /// Short-circuit OR.
    Or(Vec<Self>),
    /// Negation.
    Not(Box<Self>),
    /// A leaf comparison: resolved field, operator, typed literal.
    Compare { field: ResolvedField, op: ComparisonOperator, value: FilterValue },
}

impl CompiledExpression {
    fn evaluate(&self, context: FilterContext<'_>) -> Result<bool, FilterError> {
        match self {
            Self::And(expressions) => {
                for expression in expressions {
                    if !expression.evaluate(context)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Self::Or(expressions) => {
                for expression in expressions {
                    if expression.evaluate(context)? {
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

/// A resolved field path: either a metadata field or a decoded parameter
/// index.
#[derive(Debug, Clone)]
pub enum ResolvedField {
    /// `block.number`.
    BlockNumber,
    /// `tx.hash`.
    TransactionHash,
    /// `tx.from` (calls only).
    CallFrom,
    /// `tx.to` (calls only).
    CallTo,
    /// `event.emitter` (events only).
    EventEmitter,
    /// `event.log_index` (events only).
    EventLogIndex,
    /// `params.<name>`, resolved to the parameter's ABI index.
    Parameter(usize),
}

/// A typed literal value compiled from a [`Comparison`]'s JSON value.
#[derive(Debug, Clone)]
pub enum FilterValue {
    /// An unsigned integer literal.
    Uint(U256),
    /// A signed integer literal.
    Int(I256),
    /// A boolean literal.
    Bool(bool),
    /// An address literal.
    Address(Address),
    /// A string literal.
    String(String),
    /// A bytes literal (dynamic or fixed-size).
    Bytes(Bytes),
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
            return Err(FilterError::at(path, format!("filter exceeds {MAX_NODES} nodes")));
        }
        if depth > MAX_DEPTH {
            return Err(FilterError::at(path, format!("filter exceeds depth {MAX_DEPTH}")));
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
                FilterExpression::Not(NotExpression { not: Box::new(source) }),
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
            (source, CompiledExpression::Compare { field, op: value.op, value: literal })
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
    Ok((ResolvedField::Parameter(index), value_type(param, matches!(target, Target::Event(_)))?))
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
            return Err(FilterError::at("/filter", format!("unsupported parameter type `{ty}`")));
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
            (FilterValue::Uint(value), serde_json::Value::String(value.to_string()))
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
            (FilterValue::Int(value), serde_json::Value::String(value.to_string()))
        }
        ValueType::Bool => {
            let value =
                value.as_bool().ok_or_else(|| FilterError::at(path, "expected a boolean"))?;
            (FilterValue::Bool(value), serde_json::Value::Bool(value))
        }
        ValueType::Address => {
            let value = string(value, "a 20-byte 0x-prefixed address")?;
            if value.strip_prefix("0x").is_none_or(|hex| {
                hex.len() != 40 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit())
            }) {
                return Err(FilterError::at(path, "expected a 20-byte 0x-prefixed address"));
            }
            let address = value
                .parse::<Address>()
                .map_err(|error| FilterError::at(path, error.to_string()))?;
            (FilterValue::Address(address), serde_json::Value::String(format!("{address:#x}")))
        }
        ValueType::String => {
            let value = string(value, "a string")?;
            (FilterValue::String(value.clone()), serde_json::Value::String(value))
        }
        ValueType::Bytes(size) => {
            let value = string(value, "0x-prefixed hexadecimal bytes")?;
            let hex = value
                .strip_prefix("0x")
                .ok_or_else(|| FilterError::at(path, "expected 0x-prefixed hexadecimal bytes"))?;
            if hex.len() % 2 != 0 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(FilterError::at(path, "expected even-length hexadecimal bytes"));
            }
            let bytes = alloy::hex::decode(hex)
                .map_err(|error| FilterError::at(path, error.to_string()))?;
            if let Some(size) = size
                && bytes.len() != *size
            {
                return Err(FilterError::at(path, format!("expected {size} bytes")));
            }
            let canonical = format!("0x{}", alloy::hex::encode(&bytes));
            (FilterValue::Bytes(bytes.into()), serde_json::Value::String(canonical))
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
            Ok(match parse_value(&value_type(param, event)?, value, &path)?.0 {
                FilterValue::Uint(value) => DecodedValue::Uint(value),
                FilterValue::Int(value) => DecodedValue::Int(value),
                FilterValue::Bool(value) => DecodedValue::Bool(value),
                FilterValue::Address(value) => DecodedValue::Address(value),
                FilterValue::String(value) => DecodedValue::String(value),
                FilterValue::Bytes(value) => DecodedValue::Bytes(value),
            })
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
            DecodedValue::Bytes(value) => RuntimeValue::Bytes(&value[..]),
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
            apply(&left[..], op, &right[..])
        }
        (RuntimeValue::Bytes(left), FilterValue::Bytes(right)) => apply(left, op, &right[..]),
        _ => {
            return Err(FilterError::at(
                "/filter",
                "resolved field type does not match compiled literal",
            ));
        }
    })
}

/// A filter validation or evaluation error with a JSON-pointer-style path.
///
/// The path points at the offending sub-expression in the original filter
/// AST (e.g. `/filter/and/1/value`) so API callers can locate the error.
#[derive(Debug, Clone)]
pub struct FilterError {
    path: String,
    message: String,
}

impl FilterError {
    fn at(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self { path: path.into(), message: message.into() }
    }
}

impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}

impl std::error::Error for FilterError {}
