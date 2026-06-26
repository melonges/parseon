use alloy::dyn_abi::DynSolType;
use alloy::primitives::keccak256;
use serde::{Deserialize, Serialize};

use super::types::{SqlKind, sanitize_column, sol_type_to_sql_kind};

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Canonical tuple string like `(address,uint256)` accepted by `DynSolType::from_str`.
    pub input_types: String,
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
///   `function transfer(address to, uint256 value) public virtual returns (bool)`
///   `transfer(address to, uint256 value)`
pub fn parse_signature(input: &str) -> Result<MethodSpec, AbiError> {
    let raw = input.trim().trim_end_matches(';').trim();
    let mut rest = raw;
    // Strip optional leading `function` keyword.
    if let Some(stripped) = rest.strip_prefix("function") {
        let after = stripped.trim_start();
        // Only treat as keyword if followed by an identifier char (function name).
        if after
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        {
            rest = after;
        }
    }

    // Extract the function name up to the first '('.
    let paren = rest
        .find('(')
        .ok_or_else(|| AbiError::Parse("missing '(' in signature".into()))?;
    let name = rest[..paren].trim().to_string();
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(AbiError::Parse(format!("invalid function name: {name}")));
    }

    // Find the matching ')' for the inputs group.
    let inputs_inner = match_parens(rest, paren)?;

    // Everything after the inputs group is irrelevant (returns, modifiers, ...).

    let param_tokens = split_top_level(&inputs_inner);
    let mut params = Vec::with_capacity(param_tokens.len());
    let mut canonical_types = Vec::with_capacity(param_tokens.len());

    for tok in param_tokens {
        let tok = tok.trim();
        if tok.is_empty() {
            continue;
        }
        let (sol_type, p_name) = split_type_and_name(tok)?;
        let canon = canonical_type(&sol_type);
        // Validate the type parses in alloy.
        DynSolType::parse(&canon).map_err(|e| AbiError::Type(format!("{canon}: {e}")))?;
        let column = sanitize_column(&p_name);
        let sql_kind = sol_type_to_sql_kind(&canon);
        params.push(ParamSpec {
            name: p_name,
            sol_type: canon.clone(),
            sql_kind,
            column,
        });
        canonical_types.push(canon);
    }

    let canonical_signature = format!("{name}({})", canonical_types.join(","));
    let selector_bytes = keccak256(canonical_signature.as_bytes());
    let selector = format!("0x{}", hex_encode(&selector_bytes[..4]));
    let input_types = format!("({})", canonical_types.join(","));

    Ok(MethodSpec {
        name,
        canonical_signature,
        selector,
        input_types,
        params,
    })
}

/// Return the content between the '(' at `open` and its matching ')'.
fn match_parens(s: &str, open: usize) -> Result<String, AbiError> {
    let bytes = s.as_bytes();
    if bytes.get(open) != Some(&b'(') {
        return Err(AbiError::Parse("expected '('".into()));
    }
    let mut depth = 0isize;
    for (i, &b) in bytes.iter().enumerate().skip(open) {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(s[open + 1..i].to_string());
                }
            }
            _ => {}
        }
    }
    Err(AbiError::Parse("unmatched '('".into()))
}

/// Split a comma-separated list respecting parentheses and brackets.
fn split_top_level(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0isize;
    let mut cur = String::new();
    for b in s.bytes() {
        match b {
            b'(' | b'[' => {
                depth += 1;
                cur.push(b as char);
            }
            b')' | b']' => {
                depth -= 1;
                cur.push(b as char);
            }
            b',' if depth == 0 => {
                out.push(std::mem::take(&mut cur));
            }
            _ => cur.push(b as char),
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Split a single param token into `(sol_type, name)`.
/// The type may be a base type, an array, or a tuple, optionally followed by
/// a parameter name.
fn split_type_and_name(tok: &str) -> Result<(String, String), AbiError> {
    let tok = tok.trim();
    // Parse the type portion.
    let (ty, end) = parse_type(tok, 0)?;
    let rest = tok[end..].trim();
    // The remainder, if any, is the parameter name.
    let name = if rest.is_empty() {
        String::new()
    } else {
        let id = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect::<String>();
        if id.is_empty() {
            return Err(AbiError::Parse(format!("invalid param name in: {tok}")));
        }
        id
    };
    Ok((ty, name))
}

/// Parse a Solidity type starting at `i`. Returns `(type_string, remaining)`.
fn parse_type(s: &str, mut i: usize) -> Result<(String, usize), AbiError> {
    let bytes = s.as_bytes();
    // Tuple: `(...)` possibly with trailing array suffixes.
    if bytes.get(i) == Some(&b'(') {
        let mut depth = 0isize;
        let start = i;
        while i < bytes.len() {
            match bytes[i] {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        i += 1;
                        break;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        if depth != 0 {
            return Err(AbiError::Parse("unmatched '(' in tuple type".into()));
        }
        let mut ty = s[start..i].to_string();
        ty.push_str(&parse_array_suffixes(s, &mut i));
        return Ok((ty, i));
    }
    // Base type: alphanumeric run (e.g. `uint256`, `address`, `bytes32`).
    let start = i;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    if i == start {
        return Err(AbiError::Parse(format!("expected type in: {s}")));
    }
    let mut ty = s[start..i].to_string();
    ty.push_str(&parse_array_suffixes(s, &mut i));
    Ok((ty, i))
}

/// Consume any trailing `[]` or `[N]` suffixes.
fn parse_array_suffixes(s: &str, i: &mut usize) -> String {
    let bytes = s.as_bytes();
    let mut out = String::new();
    while *i < bytes.len() && bytes[*i] == b'[' {
        let start = *i;
        while *i < bytes.len() && bytes[*i] != b']' {
            *i += 1;
        }
        if *i < bytes.len() {
            *i += 1; // consume ']'
            out.push_str(&s[start..*i]);
        } else {
            break;
        }
    }
    out
}

/// Canonicalize a Solidity type: `uint` -> `uint256`, `int` -> `int256`.
/// Recurses into tuples.
fn canonical_type(sol: &str) -> String {
    let s = sol.trim();
    // Handle trailing array suffixes.
    if let Some(bracket) = s.find('[') {
        let base = &s[..bracket];
        let suffix = &s[bracket..];
        return format!("{}{}", canonical_base(base), suffix);
    }
    canonical_base(s)
}

fn canonical_base(s: &str) -> String {
    let s = s.trim();
    if s.starts_with('(') && s.ends_with(')') {
        let inner = &s[1..s.len() - 1];
        let parts = split_top_level(inner);
        let canon: Vec<String> = parts
            .iter()
            .map(|p| {
                let (ty, _) =
                    split_type_and_name(p).unwrap_or((p.trim().to_string(), String::new()));
                canonical_type(&ty)
            })
            .collect();
        return format!("({})", canon.join(","));
    }
    match s {
        "uint" => "uint256".to_string(),
        "int" => "int256".to_string(),
        "fixed" => "fixed256x18".to_string(),
        "ufixed" => "ufixed256x18".to_string(),
        _ => s.to_string(),
    }
}

pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

/// Decode a hex string (no `0x` prefix) into bytes.
pub fn hex_decode(s: &str) -> Vec<u8> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    let mut out = Vec::with_capacity(s.len() / 2);
    let mut chars = s.chars();
    while let (Some(h), Some(l)) = (chars.next(), chars.next()) {
        if let (Some(hi), Some(lo)) = (h.to_digit(16), l.to_digit(16)) {
            out.push(((hi << 4) | lo) as u8);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_transfer() {
        let spec = parse_signature(
            "function transfer(address to, uint256 value) public virtual returns (bool)",
        )
        .unwrap();
        assert_eq!(spec.name, "transfer");
        assert_eq!(spec.canonical_signature, "transfer(address,uint256)");
        assert_eq!(spec.selector, "0xa9059cbb");
        assert_eq!(spec.input_types, "(address,uint256)");
        assert_eq!(spec.params.len(), 2);
        assert_eq!(spec.params[0].column, "param_to");
        assert_eq!(spec.params[1].column, "param_value");
    }

    #[test]
    fn parses_approve() {
        let spec =
            parse_signature("approve(address spender, uint256 amount) returns (bool)").unwrap();
        assert_eq!(spec.canonical_signature, "approve(address,uint256)");
        assert_eq!(spec.selector, "0x095ea7b3");
    }

    #[test]
    fn parses_no_name_params() {
        let spec = parse_signature("transfer(address,uint256)").unwrap();
        assert_eq!(spec.canonical_signature, "transfer(address,uint256)");
        assert_eq!(spec.params[0].column, "param_arg");
    }

    #[test]
    fn parses_array_param() {
        let spec = parse_signature("doThing(uint256[] ids, address[] users)").unwrap();
        assert_eq!(spec.canonical_signature, "doThing(uint256[],address[])");
        assert_eq!(spec.params[0].sql_kind, SqlKind::Jsonb);
    }

    #[test]
    fn parses_uint_alias() {
        let spec = parse_signature("setUint(uint v)").unwrap();
        assert_eq!(spec.canonical_signature, "setUint(uint256)");
        assert_eq!(spec.params[0].sql_kind, SqlKind::Numeric);
    }
}
