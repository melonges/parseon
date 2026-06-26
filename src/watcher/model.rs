use alloy::primitives::Address;
use serde::Serialize;

use crate::abi::ParamSpec;

/// In-memory representation of a monitor, ready for matching/decoding.
#[derive(Debug, Clone, Serialize)]
pub struct Monitor {
    pub id: i64,
    pub chain_id: i64,
    pub address: Address,
    /// Raw selector bytes (4 bytes).
    pub selector: [u8; 4],
    pub name: String,
    pub canonical_signature: String,
    /// Canonical tuple string like `(address,uint256)`.
    pub input_types: String,
    pub params: Vec<ParamSpec>,
    pub start_block: i64,
    pub end_block: Option<i64>,
    pub cursor: Option<i64>,
    pub completed: bool,
    pub enabled: bool,
}

impl Monitor {
    /// True if this monitor should process `block_number`.
    pub fn covers(&self, block_number: i64) -> bool {
        if !self.enabled || self.completed {
            return false;
        }
        if block_number < self.start_block {
            return false;
        }
        if let Some(end) = self.end_block
            && block_number > end
        {
            return false;
        }
        true
    }

    /// Next block this monitor needs to process.
    pub fn next_block(&self) -> i64 {
        match self.cursor {
            Some(c) => c + 1,
            None => self.start_block,
        }
    }

    /// Parse a 0x-prefixed selector string into 4 bytes.
    pub fn parse_selector(s: &str) -> [u8; 4] {
        let h = s.strip_prefix("0x").unwrap_or(s);
        let bytes = crate::abi::parse::hex_decode(h);
        let mut out = [0u8; 4];
        let n = bytes.len().min(4);
        out[..n].copy_from_slice(&bytes[..n]);
        out
    }
}
