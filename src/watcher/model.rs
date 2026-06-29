use alloy::primitives::Address;
use serde::Serialize;

use crate::abi::ParamSpec;
use crate::db::monitor_repo::MonitorRow;

/// In-memory representation of a monitor, ready for matching/decoding.
#[derive(Debug, Clone, Serialize)]
pub struct Monitor {
    pub id: i64,
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

    /// Parse a 0x-prefixed selector string into exactly 4 bytes.
    pub fn parse_selector(s: &str) -> Result<[u8; 4], anyhow::Error> {
        let h = s.strip_prefix("0x").unwrap_or(s);
        let bytes =
            alloy::hex::decode(h).map_err(|e| anyhow::anyhow!("invalid selector {s}: {e}"))?;
        bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("selector must contain exactly 4 bytes: {s}"))
    }
}

impl TryFrom<&MonitorRow> for Monitor {
    type Error = anyhow::Error;

    fn try_from(row: &MonitorRow) -> Result<Self, Self::Error> {
        let address: Address = row
            .address
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid monitor address {}: {e}", row.address))?;
        let params: Vec<ParamSpec> = row.param_schema.0.clone();
        let input_types = if params.is_empty() {
            String::from("()")
        } else {
            format!(
                "({})",
                params
                    .iter()
                    .map(|p| p.sol_type.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            )
        };

        Ok(Self {
            id: row.id,
            address,
            selector: Self::parse_selector(&row.selector)?,
            name: row.name.clone(),
            canonical_signature: row.signature.clone(),
            input_types,
            params,
            start_block: row.start_block,
            end_block: row.end_block,
            cursor: row.cursor,
            completed: row.completed,
            enabled: row.enabled,
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use sqlx::types::Json;

    use super::*;

    fn row(address: &str, selector: &str) -> MonitorRow {
        MonitorRow {
            id: 1,
            address: address.to_string(),
            name: "transfer".to_string(),
            signature: "transfer(address,uint256)".to_string(),
            selector: selector.to_string(),
            param_schema: Json(Vec::new()),
            start_block: 1,
            end_block: Some(2),
            cursor: None,
            completed: false,
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn converts_valid_monitor_row() {
        let monitor = Monitor::try_from(&row(
            "0x0000000000000000000000000000000000000001",
            "0xa9059cbb",
        ))
        .unwrap();
        assert_eq!(monitor.selector, [0xa9, 0x05, 0x9c, 0xbb]);
        assert_eq!(monitor.start_block, 1);
    }

    #[test]
    fn rejects_invalid_runtime_fields() {
        assert!(Monitor::try_from(&row("not-an-address", "0xa9059cbb")).is_err());
        assert!(
            Monitor::try_from(&row("0x0000000000000000000000000000000000000001", "0x1234",))
                .is_err()
        );
    }
}
