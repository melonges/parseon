use alloy::primitives::Address;

use crate::core::{Cursor, Target};
use crate::filter::Filter;

#[derive(Debug, Clone)]
pub struct Monitor {
    pub id: i64,
    pub target: Target,
    pub start_block: i64,
    pub end_block: Option<i64>,
    pub cursor: Cursor,
    pub completed: bool,
    pub enabled: bool,
    pub filter: Filter,
}

impl Monitor {
    pub fn covers(&self, block_number: i64) -> bool {
        self.enabled
            && !self.completed
            && block_number >= self.start_block
            && self.end_block.is_none_or(|end| block_number <= end)
    }

    pub fn next_block(&self) -> i64 {
        self.cursor.next(self.start_block)
    }

    pub fn matches(&self, address: Address, selector: &[u8]) -> bool {
        self.target.address == address && self.target.selector == selector
    }

    pub fn parse_selector(value: &str) -> anyhow::Result<[u8; 4]> {
        let value = value.strip_prefix("0x").unwrap_or(value);
        let bytes = alloy::hex::decode(value)
            .map_err(|error| anyhow::anyhow!("invalid selector {value}: {error}"))?;
        bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("selector must contain exactly 4 bytes: {value}"))
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;

    use super::*;

    fn monitor() -> Monitor {
        Monitor {
            id: 1,
            target: Target {
                address: Address::ZERO,
                selector: [1, 2, 3, 4],
                signature: "f(uint256)".into(),
                inputs: Vec::new(),
            },
            start_block: 10,
            end_block: Some(12),
            cursor: Cursor(None),
            completed: false,
            enabled: true,
            filter: Filter::All,
        }
    }

    #[test]
    fn applies_range_and_state() {
        let mut monitor = monitor();
        assert!(!monitor.covers(9));
        assert!(monitor.covers(10));
        assert!(monitor.covers(12));
        assert!(!monitor.covers(13));
        monitor.enabled = false;
        assert!(!monitor.covers(10));
    }

    #[test]
    fn matches_target() {
        let monitor = monitor();
        assert!(monitor.matches(Address::ZERO, &[1, 2, 3, 4]));
        assert!(!monitor.matches(Address::ZERO, &[4, 3, 2, 1]));
    }
}
