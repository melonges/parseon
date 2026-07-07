use alloy::primitives::Address;

use super::filter::Filter;
use super::{BlockNumber, Chain, Cursor, MonitorId, Selector, Target};

#[derive(Debug, Clone)]
pub struct Monitor {
    pub id: MonitorId,
    pub chain: Chain,
    pub target: Target,
    pub start_block: BlockNumber,
    pub end_block: Option<BlockNumber>,
    pub cursor: Cursor,
    pub completed: bool,
    pub enabled: bool,
    pub filter: Filter,
}

impl Monitor {
    pub fn covers(&self, block_number: BlockNumber) -> bool {
        self.enabled
            && !self.completed
            && block_number >= self.start_block
            && self.end_block.is_none_or(|end| block_number <= end)
    }

    pub fn next_block(&self) -> Option<BlockNumber> {
        self.cursor.next(self.start_block)
    }

    pub fn matches_call(&self, address: Address, selector: Selector) -> bool {
        matches!(&self.target, Target::Call(target) if target.address == address && target.selector == selector)
    }

    pub fn matches_event(&self, address: Address, topic0: alloy::primitives::B256) -> bool {
        matches!(&self.target, Target::Event(target) if target.address == address && target.topic0 == topic0)
    }
}

#[cfg(test)]
mod tests {
    use alloy::dyn_abi::DynSolType;
    use alloy::primitives::Address;

    use super::*;
    use crate::abi::AbiParam;

    fn monitor() -> Monitor {
        Monitor {
            id: MonitorId::new(1).unwrap(),
            chain: Chain::new(1),
            target: Target::Call(crate::CallTarget {
                address: Address::ZERO,
                selector: [1, 2, 3, 4].into(),
                signature: "f(uint256)".into(),
                inputs: vec![AbiParam::new("value", DynSolType::Uint(256)).unwrap()],
            }),
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
        assert!(monitor.matches_call(Address::ZERO, [1, 2, 3, 4].into()));
        assert!(!monitor.matches_call(Address::ZERO, [4, 3, 2, 1].into()));
    }
}
