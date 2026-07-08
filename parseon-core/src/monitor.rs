use super::filter::Filter;
use super::{BlockNumber, Chain, Cursor, MonitorId, Target};

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

    pub fn needs_block(&self, block_number: BlockNumber) -> bool {
        self.covers(block_number) && self.cursor.0.is_none_or(|cursor| cursor < block_number)
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
    fn applies_cursor_progress() {
        let mut monitor = monitor();
        assert!(monitor.needs_block(10));
        monitor.cursor = Cursor(Some(10));
        assert!(!monitor.needs_block(10));
        assert!(monitor.needs_block(11));
    }
}
