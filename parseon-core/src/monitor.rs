//! The immutable monitor definition and its range/cursor helpers.
//!
//! A monitor is the user-defined indexing rule:
//! ```text
//! Monitor = Target + block range + optional Filter + Cursor
//! ```
//!
//! The monitor's chain, target, block range, and filter are fixed at creation.
//! Only `enabled` is user-mutable (for pause and resume); the worker owns
//! `cursor` and `completed` state.

use super::filter::Filter;
use super::{BlockNumber, Chain, Cursor, MonitorId, Target};

/// One user-defined indexing rule.
///
/// Workers reload monitors every poll from storage so that pause/resume and
/// out-of-band cursor repairs take effect without a restart. The
/// [`Monitor::covers`] and [`Monitor::needs_block`] helpers encode the
/// monitor's immutable range and mutable cursor progress so the scheduler
/// can plan blocks without re-deriving those invariants.
#[derive(Debug, Clone)]
pub struct Monitor {
    /// Surrogate monitor identifier.
    pub id: MonitorId,
    /// Owning chain.
    pub chain: Chain,
    /// What the monitor matches (call or event).
    pub target: Target,
    /// First block to index (inclusive).
    pub start_block: BlockNumber,
    /// Last block to index (inclusive), or `None` for live indexing.
    pub end_block: Option<BlockNumber>,
    /// Last committed block, or `None` if no block has been committed yet.
    pub cursor: Cursor,
    /// Whether the monitor has reached its `end_block` and stopped.
    pub completed: bool,
    /// Whether the worker should index this monitor. User-mutable.
    pub enabled: bool,
    /// Compiled filter. `Filter::All` matches every result.
    pub filter: Filter,
}

impl Monitor {
    /// Returns `true` if `block_number` is within the monitor's `[start_block,
    /// end_block]` range and the monitor is enabled and not completed.
    pub fn covers(&self, block_number: BlockNumber) -> bool {
        self.enabled
            && !self.completed
            && block_number >= self.start_block
            && self.end_block.is_none_or(|end| block_number <= end)
    }

    /// Returns the next block this monitor should process, based on its
    /// cursor and `start_block`.
    pub fn next_block(&self) -> BlockNumber {
        self.cursor.next(self.start_block)
    }

    /// Returns `true` if the monitor's range covers `block_number` and its
    /// cursor has not yet advanced past it.
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
