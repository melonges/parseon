use std::num::NonZeroU64;

use super::monitor::Monitor;
use super::BlockNumber;

pub fn plan_blocks(
    monitors: &[&Monitor],
    finalized_head: BlockNumber,
    batch_size: NonZeroU64,
) -> Vec<BlockNumber> {
    let mut wanted = Vec::new();
    for monitor in monitors {
        let Some(from) = monitor.next_block() else {
            continue;
        };
        let to = monitor
            .end_block
            .unwrap_or(finalized_head)
            .min(finalized_head);
        if from > to {
            continue;
        }
        let to = to.min(from.saturating_add(batch_size.get() - 1));
        wanted.extend(from..=to);
    }
    wanted.sort_unstable();
    wanted.dedup();
    wanted
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;

    use super::*;
    use crate::filter::Filter;
    use crate::monitor::Monitor;
    use crate::{CallTarget, Cursor, Target};

    fn monitor(id: u64, start: u64, cursor: Option<u64>, end: Option<u64>) -> Monitor {
        Monitor {
            id: crate::MonitorId::new(id).unwrap(),
            chain: crate::Chain::new(1),
            target: Target::Call(CallTarget {
                address: Address::ZERO,
                selector: [0; 4].into(),
                inputs: Vec::new(),
            }),
            start_block: start,
            end_block: end,
            cursor: Cursor(cursor),
            completed: false,
            enabled: true,
            filter: Filter::All,
        }
    }

    #[test]
    fn deduplicates_and_bounds_ranges() {
        let first = monitor(1, 10, None, None);
        let second = monitor(2, 11, None, Some(12));
        assert_eq!(plan_blocks(&[&first, &second], 20, NonZeroU64::new(3).unwrap()), vec![10, 11, 12]);
    }

    #[test]
    fn skips_ranges_beyond_head() {
        let monitor = monitor(1, 20, None, None);
        assert!(plan_blocks(&[&monitor], 19, NonZeroU64::new(10).unwrap()).is_empty());
    }
}
