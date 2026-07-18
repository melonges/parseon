use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::sync::Arc;

use super::BlockNumber;
use super::monitor::Monitor;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockPlan {
    pub(crate) block_number: BlockNumber,
    pub(crate) monitor_indices: Vec<usize>,
}

/// Plans finalized blocks with their covering monitors, capped to `batch_size` per monitor.
pub(crate) fn plan_blocks(
    monitors: &[Arc<Monitor>],
    finalized_head: BlockNumber,
    batch_size: NonZeroU64,
) -> Vec<BlockPlan> {
    let mut wanted = BTreeMap::<_, Vec<_>>::new();
    for (monitor_index, monitor) in monitors.iter().enumerate() {
        let from = monitor.next_block();
        let to = monitor.end_block.unwrap_or(finalized_head).min(finalized_head);
        let to = to.min(from.saturating_add(batch_size.get() - 1));
        for block_number in from..=to {
            wanted.entry(block_number).or_default().push(monitor_index);
        }
    }
    wanted
        .into_iter()
        .map(|(block_number, monitor_indices)| BlockPlan { block_number, monitor_indices })
        .collect()
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
        let first = Arc::new(monitor(1, 10, None, None));
        let second = Arc::new(monitor(2, 11, None, Some(12)));
        assert_eq!(
            plan_blocks(&[first, second], 20, NonZeroU64::new(3).unwrap()),
            vec![
                BlockPlan { block_number: 10, monitor_indices: vec![0] },
                BlockPlan { block_number: 11, monitor_indices: vec![0, 1] },
                BlockPlan { block_number: 12, monitor_indices: vec![0, 1] },
            ]
        );
    }

    #[test]
    fn skips_ranges_beyond_head() {
        let monitor = Arc::new(monitor(1, 20, None, None));
        assert!(plan_blocks(&[monitor], 19, NonZeroU64::new(10).unwrap()).is_empty());
    }

    #[test]
    fn stops_at_finalized_head() {
        let monitor = Arc::new(monitor(1, 10, None, Some(30)));
        assert_eq!(
            plan_blocks(&[monitor], 12, NonZeroU64::new(10).unwrap()),
            vec![
                BlockPlan { block_number: 10, monitor_indices: vec![0] },
                BlockPlan { block_number: 11, monitor_indices: vec![0] },
                BlockPlan { block_number: 12, monitor_indices: vec![0] },
            ]
        );
    }
}
