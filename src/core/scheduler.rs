use super::monitor::Monitor;

pub fn plan_blocks(monitors: &[&Monitor], finalized_head: i64, batch_size: i64) -> Vec<i64> {
    let mut wanted = Vec::new();
    let batch_size = batch_size.max(1);
    for monitor in monitors {
        let from = monitor.next_block();
        let to = monitor
            .end_block
            .unwrap_or(finalized_head)
            .min(finalized_head);
        if from > to {
            continue;
        }
        let to = to.min(from.saturating_add(batch_size - 1));
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
    use crate::core::filter::Filter;
    use crate::core::monitor::Monitor;
    use crate::core::{CallTarget, Cursor, Target};

    fn monitor(id: i64, start: i64, cursor: Option<i64>, end: Option<i64>) -> Monitor {
        Monitor {
            id,
            target: Target::Call(CallTarget {
                address: Address::ZERO,
                selector: [0; 4],
                signature: "f(uint256)".into(),
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
        assert_eq!(plan_blocks(&[&first, &second], 20, 3), vec![10, 11, 12]);
    }

    #[test]
    fn skips_ranges_beyond_head() {
        let monitor = monitor(1, 20, None, None);
        assert!(plan_blocks(&[&monitor], 19, 10).is_empty());
    }
}
