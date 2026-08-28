//! Per-poll monitor index and call/event decoding.
//!
//! [`MonitorIndex`] is rebuilt every poll from the monitor list loaded by
//! storage. It hashes call targets by `(address, selector)` and event targets
//! by `(address, topic0)`, then groups overlapping monitors by compatible ABI
//! wire layout. Matching source data is decoded once per layout and fanned out
//! to every covering monitor in that group.
//!
//! [`decode_calls`] and [`decode_events`] consume one block's worth of
//! transactions or logs, filter by the current block plan's monitor indices,
//! decode the matching ABI payloads, and apply each monitor's compiled filter.

use std::sync::Arc;

use rustc_hash::FxHashMap;

use super::abi::{CallDecoder, EventDecoder};
use super::filter::FilterContext;
use super::monitor::Monitor;
use super::{
    Address, B256, BlockNumber, DecodedCall, DecodedEvent, ExecutionOutcome, Selector, SourceBlock,
    SourceLog, Target,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CallLayout(Vec<String>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EventLayout(Vec<(String, bool)>);

#[derive(Debug)]
struct IndexedCallLayout {
    monitor_indices: Vec<usize>,
    decoder: CallDecoder,
}

#[derive(Debug)]
struct IndexedEventLayout {
    monitor_indices: Vec<usize>,
    decoder: EventDecoder,
}

#[derive(Debug)]
pub(crate) struct MonitorIndex {
    monitors: Vec<Arc<Monitor>>,
    calls: FxHashMap<(Address, Selector), FxHashMap<CallLayout, IndexedCallLayout>>,
    events: FxHashMap<(Address, B256), FxHashMap<EventLayout, IndexedEventLayout>>,
}

impl MonitorIndex {
    pub(crate) fn new(monitors: Vec<Monitor>) -> anyhow::Result<Self> {
        let monitors = monitors
            .into_iter()
            .filter(|monitor| monitor.enabled && !monitor.completed)
            .map(Arc::new)
            .collect::<Vec<_>>();
        let mut calls =
            FxHashMap::<(Address, Selector), FxHashMap<CallLayout, IndexedCallLayout>>::default();
        let mut events =
            FxHashMap::<(Address, B256), FxHashMap<EventLayout, IndexedEventLayout>>::default();
        for (index, monitor) in monitors.iter().enumerate() {
            match &monitor.target {
                Target::Call(target) => {
                    let layout =
                        CallLayout(target.inputs.iter().map(|param| param.sol_type()).collect());
                    let layouts = calls
                        .entry((target.address, target.selector))
                        .or_insert_with(FxHashMap::default);
                    if let Some(group) = layouts.get_mut(&layout) {
                        group.monitor_indices.push(index);
                    } else {
                        layouts.insert(
                            layout,
                            IndexedCallLayout {
                                monitor_indices: vec![index],
                                decoder: CallDecoder::new(&target.inputs),
                            },
                        );
                    }
                }
                Target::Event(target) => {
                    let layout = EventLayout(
                        target
                            .params
                            .iter()
                            .map(|param| (param.sol_type(), param.indexed))
                            .collect(),
                    );
                    let layouts = events
                        .entry((target.address, target.topic0))
                        .or_insert_with(FxHashMap::default);
                    if let Some(group) = layouts.get_mut(&layout) {
                        group.monitor_indices.push(index);
                    } else {
                        layouts.insert(
                            layout,
                            IndexedEventLayout {
                                monitor_indices: vec![index],
                                decoder: EventDecoder::new(&target.params, target.topic0)?,
                            },
                        );
                    }
                }
            }
        }
        Ok(Self { monitors, calls, events })
    }

    pub(crate) fn monitors(&self) -> &[Arc<Monitor>] {
        &self.monitors
    }

    pub(crate) fn monitor(&self, index: usize) -> Option<&Arc<Monitor>> {
        self.monitors.get(index)
    }

    fn monitor_is_planned(
        &self,
        monitor_index: usize,
        block_number: BlockNumber,
        planned_indices: &[usize],
    ) -> bool {
        planned_indices.binary_search(&monitor_index).is_ok()
            && self
                .monitors
                .get(monitor_index)
                .is_some_and(|monitor| monitor.needs_block(block_number))
    }

    pub(crate) fn has_call(
        &self,
        block_number: BlockNumber,
        address: Address,
        selector: Selector,
        planned_indices: &[usize],
    ) -> bool {
        self.calls.get(&(address, selector)).is_some_and(|layouts| {
            layouts.values().any(|group| {
                group
                    .monitor_indices
                    .iter()
                    .any(|index| self.monitor_is_planned(*index, block_number, planned_indices))
            })
        })
    }

    #[cfg(test)]
    fn has_event(
        &self,
        block_number: BlockNumber,
        address: Address,
        topic0: B256,
        planned_indices: &[usize],
    ) -> bool {
        self.events.get(&(address, topic0)).is_some_and(|layouts| {
            layouts.values().any(|group| {
                group
                    .monitor_indices
                    .iter()
                    .any(|index| self.monitor_is_planned(*index, block_number, planned_indices))
            })
        })
    }
}

pub(crate) fn decode_calls(
    block: &SourceBlock,
    monitors: &MonitorIndex,
    monitor_indices: &[usize],
    candidate_indices: &[usize],
    outcomes: Vec<ExecutionOutcome>,
) -> anyhow::Result<Vec<DecodedCall>> {
    anyhow::ensure!(
        candidate_indices.len() == outcomes.len(),
        "execution outcome count does not match call candidates"
    );
    let mut calls = Vec::new();
    for (transaction_index, outcome) in candidate_indices.iter().copied().zip(outcomes) {
        let tx = block
            .transactions
            .get(transaction_index)
            .ok_or_else(|| anyhow::anyhow!("call candidate index is outside the source block"))?;
        anyhow::ensure!(
            outcome.transaction_hash == tx.hash,
            "execution outcome order does not match call candidates"
        );
        if !outcome.succeeded {
            continue;
        }
        let Some(selector) = tx.input.get(..4).and_then(|bytes| Selector::try_from(bytes).ok())
        else {
            continue;
        };
        let Some(layouts) = monitors.calls.get(&(tx.to, selector)) else {
            continue;
        };
        let calldata = tx.input.get(4..).unwrap_or_default();
        for (layout, group) in layouts {
            if !group
                .monitor_indices
                .iter()
                .any(|index| monitors.monitor_is_planned(*index, block.number, monitor_indices))
            {
                continue;
            }
            let params = match group.decoder.decode(calldata) {
                Ok(params) => params,
                Err(error) => {
                    let monitor_ids = group
                        .monitor_indices
                        .iter()
                        .copied()
                        .filter(|index| {
                            monitors.monitor_is_planned(*index, block.number, monitor_indices)
                        })
                        .map(|index| monitors.monitors[index].id.get())
                        .collect::<Vec<_>>();
                    tracing::warn!(
                        selector = %selector,
                        tx = %tx.hash,
                        ?layout,
                        ?monitor_ids,
                        "call decode error for monitor ABI layout: {error}"
                    );
                    continue;
                }
            };
            for monitor_index in group.monitor_indices.iter().copied() {
                if !monitors.monitor_is_planned(monitor_index, block.number, monitor_indices) {
                    continue;
                }
                let monitor = monitors.monitors[monitor_index].as_ref();
                if monitor.filter.evaluate(FilterContext::Call {
                    block_number: block.number,
                    tx_hash: tx.hash,
                    from: tx.from,
                    to: tx.to,
                    params: &params,
                })? {
                    calls.push(DecodedCall {
                        monitor_id: monitor.id,
                        block_hash: block.metadata.hash,
                        block_number: block.number,
                        transaction_hash: tx.hash,
                        from: tx.from,
                        to: tx.to,
                        params: params.clone(),
                    });
                }
            }
        }
    }
    Ok(calls)
}

pub(crate) fn decode_events(
    block_number: BlockNumber,
    monitors: &MonitorIndex,
    monitor_indices: &[usize],
    logs: Vec<SourceLog>,
) -> anyhow::Result<Vec<DecodedEvent>> {
    let mut events = Vec::new();
    for log in logs {
        let Some(topic0) = log.topics.first().copied() else {
            continue;
        };
        let Some(layouts) = monitors.events.get(&(log.address, topic0)) else {
            continue;
        };
        if !layouts.values().any(|group| {
            group
                .monitor_indices
                .iter()
                .any(|index| monitors.monitor_is_planned(*index, block_number, monitor_indices))
        }) {
            continue;
        }
        anyhow::ensure!(!log.removed, "removed log returned for finalized block {block_number}");
        anyhow::ensure!(
            log.block_number == Some(block_number),
            "log has missing or incorrect block number"
        );
        let block_hash =
            log.block_hash.ok_or_else(|| anyhow::anyhow!("log is missing block hash"))?;
        let transaction_hash = log
            .transaction_hash
            .ok_or_else(|| anyhow::anyhow!("log is missing transaction hash"))?;
        let log_index = log.log_index.ok_or_else(|| anyhow::anyhow!("log is missing log index"))?;
        for (layout, group) in layouts {
            if !group
                .monitor_indices
                .iter()
                .any(|index| monitors.monitor_is_planned(*index, block_number, monitor_indices))
            {
                continue;
            }
            let params = match group.decoder.decode(&log.topics, &log.data) {
                Ok(params) => params,
                Err(error) => {
                    let monitor_ids = group
                        .monitor_indices
                        .iter()
                        .copied()
                        .filter(|index| {
                            monitors.monitor_is_planned(*index, block_number, monitor_indices)
                        })
                        .map(|index| monitors.monitors[index].id.get())
                        .collect::<Vec<_>>();
                    tracing::warn!(
                        %topic0,
                        tx = %transaction_hash,
                        log_index,
                        ?layout,
                        ?monitor_ids,
                        "event decode error for monitor ABI layout: {error}"
                    );
                    continue;
                }
            };
            for monitor_index in group.monitor_indices.iter().copied() {
                if !monitors.monitor_is_planned(monitor_index, block_number, monitor_indices) {
                    continue;
                }
                let monitor = monitors.monitors[monitor_index].as_ref();
                if monitor.filter.evaluate(FilterContext::Event {
                    block_number,
                    tx_hash: transaction_hash,
                    emitter: log.address,
                    log_index,
                    params: &params,
                })? {
                    events.push(DecodedEvent {
                        monitor_id: monitor.id,
                        block_hash,
                        block_number,
                        transaction_hash,
                        log_index,
                        params: params.clone(),
                    });
                }
            }
        }
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use alloy::dyn_abi::DynSolType;
    use alloy::primitives::{Address, B256, U256, address};
    use alloy::sol_types::SolCall;

    use super::*;
    use crate::abi::AbiParam;
    use crate::filter::{Filter, FilterDefinition, FilterExpression};
    use crate::monitor::Monitor;
    use crate::{
        BlockMetadata, BlockTransaction, CallTarget, Cursor, DecodedValue, EventTarget, Target,
    };

    alloy::sol! {
        function transfer(address to, uint256 value) external returns (bool);
    }

    fn monitor(id: u64, target: Target, cursor: Option<BlockNumber>) -> Monitor {
        Monitor {
            id: crate::MonitorId::new(id).unwrap(),
            chain: crate::Chain::new(1),
            target,
            start_block: 10,
            end_block: Some(12),
            cursor: Cursor(cursor),
            completed: false,
            enabled: true,
            filter: Filter::All,
        }
    }

    fn filtered_monitor(id: u64, target: Target, expression: serde_json::Value) -> Monitor {
        let expression: FilterExpression = serde_json::from_value(expression).unwrap();
        let filter = FilterDefinition::prepare(expression, &target).unwrap().1;
        let mut monitor = monitor(id, target, None);
        monitor.filter = filter;
        monitor
    }

    #[test]
    fn resolves_call_and_event_targets_by_key_and_block() {
        let call_address = address!("0000000000000000000000000000000000000001");
        let event_address = address!("0000000000000000000000000000000000000002");
        let selector = Selector::from([1, 2, 3, 4]);
        let topic0 = B256::repeat_byte(5);
        let index = MonitorIndex::new(vec![
            monitor(
                1,
                Target::Call(CallTarget { address: call_address, selector, inputs: Vec::new() }),
                None,
            ),
            monitor(
                2,
                Target::Event(EventTarget { address: event_address, topic0, params: Vec::new() }),
                None,
            ),
        ])
        .unwrap();

        assert!(index.has_call(10, call_address, selector, &[0]));
        assert!(index.has_event(12, event_address, topic0, &[1]));
        assert!(!index.has_call(9, call_address, selector, &[0]));
        assert!(!index.has_call(13, call_address, selector, &[0]));
        assert!(!index.has_call(10, event_address, selector, &[0]));
        assert!(!index.has_event(10, call_address, topic0, &[1]));
    }

    #[test]
    fn excludes_cursor_progress_and_accepts_duplicate_targets() {
        let address = address!("0000000000000000000000000000000000000001");
        let selector = Selector::from([1, 2, 3, 4]);
        let target = || Target::Call(CallTarget { address, selector, inputs: Vec::new() });
        let index =
            MonitorIndex::new(vec![monitor(1, target(), Some(10)), monitor(2, target(), None)])
                .unwrap();

        assert!(!index.has_call(10, address, selector, &[0]));
        assert!(index.has_call(11, address, selector, &[0]));
        assert!(index.has_call(10, address, selector, &[0, 1]));
        assert_eq!(index.calls[&(address, selector)].len(), 1);
        assert_eq!(
            index.calls[&(address, selector)].values().next().unwrap().monitor_indices,
            [0, 1]
        );
    }

    #[test]
    fn decodes_event_for_indexed_target() {
        let address = address!("0000000000000000000000000000000000000001");
        let topic0 = B256::repeat_byte(5);
        let index = MonitorIndex::new(vec![monitor(
            2,
            Target::Event(EventTarget { address, topic0, params: Vec::new() }),
            None,
        )])
        .unwrap();
        let transaction_hash = B256::repeat_byte(9);
        let decoded = decode_events(
            10,
            &index,
            &[0],
            vec![SourceLog {
                block_number: Some(10),
                block_hash: Some(B256::from([10; 32])),
                transaction_hash: Some(transaction_hash),
                log_index: Some(3),
                address,
                topics: vec![topic0],
                data: Vec::new().into(),
                removed: false,
            }],
        )
        .unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].monitor_id.get(), 2);
        assert_eq!(decoded[0].transaction_hash, transaction_hash);
        assert_eq!(decoded[0].log_index, 3);
        assert!(decoded[0].params.is_empty());
    }

    #[test]
    fn decodes_successful_matching_transactions_only() {
        let contract = address!("0000000000000000000000000000000000000001");
        let call = transferCall { to: Address::ZERO, value: U256::from(42) };
        let transaction = BlockTransaction {
            hash: B256::ZERO,
            from: Address::ZERO,
            to: contract,
            input: call.abi_encode().into(),
        };
        let monitor = Monitor {
            id: crate::MonitorId::new(9).unwrap(),
            chain: crate::Chain::new(1),
            target: Target::Call(CallTarget {
                address: contract,
                selector: transferCall::SELECTOR.into(),
                inputs: vec![
                    AbiParam::new("to", DynSolType::Address).unwrap(),
                    AbiParam::new("value", DynSolType::Uint(256)).unwrap(),
                ],
            }),
            start_block: 1,
            end_block: None,
            cursor: Cursor(None),
            completed: false,
            enabled: true,
            filter: Filter::All,
        };
        let block = SourceBlock {
            number: 1,
            metadata: BlockMetadata {
                number: 1,
                hash: B256::from([1; 32]),
                parent_hash: B256::ZERO,
                timestamp: 0,
            },
            transactions: vec![transaction.clone()],
        };
        let outcomes = vec![
            ExecutionOutcome { transaction_hash: transaction.hash, succeeded: false },
            ExecutionOutcome { transaction_hash: transaction.hash, succeeded: true },
        ];

        let index = MonitorIndex::new(vec![monitor]).unwrap();
        let decoded = decode_calls(&block, &index, &[0], &[0, 0], outcomes).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].monitor_id.get(), 9);
        assert_eq!(decoded[0].params[1], DecodedValue::Uint(U256::from(42)));
    }

    #[test]
    fn fans_out_compatible_calls_and_isolates_incompatible_layouts() {
        let contract = address!("0000000000000000000000000000000000000001");
        let recipient = Address::repeat_byte(7);
        let call = transferCall { to: recipient, value: U256::from(42) };
        let selector = Selector::from(transferCall::SELECTOR);
        let first_target = Target::Call(CallTarget {
            address: contract,
            selector,
            inputs: vec![
                AbiParam::new("to", DynSolType::Address).unwrap(),
                AbiParam::new("value", DynSolType::Uint(256)).unwrap(),
            ],
        });
        let renamed_target = Target::Call(CallTarget {
            address: contract,
            selector,
            inputs: vec![
                AbiParam::new("recipient", DynSolType::Address).unwrap(),
                AbiParam::new("amount", DynSolType::Uint(256)).unwrap(),
            ],
        });
        let incompatible_target = Target::Call(CallTarget {
            address: contract,
            selector,
            inputs: vec![AbiParam::new("text", DynSolType::String).unwrap()],
        });
        let index = MonitorIndex::new(vec![
            monitor(1, first_target.clone(), None),
            filtered_monitor(
                2,
                renamed_target,
                serde_json::json!({"field":"params.amount","op":"eq","value":"42"}),
            ),
            filtered_monitor(
                3,
                first_target,
                serde_json::json!({"field":"params.value","op":"gt","value":"42"}),
            ),
            monitor(4, incompatible_target, None),
        ])
        .unwrap();
        assert_eq!(index.calls[&(contract, selector)].len(), 2);

        let transaction = BlockTransaction {
            hash: B256::repeat_byte(9),
            from: Address::ZERO,
            to: contract,
            input: call.abi_encode().into(),
        };
        let block = SourceBlock {
            number: 10,
            metadata: BlockMetadata {
                number: 10,
                hash: B256::from([10; 32]),
                parent_hash: B256::ZERO,
                timestamp: 0,
            },
            transactions: vec![transaction.clone()],
        };
        let decoded = decode_calls(
            &block,
            &index,
            &[0, 1, 2, 3],
            &[0],
            vec![ExecutionOutcome { transaction_hash: transaction.hash, succeeded: true }],
        )
        .unwrap();

        assert_eq!(decoded.iter().map(|call| call.monitor_id.get()).collect::<Vec<_>>(), [1, 2]);
        assert_eq!(decoded[0].params[0], DecodedValue::Address(recipient));
        assert_eq!(decoded[1].params[1], DecodedValue::Uint(U256::from(42)));
    }

    #[test]
    fn fans_out_compatible_events_and_isolates_indexed_layout_errors() {
        let address = address!("0000000000000000000000000000000000000001");
        let topic0 = B256::repeat_byte(5);
        let non_indexed = |name| EventTarget {
            address,
            topic0,
            params: vec![AbiParam::new(name, DynSolType::Uint(256)).unwrap()],
        };
        let indexed = EventTarget {
            address,
            topic0,
            params: vec![AbiParam::new("value", DynSolType::Uint(256)).unwrap().with_indexed(true)],
        };
        let index = MonitorIndex::new(vec![
            monitor(1, Target::Event(non_indexed("value")), None),
            monitor(2, Target::Event(non_indexed("amount")), None),
            monitor(3, Target::Event(indexed), None),
        ])
        .unwrap();
        assert_eq!(index.events[&(address, topic0)].len(), 2);

        let decoded = decode_events(
            10,
            &index,
            &[0, 1, 2],
            vec![SourceLog {
                block_number: Some(10),
                block_hash: Some(B256::from([10; 32])),
                transaction_hash: Some(B256::repeat_byte(9)),
                log_index: Some(3),
                address,
                topics: vec![topic0],
                data: U256::from(42).to_be_bytes::<32>().to_vec().into(),
                removed: false,
            }],
        )
        .unwrap();

        assert_eq!(decoded.iter().map(|event| event.monitor_id.get()).collect::<Vec<_>>(), [1, 2]);
        assert_eq!(decoded[0].params, [DecodedValue::Uint(U256::from(42))]);
        assert_eq!(decoded[1].params, [DecodedValue::Uint(U256::from(42))]);
    }

    #[test]
    fn restricts_call_decoding_to_the_current_block_plan() {
        let first_address = Address::repeat_byte(1);
        let second_address = Address::repeat_byte(2);
        let first_selector = Selector::from([1, 2, 3, 4]);
        let second_selector = Selector::from([5, 6, 7, 8]);
        let index = MonitorIndex::new(vec![
            monitor(
                1,
                Target::Call(CallTarget {
                    address: first_address,
                    selector: first_selector,
                    inputs: Vec::new(),
                }),
                None,
            ),
            monitor(
                2,
                Target::Call(CallTarget {
                    address: second_address,
                    selector: second_selector,
                    inputs: Vec::new(),
                }),
                None,
            ),
        ])
        .unwrap();
        let block = SourceBlock {
            number: 10,
            metadata: BlockMetadata {
                number: 10,
                hash: B256::from([10; 32]),
                parent_hash: B256::ZERO,
                timestamp: 0,
            },
            transactions: vec![
                BlockTransaction {
                    hash: B256::repeat_byte(1),
                    from: Address::ZERO,
                    to: first_address,
                    input: first_selector.to_vec().into(),
                },
                BlockTransaction {
                    hash: B256::repeat_byte(2),
                    from: Address::ZERO,
                    to: second_address,
                    input: second_selector.to_vec().into(),
                },
            ],
        };
        let outcomes = block
            .transactions
            .iter()
            .map(|transaction| ExecutionOutcome {
                transaction_hash: transaction.hash,
                succeeded: true,
            })
            .collect();

        let decoded = decode_calls(&block, &index, &[1], &[0, 1], outcomes).unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].monitor_id.get(), 2);
    }

    #[test]
    fn restricts_event_decoding_to_the_current_block_plan() {
        let first_address = Address::repeat_byte(1);
        let second_address = Address::repeat_byte(2);
        let first_topic = B256::repeat_byte(3);
        let second_topic = B256::repeat_byte(4);
        let index = MonitorIndex::new(vec![
            monitor(
                1,
                Target::Event(EventTarget {
                    address: first_address,
                    topic0: first_topic,
                    params: Vec::new(),
                }),
                None,
            ),
            monitor(
                2,
                Target::Event(EventTarget {
                    address: second_address,
                    topic0: second_topic,
                    params: Vec::new(),
                }),
                None,
            ),
        ])
        .unwrap();
        let log = |index, address, topic0| SourceLog {
            block_number: Some(10),
            block_hash: Some(B256::from([10; 32])),
            transaction_hash: Some(B256::repeat_byte(index)),
            log_index: Some(u64::from(index)),
            address,
            topics: vec![topic0],
            data: Vec::new().into(),
            removed: false,
        };

        let decoded = decode_events(
            10,
            &index,
            &[1],
            vec![log(1, first_address, first_topic), log(2, second_address, second_topic)],
        )
        .unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].monitor_id.get(), 2);
    }
}
