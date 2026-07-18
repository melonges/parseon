use std::collections::HashMap;
use std::sync::Arc;

use super::abi::{CallDecoder, EventDecoder};
use super::filter::FilterContext;
use super::monitor::Monitor;
use super::{
    Address, B256, BlockNumber, DecodedCall, DecodedEvent, ExecutionOutcome, Selector, SourceBlock,
    SourceLog, Target,
};

#[derive(Debug)]
struct IndexedCall {
    monitor_index: usize,
    decoder: CallDecoder,
}

#[derive(Debug)]
struct IndexedEvent {
    monitor_index: usize,
    decoder: EventDecoder,
}

#[derive(Debug)]
pub(crate) struct MonitorIndex {
    monitors: Vec<Arc<Monitor>>,
    calls: HashMap<(Address, Selector), IndexedCall>,
    events: HashMap<(Address, B256), IndexedEvent>,
}

impl MonitorIndex {
    pub(crate) fn new(monitors: Vec<Monitor>) -> anyhow::Result<Self> {
        let monitors = monitors
            .into_iter()
            .filter(|monitor| monitor.enabled && !monitor.completed)
            .map(Arc::new)
            .collect::<Vec<_>>();
        let mut calls = HashMap::new();
        let mut events = HashMap::new();
        for (index, monitor) in monitors.iter().enumerate() {
            match &monitor.target {
                Target::Call(target) => {
                    anyhow::ensure!(
                        calls
                            .insert(
                                (target.address, target.selector),
                                IndexedCall {
                                    monitor_index: index,
                                    decoder: CallDecoder::new(&target.inputs),
                                },
                            )
                            .is_none(),
                        "duplicate call monitor target"
                    );
                }
                Target::Event(target) => {
                    anyhow::ensure!(
                        events
                            .insert(
                                (target.address, target.topic0),
                                IndexedEvent {
                                    monitor_index: index,
                                    decoder: EventDecoder::new(&target.params, target.topic0)?,
                                },
                            )
                            .is_none(),
                        "duplicate event monitor target"
                    );
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

    pub(crate) fn call(
        &self,
        block_number: BlockNumber,
        address: Address,
        selector: Selector,
    ) -> Option<(usize, &Monitor, &CallDecoder)> {
        self.calls
            .get(&(address, selector))
            .map(|entry| {
                (entry.monitor_index, self.monitors[entry.monitor_index].as_ref(), &entry.decoder)
            })
            .filter(|(_, monitor, _)| monitor.needs_block(block_number))
    }

    pub(crate) fn event(
        &self,
        block_number: BlockNumber,
        address: Address,
        topic0: B256,
    ) -> Option<(usize, &Monitor, &EventDecoder)> {
        self.events
            .get(&(address, topic0))
            .map(|entry| {
                (entry.monitor_index, self.monitors[entry.monitor_index].as_ref(), &entry.decoder)
            })
            .filter(|(_, monitor, _)| monitor.needs_block(block_number))
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
        let Some((monitor_index, monitor, decoder)) = monitors.call(block.number, tx.to, selector)
        else {
            continue;
        };
        if monitor_indices.binary_search(&monitor_index).is_err() {
            continue;
        }
        let calldata = tx.input.get(4..).unwrap_or_default();
        let Target::Call(target) = &monitor.target else {
            continue;
        };
        let params = match decoder.decode(calldata) {
            Ok(params) => params,
            Err(error) => {
                tracing::warn!(
                    monitor = %monitor.id,
                    selector = %target.selector,
                    tx = %tx.hash,
                    "decode error: {error}"
                );
                continue;
            }
        };
        if monitor.filter.evaluate(FilterContext::Call {
            block_number: block.number,
            tx_hash: tx.hash,
            from: tx.from,
            to: tx.to,
            params: &params,
        })? {
            calls.push(DecodedCall {
                monitor_id: monitor.id,
                block_number: block.number,
                transaction_hash: tx.hash,
                from: tx.from,
                to: tx.to,
                params,
            });
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
        let Some((monitor_index, monitor, decoder)) =
            monitors.event(block_number, log.address, topic0)
        else {
            continue;
        };
        if monitor_indices.binary_search(&monitor_index).is_err() {
            continue;
        }
        anyhow::ensure!(!log.removed, "removed log returned for finalized block {block_number}");
        anyhow::ensure!(
            log.block_number == Some(block_number),
            "log has missing or incorrect block number"
        );
        let transaction_hash = log
            .transaction_hash
            .ok_or_else(|| anyhow::anyhow!("log is missing transaction hash"))?;
        let log_index = log.log_index.ok_or_else(|| anyhow::anyhow!("log is missing log index"))?;
        let Target::Event(target) = &monitor.target else { unreachable!() };
        let params = decoder.decode(&log.topics, &log.data).map_err(|error| {
            anyhow::anyhow!(
                "event decode failed for monitor {} (topic0 {}): {error}",
                monitor.id,
                target.topic0
            )
        })?;
        if monitor.filter.evaluate(FilterContext::Event {
            block_number,
            tx_hash: transaction_hash,
            emitter: log.address,
            log_index,
            params: &params,
        })? {
            events.push(DecodedEvent {
                monitor_id: monitor.id,
                block_number,
                transaction_hash,
                log_index,
                params,
            });
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
    use crate::filter::Filter;
    use crate::monitor::Monitor;
    use crate::{BlockTransaction, CallTarget, Cursor, DecodedValue, EventTarget, Target};

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

        assert_eq!(index.call(10, call_address, selector).unwrap().1.id.get(), 1);
        assert_eq!(index.event(12, event_address, topic0).unwrap().1.id.get(), 2);
        assert!(index.call(9, call_address, selector).is_none());
        assert!(index.call(13, call_address, selector).is_none());
        assert!(index.call(10, event_address, selector).is_none());
        assert!(index.event(10, call_address, topic0).is_none());
    }

    #[test]
    fn excludes_cursor_progress_and_duplicate_targets() {
        let address = address!("0000000000000000000000000000000000000001");
        let selector = Selector::from([1, 2, 3, 4]);
        let target = || Target::Call(CallTarget { address, selector, inputs: Vec::new() });
        let index = MonitorIndex::new(vec![monitor(1, target(), Some(10))]).unwrap();
        assert!(index.call(10, address, selector).is_none());
        assert_eq!(index.call(11, address, selector).unwrap().1.id.get(), 1);

        let error = MonitorIndex::new(vec![monitor(1, target(), None), monitor(2, target(), None)])
            .unwrap_err();
        assert!(error.to_string().contains("duplicate call monitor target"));
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
        let block = SourceBlock { number: 1, transactions: vec![transaction.clone()] };
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
