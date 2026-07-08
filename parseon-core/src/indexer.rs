use super::abi::{decode_calldata, decode_event};
use super::monitor::Monitor;
use super::{BlockNumber, DecodedCall, DecodedEvent, ExecutedTransaction, Selector, SourceBlock, SourceLog, Target};

pub fn decode_calls(
    block: &SourceBlock,
    monitors: &[Monitor],
    transactions: Vec<ExecutedTransaction>,
) -> Vec<DecodedCall> {
    let mut calls = Vec::new();
    for tx in transactions.into_iter().filter_map(|tx| tx.succeeded.then(|| tx.transaction)) {
        let Some(selector) = tx
            .input
            .get(..4)
            .and_then(|bytes| Selector::try_from(bytes).ok())
        else {
            continue;
        };
        let Some(monitor) = monitors
            .iter()
            .find(|monitor| monitor.matches_call(tx.to, selector))
        else {
            continue;
        };
        let calldata = tx.input.get(4..).unwrap_or_default();
        let Target::Call(target) = &monitor.target else {
            continue;
        };
        let params = match decode_calldata(&target.inputs, calldata) {
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
        let call = DecodedCall {
            monitor_id: monitor.id,
            block_number: block.number,
            transaction: ExecutedTransaction { transaction: tx, succeeded: true },
            params,
        };
        if monitor.filter.matches(&call) {
            calls.push(call);
        }
    }
    calls
}

pub fn decode_events(
    block_number: BlockNumber,
    monitors: &[Monitor],
    logs: Vec<SourceLog>,
) -> anyhow::Result<Vec<DecodedEvent>> {
    let mut events = Vec::new();
    for log in logs {
        let Some(topic0) = log.topics.first().copied() else {
            continue;
        };
        let matching = monitors
            .iter()
            .filter(|m| m.matches_event(log.address, topic0))
            .collect::<Vec<_>>();
        if matching.is_empty() {
            continue;
        }
        anyhow::ensure!(
            !log.removed,
            "removed log returned for finalized block {block_number}"
        );
        anyhow::ensure!(
            log.block_number == Some(block_number),
            "log has missing or incorrect block number"
        );
        let transaction_hash = log
            .transaction_hash
            .ok_or_else(|| anyhow::anyhow!("log is missing transaction hash"))?;
        let log_index = log
            .log_index
            .ok_or_else(|| anyhow::anyhow!("log is missing log index"))?;
        for monitor in matching {
            let Target::Event(target) = &monitor.target else {
                unreachable!()
            };
            let params = decode_event(&target.params, target.topic0, &log.topics, &log.data)
                .map_err(|error| {
                    anyhow::anyhow!(
                        "event decode failed for monitor {} (topic0 {}): {error}",
                        monitor.id,
                        target.topic0
                    )
                })?;
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
    use crate::{BlockTransaction, CallTarget, Cursor, DecodedValue, Target};

    alloy::sol! {
        function transfer(address to, uint256 value) external returns (bool);
    }

    #[test]
    fn decodes_successful_matching_transactions_only() {
        let contract = address!("0000000000000000000000000000000000000001");
        let call = transferCall {
            to: Address::ZERO,
            value: U256::from(42),
        };
        let transaction = BlockTransaction {
            hash: B256::ZERO,
            to: contract,
            input: call.abi_encode(),
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
            transactions: vec![transaction.clone()],
        };
        let executed = vec![
            ExecutedTransaction {
                transaction: transaction.clone(),
                succeeded: false,
            },
            ExecutedTransaction {
                transaction,
                succeeded: true,
            },
        ];

        let decoded = decode_calls(&block, &[monitor], executed);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].monitor_id.get(), 9);
        assert_eq!(decoded[0].params[1], DecodedValue::Uint(U256::from(42)));
    }
}
