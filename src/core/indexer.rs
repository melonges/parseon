use super::abi::decode_calldata;
use super::monitor::Monitor;
use super::{DecodedCall, ExecutedTransaction, SourceBlock};

pub fn decode_calls(
    block: &SourceBlock,
    monitors: &[Monitor],
    transactions: Vec<ExecutedTransaction>,
) -> Vec<DecodedCall> {
    let mut calls = Vec::new();
    for transaction in transactions {
        if !transaction.succeeded {
            continue;
        }
        let selector = transaction.transaction.input.get(..4).unwrap_or_default();
        let Some(monitor) = monitors
            .iter()
            .find(|monitor| monitor.matches(transaction.transaction.to, selector))
        else {
            continue;
        };
        let calldata = transaction.transaction.input.get(4..).unwrap_or_default();
        let params = match decode_calldata(&monitor.target.inputs, calldata) {
            Ok(params) => params,
            Err(error) => {
                tracing::warn!(
                    monitor = monitor.id,
                    signature = %monitor.target.signature,
                    tx = %transaction.transaction.hash,
                    "decode error: {error}"
                );
                continue;
            }
        };
        let call = DecodedCall {
            monitor_id: monitor.id,
            block_number: block.number,
            block_hash: block.hash,
            transaction,
            params,
        };
        if monitor.filter.matches(&call) {
            calls.push(call);
        }
    }
    calls
}

#[cfg(test)]
mod tests {
    use alloy::dyn_abi::DynSolType;
    use alloy::primitives::{Address, B256, U256, address};
    use alloy::sol_types::SolCall;

    use super::*;
    use crate::core::abi::AbiParam;
    use crate::core::filter::Filter;
    use crate::core::monitor::Monitor;
    use crate::core::{BlockTransaction, Cursor, DecodedValue, Target};

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
            from: Address::ZERO,
            to: contract,
            input: call.abi_encode(),
            value: U256::ZERO,
        };
        let monitor = Monitor {
            id: 9,
            target: Target {
                address: contract,
                selector: transferCall::SELECTOR,
                signature: "transfer(address,uint256)".into(),
                inputs: vec![
                    AbiParam::new("to", DynSolType::Address).unwrap(),
                    AbiParam::new("value", DynSolType::Uint(256)).unwrap(),
                ],
            },
            start_block: 1,
            end_block: None,
            cursor: Cursor(None),
            completed: false,
            enabled: true,
            filter: Filter::All,
        };
        let block = SourceBlock {
            number: 1,
            hash: B256::ZERO,
            transactions: vec![transaction.clone()],
        };
        let executed = vec![
            ExecutedTransaction {
                transaction: transaction.clone(),
                gas_used: 1,
                gas_price: 1,
                succeeded: false,
            },
            ExecutedTransaction {
                transaction,
                gas_used: 1,
                gas_price: 1,
                succeeded: true,
            },
        ];

        let decoded = decode_calls(&block, &[monitor], executed);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].monitor_id, 9);
        assert_eq!(decoded[0].params[1], DecodedValue::Uint(U256::from(42)));
    }
}
