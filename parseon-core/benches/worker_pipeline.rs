use std::time::Duration;

use alloy::dyn_abi::DynSolType;
use alloy::primitives::{Address, B256, U256};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures_util::StreamExt;
use parseon_core::abi::AbiParam;
use parseon_core::filter::{Filter, FilterContext, FilterDefinition, FilterExpression};
use parseon_core::pipeline;
use parseon_core::{CallTarget, DecodedValue, EventTarget, Target};

async fn run_pipeline(concurrency: usize) {
    let preparations = (0..20).map(|_| async {
        tokio::time::sleep(Duration::from_millis(5)).await;
    });
    let mut prepared = pipeline::ordered(preparations, concurrency);
    while prepared.next().await.is_some() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn run_filters(filter: Filter, event: bool, concurrency: usize) -> usize {
    let blocks = (0..64).map(|block_number| {
        let filter = filter.clone();
        async move {
            tokio::task::yield_now().await;
            (0..256)
                .filter(|index| {
                    let params = [DecodedValue::Uint(U256::from(*index))];
                    let context = if event {
                        FilterContext::Event {
                            block_number,
                            tx_hash: B256::repeat_byte(*index as u8),
                            emitter: Address::ZERO,
                            log_index: *index,
                            params: &params,
                        }
                    } else {
                        FilterContext::Call {
                            block_number,
                            tx_hash: B256::repeat_byte(*index as u8),
                            from: Address::ZERO,
                            to: Address::ZERO,
                            params: &params,
                        }
                    };
                    filter.evaluate(context).expect("compiled filter evaluation failed")
                })
                .count()
        }
    });
    pipeline::ordered(blocks, concurrency)
        .fold(0, |total, count| async move { total + count })
        .await
}

fn expression(value: serde_json::Value) -> FilterExpression {
    serde_json::from_value(value).expect("invalid benchmark filter expression")
}

fn compiled(target: &Target, value: serde_json::Value) -> (FilterExpression, Filter) {
    let expression = expression(value);
    let filter =
        FilterDefinition::prepare(expression.clone(), target).expect("invalid benchmark filter").1;
    (expression, filter)
}

fn benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("failed to create benchmark runtime");
    let mut pipeline_group = c.benchmark_group("worker_pipeline");
    pipeline_group.sample_size(30);
    for concurrency in [1, 4] {
        pipeline_group.bench_with_input(
            BenchmarkId::from_parameter(concurrency),
            &concurrency,
            |b, concurrency| {
                b.to_async(&runtime).iter(|| run_pipeline(*concurrency));
            },
        );
    }
    pipeline_group.finish();

    let param =
        AbiParam::new("value", DynSolType::Uint(256)).expect("valid benchmark ABI parameter");
    let call = Target::Call(CallTarget {
        address: Address::ZERO,
        selector: [0; 4].into(),
        inputs: vec![param.clone()],
    });
    let event = Target::Event(EventTarget {
        address: Address::ZERO,
        topic0: B256::ZERO,
        params: vec![param],
    });
    let (call_leaf_source, call_leaf) =
        compiled(&call, serde_json::json!({"field":"params.value","op":"gte","value":"128"}));
    let (_, call_compound) = compiled(
        &call,
        serde_json::json!({"and":[
            {"field":"tx.from","op":"eq","value":format!("{:#x}", Address::ZERO)},
            {"field":"params.value","op":"gte","value":"128"},
            {"not":{"field":"tx.hash","op":"eq","value":format!("0x{}", "ff".repeat(32))}}
        ]}),
    );
    let (event_leaf_source, event_leaf) =
        compiled(&event, serde_json::json!({"field":"event.log_index","op":"gte","value":"128"}));
    let (_, event_compound) = compiled(
        &event,
        serde_json::json!({"or":[
            {"field":"params.value","op":"lt","value":"64"},
            {"field":"params.value","op":"gte","value":"192"},
            {"not":{"field":"event.emitter","op":"eq","value":format!("{:#x}", Address::ZERO)}}
        ]}),
    );

    let cases = [
        ("call/all", Filter::All, false),
        ("call/leaf", call_leaf, false),
        ("call/compound", call_compound, false),
        ("event/all", Filter::All, true),
        ("event/leaf", event_leaf, true),
        ("event/compound", event_compound, true),
    ];
    let mut filter_group = c.benchmark_group("filter_pipeline");
    filter_group.throughput(Throughput::Elements(64 * 256));
    for concurrency in [1, 4] {
        for (name, filter, event) in &cases {
            filter_group.bench_with_input(
                BenchmarkId::new(*name, concurrency),
                &concurrency,
                |b, concurrency| {
                    b.to_async(&runtime).iter(|| run_filters(filter.clone(), *event, *concurrency));
                },
            );
        }
    }
    filter_group.finish();

    let mut compile_group = c.benchmark_group("filter_compile");
    compile_group.bench_function("call_leaf", |b| {
        b.iter(|| {
            FilterDefinition::prepare(call_leaf_source.clone(), &call)
                .expect("valid benchmark filter")
        })
    });
    compile_group.bench_function("event_leaf", |b| {
        b.iter(|| {
            FilterDefinition::prepare(event_leaf_source.clone(), &event)
                .expect("valid benchmark filter")
        })
    });
    compile_group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
