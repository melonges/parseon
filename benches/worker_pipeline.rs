#[path = "../src/core/pipeline.rs"]
mod pipeline;

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use futures_util::StreamExt;

async fn run_pipeline(concurrency: usize) {
    let preparations = (0..20).map(|_| async {
        tokio::time::sleep(Duration::from_millis(5)).await;
    });
    let mut prepared = pipeline::ordered(preparations, concurrency);
    while prepared.next().await.is_some() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("worker_pipeline");
    group.sample_size(30);
    for concurrency in [1, 4] {
        group.bench_with_input(
            BenchmarkId::from_parameter(concurrency),
            &concurrency,
            |b, concurrency| {
                b.to_async(&runtime).iter(|| run_pipeline(*concurrency));
            },
        );
    }
    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
