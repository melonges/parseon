//! A tiny ordered concurrency primitive used by the worker.
//!
//! [`ordered`] runs an iterator of futures with a bounded concurrency level
//! while preserving the input order in the output stream. The worker uses it to
//! prepare blocks concurrently but commit them in block-number order.

use std::future::Future;

use futures_util::{Stream, StreamExt, stream};

/// Runs `futures` with at most `concurrency` futures in flight at a time,
/// yielding each future's output in the same order as the input.
///
/// `concurrency` is clamped to a minimum of 1 so a zero value is safe.
pub fn ordered<F>(
    futures: impl IntoIterator<Item = F>,
    concurrency: usize,
) -> impl Stream<Item = F::Output>
where
    F: Future,
{
    stream::iter(futures).buffered(concurrency.max(1))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures_util::StreamExt;

    #[tokio::test]
    async fn pipeline_bounds_work_and_yields_in_input_order() {
        let current = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let futures = (0..8).map(|value| {
            let current = current.clone();
            let maximum = maximum.clone();
            async move {
                let in_flight = current.fetch_add(1, Ordering::SeqCst) + 1;
                maximum.fetch_max(in_flight, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis((8 - value) as u64)).await;
                current.fetch_sub(1, Ordering::SeqCst);
                value
            }
        });

        let values = super::ordered(futures, 3).collect::<Vec<_>>().await;

        assert_eq!(values, (0..8).collect::<Vec<_>>());
        assert_eq!(maximum.load(Ordering::SeqCst), 3);
    }
}
