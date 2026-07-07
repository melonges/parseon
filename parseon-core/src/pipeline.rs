use std::future::Future;

use futures_util::{Stream, StreamExt, stream};

pub fn ordered<F>(
    futures: impl IntoIterator<Item = F>,
    concurrency: usize,
) -> impl Stream<Item = F::Output>
where
    F: Future,
{
    stream::iter(futures).buffered(concurrency.max(1))
}
