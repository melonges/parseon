use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use alloy::eips::BlockNumberOrTag;
use alloy::network::AnyNetwork;
use alloy::network::BlockResponse;
use alloy::providers::{Provider, RootProvider};
use alloy::rpc::client::RpcClient;
use alloy::rpc::types::Filter;
use alloy::transports::http::reqwest::Client;
use anyhow::Context;
use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::fetch;
use crate::transport::RotatingHttp;
use parseon_core::ports::{
    BlockRange, BlockSource, BlockSourceFactory, BlockSourceRequestError, InFlightGuard, LogQuery,
    LogTarget, NoopTelemetry, Telemetry,
};
use parseon_core::{BlockNumber, ChainId, ExecutionOutcome, SourceBlock, SourceLog, TxHash, Url};

pub(crate) type HttpProvider = RootProvider<AnyNetwork>;

const CAPABILITY_UNKNOWN: u8 = 0;
const CAPABILITY_SUPPORTED: u8 = 1;
const CAPABILITY_UNSUPPORTED: u8 = 2;

#[derive(Debug, Clone, Copy)]
pub struct RpcConfig {
    pub request_concurrency: NonZeroUsize,
    pub batch_size: NonZeroUsize,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            request_concurrency: NonZeroUsize::new(16).expect("16 is non-zero"),
            batch_size: NonZeroUsize::new(20).expect("20 is non-zero"),
        }
    }
}

pub struct JsonRpcBlockSource {
    provider: HttpProvider,
    /// Shared transport behind `provider`, kept for in-place RPC URL
    /// rotation. `None` only in tests with a mocked transport.
    transport: Option<RotatingHttp>,
    request_concurrency: usize,
    batch_size: usize,
    permits: Semaphore,
    chain_id: OnceLock<ChainId>,
    batch_capability: AtomicU8,
    block_receipts_capability: AtomicU8,
    telemetry: Arc<dyn Telemetry>,
}

#[derive(Clone)]
pub struct JsonRpcBlockSourceFactory {
    config: RpcConfig,
    telemetry: Arc<dyn Telemetry>,
}

impl Default for JsonRpcBlockSourceFactory {
    fn default() -> Self {
        Self::new(RpcConfig::default(), Arc::new(NoopTelemetry))
    }
}

impl JsonRpcBlockSourceFactory {
    pub fn new(config: RpcConfig, telemetry: Arc<dyn Telemetry>) -> Self {
        Self { config, telemetry }
    }
}

impl BlockSourceFactory for JsonRpcBlockSourceFactory {
    fn connect(&self, rpc_url: &Url) -> anyhow::Result<Arc<dyn BlockSource>> {
        Ok(Arc::new(JsonRpcBlockSource::connect(rpc_url, self.config, self.telemetry.clone())?))
    }
}

impl JsonRpcBlockSource {
    pub fn connect(
        rpc_url: &Url,
        config: RpcConfig,
        telemetry: Arc<dyn Telemetry>,
    ) -> anyhow::Result<Self> {
        let (provider, transport) = build(rpc_url)?;
        Ok(Self::with_transport(provider, Some(transport), config, telemetry))
    }

    /// Builds a source over a provider with a mocked transport (no live
    /// transport to rotate).
    #[cfg(test)]
    fn with_provider(
        provider: HttpProvider,
        config: RpcConfig,
        telemetry: Arc<dyn Telemetry>,
    ) -> Self {
        Self::with_transport(provider, None, config, telemetry)
    }

    fn with_transport(
        provider: HttpProvider,
        transport: Option<RotatingHttp>,
        config: RpcConfig,
        telemetry: Arc<dyn Telemetry>,
    ) -> Self {
        let request_concurrency = config.request_concurrency.get();
        Self {
            provider,
            transport,
            request_concurrency,
            batch_size: config.batch_size.get(),
            permits: Semaphore::new(request_concurrency),
            chain_id: OnceLock::new(),
            batch_capability: AtomicU8::new(CAPABILITY_UNKNOWN),
            block_receipts_capability: AtomicU8::new(CAPABILITY_UNKNOWN),
            telemetry,
        }
    }

    async fn acquire(&self) -> anyhow::Result<SemaphorePermit<'_>> {
        Ok(self.permits.acquire().await?)
    }

    async fn observed<T, F>(
        &self,
        operation: &'static str,
        strategy: &'static str,
        future: F,
    ) -> anyhow::Result<T>
    where
        F: Future<Output = anyhow::Result<T>>,
    {
        let _permit = self.acquire().await?;
        let chain_id = self.chain_id.get().copied();
        let _in_flight =
            chain_id.map(|chain_id| InFlightGuard::new(self.telemetry.as_ref(), chain_id, "rpc"));
        let started = Instant::now();
        let result = future.await;
        if let Some(chain_id) = chain_id {
            self.telemetry.record_rpc(
                chain_id,
                operation,
                strategy,
                if result.is_ok() { "success" } else { "error" },
                started.elapsed(),
            );
        }
        result
    }

    async fn single_receipts(
        &self,
        transaction_hashes: &[TxHash],
    ) -> anyhow::Result<Vec<ExecutionOutcome>> {
        let mut receipts =
            stream::iter(transaction_hashes.iter().copied().map(|transaction_hash| async move {
                self.observed("receipts", "single", async {
                    fetch::fetch_receipt(&self.provider, transaction_hash).await
                })
                .await
            }))
            .buffered(self.request_concurrency);
        let mut out = Vec::with_capacity(transaction_hashes.len());
        while let Some(receipt) = receipts.next().await {
            out.push(receipt?);
        }
        Ok(out)
    }

    async fn batched_receipts(
        &self,
        transaction_hashes: &[TxHash],
    ) -> anyhow::Result<Vec<ExecutionOutcome>> {
        let ranges = (0..transaction_hashes.len())
            .step_by(self.batch_size)
            .map(|start| start..(start + self.batch_size).min(transaction_hashes.len()))
            .collect::<Vec<_>>();
        let mut batches = stream::iter(ranges.into_iter().map(|range| async move {
            let chunk = &transaction_hashes[range];
            self.observed("receipts", "batch", async {
                fetch::fetch_receipt_batch(&self.provider, chunk).await
            })
            .await
        }))
        .buffered(self.request_concurrency);
        let mut out = Vec::with_capacity(transaction_hashes.len());
        while let Some(batch) = batches.next().await {
            out.extend(batch?);
        }
        Ok(out)
    }

    async fn optimized_receipts(
        &self,
        block_number: BlockNumber,
        transaction_hashes: &[TxHash],
    ) -> anyhow::Result<Vec<ExecutionOutcome>> {
        if transaction_hashes.is_empty() {
            return Ok(Vec::new());
        }
        if transaction_hashes.len() == 1 {
            return self.single_receipts(transaction_hashes).await;
        }

        if transaction_hashes.len() > self.batch_size
            && self.block_receipts_capability.load(Ordering::Relaxed) != CAPABILITY_UNSUPPORTED
        {
            let block_receipts = self
                .observed("receipts", "block_receipts", async {
                    fetch::fetch_block_receipts(&self.provider, block_number, transaction_hashes)
                        .await
                })
                .await;
            match block_receipts {
                Ok(receipts) => {
                    self.block_receipts_capability.store(CAPABILITY_SUPPORTED, Ordering::Relaxed);
                    return Ok(receipts);
                }
                Err(error) if block_receipts_unavailable(&error) => {
                    self.block_receipts_capability.store(CAPABILITY_UNSUPPORTED, Ordering::Relaxed);
                    tracing::debug!(
                        chain_id = ?self.chain_id.get(),
                        "block receipt optimization unavailable; falling back"
                    );
                }
                Err(error) => return Err(error),
            }
        }

        if self.batch_capability.load(Ordering::Relaxed) != CAPABILITY_UNSUPPORTED {
            match self.batched_receipts(transaction_hashes).await {
                Ok(receipts) => {
                    self.batch_capability.store(CAPABILITY_SUPPORTED, Ordering::Relaxed);
                    return Ok(receipts);
                }
                Err(error) if unsupported_batch(&error) => {
                    self.batch_capability.store(CAPABILITY_UNSUPPORTED, Ordering::Relaxed);
                    tracing::debug!(
                        chain_id = ?self.chain_id.get(),
                        "RPC batching unavailable; falling back"
                    );
                }
                Err(error) => return Err(error),
            }
        }
        self.single_receipts(transaction_hashes).await
    }

    async fn adaptive_logs(&self, query: LogQuery) -> anyhow::Result<Vec<SourceLog>> {
        if query.is_empty() {
            return Ok(Vec::new());
        }
        let (range, targets) = query.into_parts();
        let mut queries = stream::iter(
            exact_log_filters(&targets)
                .into_iter()
                .map(|filter| self.adaptive_filter_logs(range, filter)),
        )
        .buffer_unordered(self.request_concurrency);
        let mut logs = Vec::new();
        while let Some(fetched) = queries.next().await {
            logs.extend(fetched?);
        }
        logs.sort_unstable_by_key(|log| (log.block_number, log.log_index));
        Ok(logs)
    }

    async fn adaptive_filter_logs(
        &self,
        requested: BlockRange,
        mut filter: Filter,
    ) -> anyhow::Result<Vec<SourceLog>> {
        let mut pending = vec![requested];
        let mut logs = Vec::new();
        while let Some(range) = pending.pop() {
            let strategy = if range.start() == range.end() { "single" } else { "range" };
            filter.block_option = (range.start()..=range.end()).into();
            match self
                .observed("logs", strategy, async {
                    fetch::fetch_logs(&self.provider, &filter).await
                })
                .await
            {
                Ok(mut fetched) => logs.append(&mut fetched),
                Err(error) if log_range_limited(&error) && range.start() < range.end() => {
                    let middle = range.start() + (range.end() - range.start()) / 2;
                    let left = parseon_core::ports::BlockRange::new(range.start(), middle)
                        .expect("split log range is ordered");
                    let right = parseon_core::ports::BlockRange::new(middle + 1, range.end())
                        .expect("split log range is ordered");
                    pending.push(right);
                    pending.push(left);
                }
                Err(error) if log_range_limited(&error) => {
                    return Err(error.context(format!(
                        "log query limit exceeded for single block {}",
                        range.start()
                    )));
                }
                Err(error) => return Err(error),
            }
        }
        Ok(logs)
    }
}

fn exact_log_filters(targets: &[LogTarget]) -> Vec<Filter> {
    let mut by_address = BTreeMap::<_, Vec<_>>::new();
    let mut by_topic = BTreeMap::<_, Vec<_>>::new();
    for target in targets {
        by_address.entry(target.address()).or_default().push(target.topic0());
        by_topic.entry(target.topic0()).or_default().push(target.address());
    }
    if by_address.len() <= by_topic.len() {
        by_address
            .into_iter()
            .map(|(address, topic0s)| Filter::new().address(address).event_signature(topic0s))
            .collect()
    } else {
        by_topic
            .into_iter()
            .map(|(topic0, addresses)| Filter::new().address(addresses).event_signature(topic0))
            .collect()
    }
}

#[async_trait]
impl BlockSource for JsonRpcBlockSource {
    async fn chain_id(&self) -> anyhow::Result<u64> {
        let _permit = self.acquire().await?;
        let started = Instant::now();
        let result = chain_id(&self.provider).await;
        match result {
            Ok(chain_id) => {
                let _cached_chain_id = self.chain_id.get_or_init(|| chain_id);
                self.telemetry.record_rpc(
                    chain_id,
                    "chain_id",
                    "single",
                    "success",
                    started.elapsed(),
                );
                Ok(chain_id)
            }
            Err(error) => Err(source_request_error(error)),
        }
    }

    async fn finalized_head(&self) -> anyhow::Result<BlockNumber> {
        self.observed("finalized_head", "single", async { finalized_number(&self.provider).await })
            .await
            .map_err(source_request_error)
    }

    async fn fetch_block(&self, block_number: BlockNumber) -> anyhow::Result<SourceBlock> {
        self.observed("block", "single", async {
            fetch::fetch_block(&self.provider, block_number).await
        })
        .await
        .map_err(source_request_error)
    }

    async fn fetch_execution_outcomes(
        &self,
        block_number: BlockNumber,
        transaction_hashes: &[TxHash],
    ) -> anyhow::Result<Vec<ExecutionOutcome>> {
        self.optimized_receipts(block_number, transaction_hashes)
            .await
            .map_err(source_request_error)
    }

    async fn fetch_logs(&self, query: LogQuery) -> anyhow::Result<Vec<SourceLog>> {
        self.adaptive_logs(query).await.map_err(source_request_error)
    }

    fn set_rpc_url(&self, rpc_url: &Url) -> anyhow::Result<()> {
        let transport =
            self.transport.as_ref().context("RPC URL rotation requires a live HTTP transport")?;
        transport.set_url(rpc_url.clone());
        // Endpoint capabilities (batching, block receipts) may differ on the
        // new URL; re-probe them. The cached chain ID stays valid because
        // callers guarantee the new URL serves the same chain.
        self.batch_capability.store(CAPABILITY_UNKNOWN, Ordering::Relaxed);
        self.block_receipts_capability.store(CAPABILITY_UNKNOWN, Ordering::Relaxed);
        Ok(())
    }
}

fn source_request_error(error: anyhow::Error) -> anyhow::Error {
    if error
        .chain()
        .any(|cause| cause.downcast_ref::<alloy::transports::TransportError>().is_some())
    {
        anyhow::Error::new(BlockSourceRequestError::new(error))
    } else {
        error
    }
}

fn log_range_limited(error: &anyhow::Error) -> bool {
    let Some((_, message, retryable)) = rpc_error_response(error) else { return false };
    let message = message.to_ascii_lowercase();
    if retryable
        && ["rate limit", "rate exceeded", "too many requests", "request limit", "credits"]
            .iter()
            .any(|needle| message.contains(needle))
    {
        return false;
    }
    (message.contains("block")
        && message.contains("range")
        && ["limit", "limited", "exceed", "too wide", "too large"]
            .iter()
            .any(|needle| message.contains(needle)))
        || (message.contains("query returned more than") && message.contains("result"))
        || message.contains("log response size exceeded")
        || (message.contains("too many")
            && (message.contains("logs") || message.contains("results")))
}

fn rpc_error_response(error: &anyhow::Error) -> Option<(i64, &str, bool)> {
    let response = error.downcast_ref::<alloy::transports::TransportError>()?.as_error_resp()?;
    Some((response.code, response.message.as_ref(), response.is_retry_err()))
}

fn block_receipts_unavailable(error: &anyhow::Error) -> bool {
    rpc_error_response(error).is_some_and(|(code, _, _)| matches!(code, -32601 | -32602))
        || error.downcast_ref::<fetch::BlockReceiptsResponseError>().is_some()
}

fn unsupported_batch(error: &anyhow::Error) -> bool {
    rpc_error_response(error).is_some_and(|(code, message, _)| {
        if code == -32600 {
            return true;
        }
        let message = message.to_ascii_lowercase();
        message.contains("batch")
            && ["not supported", "unsupported", "disabled", "not allowed"]
                .iter()
                .any(|needle| message.contains(needle))
    })
}

pub(crate) fn build(rpc_url: &Url) -> anyhow::Result<(HttpProvider, RotatingHttp)> {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .context("build HTTP client")?;
    let transport = RotatingHttp::new(client, rpc_url.clone());
    let rpc_client = RpcClient::new(transport.clone(), transport.guess_local());
    Ok((RootProvider::<AnyNetwork>::new(rpc_client), transport))
}

pub(crate) async fn chain_id(provider: &HttpProvider) -> anyhow::Result<u64> {
    Ok(provider.get_chain_id().await?)
}

pub(crate) async fn finalized_number(provider: &HttpProvider) -> anyhow::Result<u64> {
    let block = provider
        .get_block_by_number(BlockNumberOrTag::Finalized)
        .await?
        .context("finalized block not found")?;
    Ok(block.header().number)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use alloy::network::AnyNetwork;
    use alloy::primitives::{Address, B256};
    use alloy::providers::RootProvider;
    use alloy::rpc::client::RpcClient;
    use alloy::transports::mock::Asserter;
    use alloy_json_rpc::ErrorPayload;
    use alloy_rpc_types_any::AnyTransactionReceipt;
    use parseon_core::ports::{BlockRange, BlockSource, LogQuery, LogTarget, NoopTelemetry};

    use super::{
        CAPABILITY_SUPPORTED, CAPABILITY_UNKNOWN, CAPABILITY_UNSUPPORTED, JsonRpcBlockSource,
        RpcConfig, exact_log_filters,
    };
    use crate::fetch;

    fn source(asserter: Asserter) -> JsonRpcBlockSource {
        source_with_batch_size(asserter, 20)
    }

    fn source_with_batch_size(asserter: Asserter, batch_size: usize) -> JsonRpcBlockSource {
        JsonRpcBlockSource::with_provider(
            RootProvider::<AnyNetwork>::new(RpcClient::mocked(asserter)),
            RpcConfig {
                request_concurrency: NonZeroUsize::new(4).expect("non-zero"),
                batch_size: NonZeroUsize::new(batch_size).expect("non-zero"),
            },
            Arc::new(NoopTelemetry),
        )
    }

    fn query(start: u64, end: u64) -> LogQuery {
        LogQuery::new(
            BlockRange::new(start, end).expect("ordered range"),
            vec![LogTarget::new(Address::ZERO, B256::ZERO)],
        )
    }

    fn rpc_error(code: i64, message: &'static str) -> ErrorPayload {
        ErrorPayload { code, message: Cow::Borrowed(message), data: None }
    }

    fn receipt(transaction_hash: B256, succeeded: bool) -> AnyTransactionReceipt {
        serde_json::from_value(serde_json::json!({
            "status": if succeeded { "0x1" } else { "0x0" },
            "cumulativeGasUsed": "0x1",
            "logs": [],
            "logsBloom": format!("0x{}", "00".repeat(256)),
            "type": "0x0",
            "transactionHash": format!("{transaction_hash:#x}"),
            "transactionIndex": "0x0",
            "blockHash": format!("{:#x}", B256::repeat_byte(9)),
            "blockNumber": "0xa",
            "gasUsed": "0x1",
            "effectiveGasPrice": "0x1",
            "from": format!("{:#x}", Address::ZERO),
            "to": format!("{:#x}", Address::repeat_byte(1)),
            "contractAddress": null
        }))
        .expect("valid receipt fixture")
    }

    fn rpc_log(block_number: u64, log_index: u64) -> alloy::rpc::types::Log {
        serde_json::from_value(serde_json::json!({
            "address": format!("{:#x}", Address::ZERO),
            "topics": [format!("{:#x}", B256::ZERO)],
            "data": "0x",
            "blockHash": format!("{:#x}", B256::repeat_byte(9)),
            "blockNumber": format!("0x{block_number:x}"),
            "transactionHash": format!("{:#x}", B256::repeat_byte(block_number as u8)),
            "transactionIndex": "0x0",
            "logIndex": format!("0x{log_index:x}"),
            "removed": false
        }))
        .expect("valid log fixture")
    }

    #[tokio::test]
    async fn fetches_a_supported_log_range_once() {
        let asserter = Asserter::new();
        asserter.push_success(&Vec::<alloy::rpc::types::Log>::new());

        assert!(source(asserter.clone()).adaptive_logs(query(10, 12)).await.unwrap().is_empty());
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn bisects_provider_log_range_limits() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("block range exceeds provider limit");
        asserter.push_success(&Vec::<alloy::rpc::types::Log>::new());
        asserter.push_success(&Vec::<alloy::rpc::types::Log>::new());

        assert!(source(asserter.clone()).adaptive_logs(query(10, 11)).await.unwrap().is_empty());
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn bisects_plural_provider_block_range_limits() {
        let asserter = Asserter::new();
        asserter.push_failure(rpc_error(
            -32602,
            "eth_getLogs and eth_newFilter are limited to a 10,000 blocks range",
        ));
        asserter.push_success(&Vec::<alloy::rpc::types::Log>::new());
        asserter.push_success(&Vec::<alloy::rpc::types::Log>::new());

        assert!(source(asserter.clone()).adaptive_logs(query(10, 11)).await.unwrap().is_empty());
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn sorts_logs_after_ranged_fetching() {
        let asserter = Asserter::new();
        asserter.push_success(&vec![rpc_log(12, 3), rpc_log(10, 2), rpc_log(10, 1)]);

        let logs = source(asserter).adaptive_logs(query(10, 12)).await.unwrap();

        assert_eq!(
            logs.iter().map(|log| (log.block_number, log.log_index)).collect::<Vec<_>>(),
            [(Some(10), Some(1)), (Some(10), Some(2)), (Some(12), Some(3))]
        );
    }

    #[tokio::test]
    async fn does_not_split_unrelated_rpc_failures() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("rate limit exceeded");
        asserter.push_success(&Vec::<alloy::rpc::types::Log>::new());

        assert!(source(asserter.clone()).adaptive_logs(query(10, 11)).await.is_err());
        assert_eq!(asserter.read_q().len(), 1);
    }

    #[tokio::test]
    async fn does_not_split_rpc_rate_limits() {
        let asserter = Asserter::new();
        asserter.push_failure(rpc_error(-32005, "rate limited, try again later"));
        asserter.push_success(&Vec::<alloy::rpc::types::Log>::new());

        assert!(source(asserter.clone()).adaptive_logs(query(10, 11)).await.is_err());
        assert_eq!(asserter.read_q().len(), 1);
    }

    #[tokio::test]
    async fn fails_when_one_block_exceeds_the_log_limit() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("too many log results");

        let error = source(asserter).adaptive_logs(query(10, 10)).await.unwrap_err();
        assert!(error.to_string().contains("single block 10"));
    }

    #[test]
    fn exact_log_filters_never_create_cross_product_targets() {
        let first_address = Address::repeat_byte(1);
        let second_address = Address::repeat_byte(2);
        let first_topic = B256::repeat_byte(3);
        let second_topic = B256::repeat_byte(4);

        let filters = exact_log_filters(&[
            LogTarget::new(first_address, first_topic),
            LogTarget::new(second_address, second_topic),
        ]);

        assert_eq!(filters.len(), 2);
        assert!(filters.iter().any(|filter| filter.address.matches(&first_address)
            && filter.topics[0].matches(&first_topic)));
        assert!(!filters.iter().any(|filter| {
            filter.address.matches(&first_address) && filter.topics[0].matches(&second_topic)
        }));
    }

    #[tokio::test]
    async fn block_receipts_are_reordered_to_match_requested_hashes() {
        let asserter = Asserter::new();
        let first = B256::repeat_byte(1);
        let second = B256::repeat_byte(2);
        asserter.push_success(&vec![receipt(second, false), receipt(first, true)]);
        let source = source(asserter);

        let outcomes =
            fetch::fetch_block_receipts(&source.provider, 10, &[first, second]).await.unwrap();

        assert_eq!(outcomes[0].transaction_hash, first);
        assert!(outcomes[0].succeeded);
        assert_eq!(outcomes[1].transaction_hash, second);
        assert!(!outcomes[1].succeeded);
    }

    #[tokio::test]
    async fn receipt_batches_preserve_order_across_concurrent_chunks() {
        let asserter = Asserter::new();
        let hashes = (1..=5).map(B256::repeat_byte).collect::<Vec<_>>();
        for hash in &hashes {
            asserter.push_success(&receipt(*hash, true));
        }
        let source = source_with_batch_size(asserter, 2);

        let outcomes = source.batched_receipts(&hashes).await.unwrap();

        assert_eq!(
            outcomes.iter().map(|outcome| outcome.transaction_hash).collect::<Vec<_>>(),
            hashes
        );
    }

    #[tokio::test]
    async fn missing_block_receipts_method_falls_back_and_caches_capability() {
        let asserter = Asserter::new();
        let hashes = (1..=3).map(B256::repeat_byte).collect::<Vec<_>>();
        asserter.push_failure(ErrorPayload::method_not_found());
        for hash in &hashes {
            asserter.push_success(&receipt(*hash, true));
        }
        let source = source_with_batch_size(asserter.clone(), 2);

        let outcomes = source.optimized_receipts(10, &hashes).await.unwrap();

        assert_eq!(outcomes.len(), hashes.len());
        assert_eq!(
            source.block_receipts_capability.load(Ordering::Relaxed),
            CAPABILITY_UNSUPPORTED
        );
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn unsupported_batches_fall_back_to_ordered_single_receipts() {
        let asserter = Asserter::new();
        let hashes = [B256::repeat_byte(1), B256::repeat_byte(2)];
        asserter.push_failure(ErrorPayload::invalid_request());
        asserter.push_failure(ErrorPayload::invalid_request());
        for hash in hashes {
            asserter.push_success(&receipt(hash, true));
        }
        let source = source_with_batch_size(asserter.clone(), 2);

        let outcomes = source.optimized_receipts(10, &hashes).await.unwrap();

        assert_eq!(
            outcomes.iter().map(|outcome| outcome.transaction_hash).collect::<Vec<_>>(),
            hashes
        );
        assert_eq!(source.batch_capability.load(Ordering::Relaxed), CAPABILITY_UNSUPPORTED);
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn transient_block_receipt_failures_do_not_fan_out() {
        let asserter = Asserter::new();
        let hashes = (1..=3).map(B256::repeat_byte).collect::<Vec<_>>();
        asserter.push_failure(rpc_error(-32005, "rate limited"));
        asserter.push_success(&receipt(hashes[0], true));
        let source = source_with_batch_size(asserter.clone(), 2);

        assert!(source.optimized_receipts(10, &hashes).await.is_err());
        assert_eq!(asserter.read_q().len(), 1);
    }

    #[tokio::test]
    async fn set_rpc_url_rotates_the_transport_and_resets_endpoint_capabilities() {
        let source = JsonRpcBlockSource::connect(
            &"http://localhost:8545".parse().unwrap(),
            RpcConfig::default(),
            Arc::new(NoopTelemetry),
        )
        .unwrap();
        source.batch_capability.store(CAPABILITY_SUPPORTED, Ordering::Relaxed);
        source.block_receipts_capability.store(CAPABILITY_SUPPORTED, Ordering::Relaxed);

        BlockSource::set_rpc_url(&source, &"http://localhost:9545".parse().unwrap()).unwrap();

        assert_eq!(source.batch_capability.load(Ordering::Relaxed), CAPABILITY_UNKNOWN);
        assert_eq!(source.block_receipts_capability.load(Ordering::Relaxed), CAPABILITY_UNKNOWN);
    }

    #[tokio::test]
    async fn set_rpc_url_bails_for_mocked_transports() {
        let source = source(Asserter::new());

        assert!(
            BlockSource::set_rpc_url(&source, &"http://localhost:9545".parse().unwrap()).is_err()
        );
    }

    #[test]
    fn redacts_transport_errors_but_preserves_safe_response_errors() {
        let transport = anyhow::Error::new(alloy::transports::TransportErrorKind::custom_str(
            "https://secret.invalid failed",
        ));
        let transport = super::source_request_error(transport);
        assert!(transport.downcast_ref::<parseon_core::ports::BlockSourceRequestError>().is_some());

        let response = anyhow::Error::new(fetch::BlockReceiptsResponseError::MissingBlock(10));
        let response = super::source_request_error(response);
        assert!(response.downcast_ref::<fetch::BlockReceiptsResponseError>().is_some());
    }
}
