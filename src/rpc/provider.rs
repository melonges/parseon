use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU8, Ordering};
use std::time::Instant;

use alloy::eips::BlockNumberOrTag;
use alloy::network::AnyNetwork;
use alloy::network::BlockResponse;
use alloy::primitives::{Address, B256};
use alloy::providers::{Provider, RootProvider};
use alloy::transports::http::reqwest::Client;
use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::core::ports::{BlockSource, BlockSourceFactory, InFlightGuard, NoopTelemetry, Telemetry};
use crate::core::{BlockTransaction, ExecutedTransaction, SourceBlock, SourceLog};
use crate::error::{AppError, AppResult};
use crate::rpc::fetch;

pub type HttpProvider = RootProvider<AnyNetwork>;

const CAPABILITY_UNKNOWN: u8 = 0;
const CAPABILITY_SUPPORTED: u8 = 1;
const CAPABILITY_UNSUPPORTED: u8 = 2;

#[derive(Debug, Clone, Copy)]
pub struct RpcConfig {
    pub request_concurrency: usize,
    pub batch_size: usize,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            request_concurrency: 16,
            batch_size: 20,
        }
    }
}

pub struct JsonRpcBlockSource {
    provider: HttpProvider,
    request_concurrency: usize,
    batch_size: usize,
    permits: Arc<Semaphore>,
    chain_id: AtomicI64,
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
    fn connect(&self, rpc_url: &str) -> anyhow::Result<Arc<dyn BlockSource>> {
        Ok(Arc::new(JsonRpcBlockSource::connect(
            rpc_url,
            self.config,
            self.telemetry.clone(),
        )?))
    }
}

impl JsonRpcBlockSource {
    pub fn connect(
        rpc_url: &str,
        config: RpcConfig,
        telemetry: Arc<dyn Telemetry>,
    ) -> AppResult<Self> {
        let request_concurrency = config.request_concurrency.max(1);
        Ok(Self {
            provider: build(rpc_url)?,
            request_concurrency,
            batch_size: config.batch_size.max(1),
            permits: Arc::new(Semaphore::new(request_concurrency)),
            chain_id: AtomicI64::new(-1),
            batch_capability: AtomicU8::new(CAPABILITY_UNKNOWN),
            block_receipts_capability: AtomicU8::new(CAPABILITY_UNKNOWN),
            telemetry,
        })
    }

    async fn acquire(&self) -> anyhow::Result<OwnedSemaphorePermit> {
        Ok(self.permits.clone().acquire_owned().await?)
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
        let chain_id = self.chain_id.load(Ordering::Relaxed);
        let _in_flight =
            (chain_id >= 0).then(|| InFlightGuard::new(self.telemetry.as_ref(), chain_id, "rpc"));
        let started = Instant::now();
        let result = future.await;
        if chain_id >= 0 {
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
        transactions: &[BlockTransaction],
    ) -> anyhow::Result<Vec<ExecutedTransaction>> {
        let mut receipts =
            stream::iter(transactions.iter().cloned().map(|transaction| async move {
                self.observed("receipts", "single", async {
                    Ok(fetch::fetch_receipt(&self.provider, &transaction).await?)
                })
                .await
            }))
            .buffered(self.request_concurrency);
        let mut out = Vec::with_capacity(transactions.len());
        while let Some(receipt) = receipts.next().await {
            out.push(receipt?);
        }
        Ok(out)
    }

    async fn batched_receipts(
        &self,
        transactions: &[BlockTransaction],
    ) -> anyhow::Result<Vec<ExecutedTransaction>> {
        let chunks = transactions
            .chunks(self.batch_size)
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<_>>();
        let mut batches = stream::iter(chunks.into_iter().map(|chunk| async move {
            self.observed("receipts", "batch", async {
                Ok(fetch::fetch_receipt_batch(&self.provider, &chunk).await?)
            })
            .await
        }))
        .buffered(self.request_concurrency);
        let mut out = Vec::with_capacity(transactions.len());
        while let Some(batch) = batches.next().await {
            out.extend(batch?);
        }
        Ok(out)
    }

    async fn optimized_receipts(
        &self,
        block: &SourceBlock,
        transactions: &[BlockTransaction],
    ) -> anyhow::Result<Vec<ExecutedTransaction>> {
        if transactions.is_empty() {
            return Ok(Vec::new());
        }
        if transactions.len() == 1 {
            return self.single_receipts(transactions).await;
        }

        if transactions.len() > self.batch_size
            && self.block_receipts_capability.load(Ordering::Relaxed) != CAPABILITY_UNSUPPORTED
        {
            let block_receipts = self
                .observed("receipts", "block_receipts", async {
                    Ok(fetch::fetch_block_receipts(
                        &self.provider,
                        u64::try_from(block.number)?,
                        transactions,
                    )
                    .await?)
                })
                .await;
            match block_receipts {
                Ok(receipts) => {
                    self.block_receipts_capability
                        .store(CAPABILITY_SUPPORTED, Ordering::Relaxed);
                    return Ok(receipts);
                }
                Err(error) => {
                    if unsupported_method(&error) {
                        self.block_receipts_capability
                            .store(CAPABILITY_UNSUPPORTED, Ordering::Relaxed);
                    }
                    tracing::debug!(
                        chain_id = self.chain_id.load(Ordering::Relaxed),
                        "block receipt optimization unavailable; falling back"
                    );
                }
            }
        }

        if self.batch_capability.load(Ordering::Relaxed) != CAPABILITY_UNSUPPORTED {
            match self.batched_receipts(transactions).await {
                Ok(receipts) => {
                    self.batch_capability
                        .store(CAPABILITY_SUPPORTED, Ordering::Relaxed);
                    return Ok(receipts);
                }
                Err(error) => {
                    if unsupported_batch(&error) {
                        self.batch_capability
                            .store(CAPABILITY_UNSUPPORTED, Ordering::Relaxed);
                    }
                    tracing::debug!(
                        chain_id = self.chain_id.load(Ordering::Relaxed),
                        "RPC batching unavailable; falling back"
                    );
                }
            }
        }
        self.single_receipts(transactions).await
    }
}

#[async_trait]
impl BlockSource for JsonRpcBlockSource {
    async fn chain_id(&self) -> anyhow::Result<u64> {
        let _permit = self.acquire().await?;
        let started = Instant::now();
        let result = chain_id(&self.provider).await.map_err(anyhow::Error::from);
        match result {
            Ok(chain_id) => {
                if let Ok(metric_chain_id) = i64::try_from(chain_id) {
                    self.chain_id.store(metric_chain_id, Ordering::Relaxed);
                    self.telemetry.record_rpc(
                        metric_chain_id,
                        "chain_id",
                        "single",
                        "success",
                        started.elapsed(),
                    );
                }
                Ok(chain_id)
            }
            Err(error) => Err(error),
        }
    }

    async fn finalized_head(&self) -> anyhow::Result<i64> {
        self.observed("finalized_head", "single", async {
            Ok(i64::try_from(finalized_number(&self.provider).await?)?)
        })
        .await
    }

    async fn fetch_block(&self, block_number: i64) -> anyhow::Result<SourceBlock> {
        self.observed("block", "single", async {
            Ok(fetch::fetch_block(&self.provider, u64::try_from(block_number)?).await?)
        })
        .await
    }

    async fn fetch_executed_transactions(
        &self,
        block: &SourceBlock,
        transactions: &[BlockTransaction],
    ) -> anyhow::Result<Vec<ExecutedTransaction>> {
        self.optimized_receipts(block, transactions).await
    }

    async fn fetch_logs(
        &self,
        block_number: i64,
        addresses: &[Address],
        topic0s: &[B256],
    ) -> anyhow::Result<Vec<SourceLog>> {
        self.observed("logs", "single", async {
            Ok(fetch::fetch_logs(
                &self.provider,
                u64::try_from(block_number)?,
                addresses,
                topic0s,
            )
            .await?)
        })
        .await
    }
}

fn unsupported_method(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    message.contains("-32601") || message.contains("method not found")
}

fn unsupported_batch(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    message.contains("batch")
        && (message.contains("not supported")
            || message.contains("unsupported")
            || message.contains("invalid request"))
}

pub fn build(rpc_url: &str) -> AppResult<HttpProvider> {
    let url = rpc_url
        .parse()
        .map_err(|e| AppError::BadRequest(format!("invalid rpc_url: {e}")))?;
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| AppError::Internal(anyhow::anyhow!("http client: {e}")))?;
    let rpc_client = alloy::rpc::client::ClientBuilder::default().http_with_client(client, url);
    Ok(RootProvider::<AnyNetwork>::new(rpc_client))
}

pub async fn chain_id(provider: &HttpProvider) -> AppResult<u64> {
    provider.get_chain_id().await.map_err(AppError::Rpc)
}

pub async fn finalized_number(provider: &HttpProvider) -> AppResult<u64> {
    let block = provider
        .get_block_by_number(BlockNumberOrTag::Finalized)
        .await
        .map_err(AppError::Rpc)?
        .ok_or_else(|| AppError::NotFound("finalized block".to_string()))?;
    Ok(block.header().number)
}
