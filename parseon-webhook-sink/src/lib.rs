use std::num::NonZeroUsize;
use std::sync::Arc;

use parseon_core::Url;
use parseon_core::ports::{Sink, SinkBatch};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct WebhookSink {
    inner: Arc<Inner>,
}

struct Inner {
    client: reqwest::Client,
    url: Url,
    attempts: Arc<Semaphore>,
    cancel: CancellationToken,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl WebhookSink {
    pub fn new(url: Url, concurrency: NonZeroUsize) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Inner {
                client: reqwest::Client::builder()
                    .redirect(reqwest::redirect::Policy::none())
                    .build()?,
                url,
                attempts: Arc::new(Semaphore::new(concurrency.get())),
                cancel: CancellationToken::new(),
            }),
        })
    }
}

impl Sink for WebhookSink {
    fn submit(&self, batch: SinkBatch) {
        if batch.results.is_empty() || self.inner.cancel.is_cancelled() {
            return;
        }
        let Ok(permit) = self.inner.attempts.clone().try_acquire_owned() else {
            tracing::warn!("webhook batch dropped because delivery is saturated");
            return;
        };
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let attempt = inner.client.post(inner.url.clone()).json(&batch).send();
            let result = tokio::select! {
                biased;
                () = inner.cancel.cancelled() => return,
                result = attempt => result,
            };
            match result {
                Ok(response) if response.status().is_success() => {}
                Ok(_) => tracing::warn!("webhook delivery returned a non-success status"),
                Err(_) => tracing::warn!("webhook delivery failed"),
            }
            drop(permit);
        });
    }

    fn shutdown(&self) {
        self.inner.cancel.cancel();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode, header};
    use axum::routing::post;
    use axum::{Json, Router};
    use parseon_core::ports::SinkResult;
    use parseon_core::{Address, B256};
    use tokio::sync::{Mutex, Notify};

    use super::*;

    type CapturedRequests = Arc<Mutex<Vec<(HeaderMap, serde_json::Value)>>>;

    fn batch() -> SinkBatch {
        SinkBatch {
            version: 1,
            chain_id: 8453,
            block_number: 123,
            results: vec![
                SinkResult::Call {
                    monitor_id: 7,
                    tx_hash: B256::repeat_byte(1),
                    from: Address::repeat_byte(2),
                    to: Address::repeat_byte(3),
                    params: serde_json::json!({"value": "42"}),
                },
                SinkResult::Event {
                    monitor_id: 8,
                    tx_hash: B256::repeat_byte(4),
                    emitter: Address::repeat_byte(5),
                    log_index: 3,
                    params: serde_json::json!({"owner": format!("{:#x}", Address::repeat_byte(6))}),
                },
            ],
        }
    }

    async fn serve(router: Router) -> (Url, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move { drop(axum::serve(listener, router).await) });
        (format!("http://{address}").parse().unwrap(), handle)
    }

    async fn wait_for(predicate: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !predicate() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn serializes_published_json_contract() {
        let value = serde_json::to_value(batch()).unwrap();
        assert_eq!(value["version"], 1);
        assert_eq!(value["chain_id"], 8453);
        assert_eq!(value["block_number"], 123);
        assert_eq!(value["results"][0]["kind"], "call");
        assert_eq!(value["results"][0]["monitor_id"], 7);
        assert_eq!(value["results"][0]["params"]["value"], "42");
        assert_eq!(value["results"][1]["kind"], "event");
        assert_eq!(value["results"][1]["log_index"], 3);
        assert!(value.get("delivery_id").is_none());
        assert!(value.get("delivery_timestamp").is_none());
    }

    #[tokio::test]
    async fn accepts_every_2xx_and_sends_no_empty_batch() {
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let router = Router::new()
            .route(
                "/hook",
                post(
                    |State(bodies): State<CapturedRequests>,
                     headers: HeaderMap,
                     Json(body)| async move {
                        bodies.lock().await.push((headers, body));
                        StatusCode::NO_CONTENT
                    },
                ),
            )
            .with_state(bodies.clone());
        let (base, handle) = serve(router).await;
        let sink =
            WebhookSink::new(base.join("hook").unwrap(), NonZeroUsize::new(1).unwrap()).unwrap();
        sink.submit(SinkBatch { results: Vec::new(), ..batch() });
        sink.submit(batch());
        wait_for(|| bodies.try_lock().is_ok_and(|bodies| bodies.len() == 1)).await;
        let bodies = bodies.lock().await;
        assert_eq!(bodies[0].1["version"], 1);
        assert!(bodies[0].0.get(header::AUTHORIZATION).is_none());
        handle.abort();
    }

    #[tokio::test]
    async fn does_not_retry_failures_or_follow_redirects() {
        let failures = Arc::new(AtomicUsize::new(0));
        let redirects = Arc::new(AtomicUsize::new(0));
        let successes = Arc::new(AtomicUsize::new(0));
        let router = Router::new()
            .route(
                "/failure",
                post(
                    |State((failures, _, _)): State<(
                        Arc<AtomicUsize>,
                        Arc<AtomicUsize>,
                        Arc<AtomicUsize>,
                    )>| async move {
                        failures.fetch_add(1, Ordering::SeqCst);
                        StatusCode::INTERNAL_SERVER_ERROR
                    },
                ),
            )
            .route(
                "/redirect",
                post(
                    |State((_, redirects, _)): State<(
                        Arc<AtomicUsize>,
                        Arc<AtomicUsize>,
                        Arc<AtomicUsize>,
                    )>| async move {
                        redirects.fetch_add(1, Ordering::SeqCst);
                        let mut headers = HeaderMap::new();
                        headers.insert(header::LOCATION, "/success".parse().unwrap());
                        (StatusCode::TEMPORARY_REDIRECT, headers)
                    },
                ),
            )
            .route(
                "/success",
                post(
                    |State((_, _, successes)): State<(
                        Arc<AtomicUsize>,
                        Arc<AtomicUsize>,
                        Arc<AtomicUsize>,
                    )>| async move {
                        successes.fetch_add(1, Ordering::SeqCst);
                        StatusCode::OK
                    },
                ),
            )
            .with_state((failures.clone(), redirects.clone(), successes.clone()));
        let (base, handle) = serve(router).await;
        let concurrency = NonZeroUsize::new(1).unwrap();
        WebhookSink::new(base.join("failure").unwrap(), concurrency).unwrap().submit(batch());
        WebhookSink::new(base.join("redirect").unwrap(), concurrency).unwrap().submit(batch());
        wait_for(|| failures.load(Ordering::SeqCst) == 1 && redirects.load(Ordering::SeqCst) == 1)
            .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(failures.load(Ordering::SeqCst), 1);
        assert_eq!(successes.load(Ordering::SeqCst), 0);

        let unused = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let network_failure =
            Url::parse(&format!("http://{}", unused.local_addr().unwrap())).unwrap();
        drop(unused);
        WebhookSink::new(network_failure, concurrency).unwrap().submit(batch());
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.abort();
    }

    #[tokio::test]
    async fn drops_when_saturated_has_no_attempt_timeout_and_cancels_shutdown() {
        let started = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let router = Router::new()
            .route(
                "/hook",
                post(
                    |State((started, release)): State<(Arc<AtomicUsize>, Arc<Notify>)>| async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        release.notified().await;
                        StatusCode::OK
                    },
                ),
            )
            .with_state((started.clone(), release));
        let (base, handle) = serve(router).await;
        let sink =
            WebhookSink::new(base.join("hook").unwrap(), NonZeroUsize::new(1).unwrap()).unwrap();
        sink.submit(batch());
        wait_for(|| started.load(Ordering::SeqCst) == 1).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(sink.inner.attempts.available_permits(), 0);
        sink.submit(batch());
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(started.load(Ordering::SeqCst), 1);
        sink.shutdown();
        wait_for(|| sink.inner.attempts.available_permits() == 1).await;
        handle.abort();
    }

    #[test]
    fn accepts_syntactically_valid_unsupported_schemes() {
        assert!(
            WebhookSink::new(
                "ftp://example.com/hook".parse().unwrap(),
                NonZeroUsize::new(1).unwrap()
            )
            .is_ok()
        );
    }
}
