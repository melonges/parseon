//! HTTP transport with an in-place rotatable RPC URL.
//!
//! Alloy's [`Http`] transport owns its URL immutably once boxed inside an
//! `RpcClient`, but exposes the raw [`Http::set_url`] setter.
//! [`RotatingHttp`] keeps the transport behind a shared lock so the URL can
//! be rotated after the client (and every provider built on it) is already
//! connected: clones share the inner transport, so one `set_url` call covers
//! every request issued afterwards.

use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::task::{Context, Poll};

use alloy::transports::http::Http;
use alloy::transports::http::reqwest::{Client, redirect};
use alloy::transports::{TransportError, TransportFut};
use alloy_json_rpc::{RequestPacket, ResponsePacket};
use parking_lot::RwLock;
use parseon_core::Url;
use tower::Service;

/// A cloneable [`Http`] transport whose endpoint URL rotates in place via
/// [`Http::set_url`].
#[derive(Clone, Debug)]
pub(crate) struct RotatingHttp {
    inner: Arc<RwLock<Http<Client>>>,
}

impl RotatingHttp {
    pub(crate) fn new(client: Client, url: Url) -> Self {
        Self { inner: Arc::new(RwLock::new(Http::with_client(client, url))) }
    }

    /// Guess whether the current URL is local; see [`Http::guess_local`].
    pub(crate) fn guess_local(&self) -> bool {
        self.inner.read().guess_local()
    }

    /// Rotates the endpoint URL. In-flight requests finish against the old
    /// URL; subsequent requests use the new one.
    pub(crate) fn set_url(&self, url: Url, allow_private: bool) -> anyhow::Result<()> {
        let client = client_for_url(&url, allow_private)?;
        *self.inner.write() = Http::with_client(client, url);
        Ok(())
    }
}

pub(crate) fn client_for_url(url: &Url, allow_private: bool) -> anyhow::Result<Client> {
    let host = url.host_str().ok_or_else(|| anyhow::anyhow!("RPC URL must contain a host"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| anyhow::anyhow!("RPC URL must contain a port"))?;
    let addresses = (host, port)
        .to_socket_addrs()
        .map_err(|error| anyhow::anyhow!("RPC host resolution failed: {error}"))?
        .collect::<Vec<_>>();
    anyhow::ensure!(!addresses.is_empty(), "RPC host has no addresses");
    if !allow_private {
        for address in &addresses {
            anyhow::ensure!(
                !super::provider::is_private_address(address.ip()),
                "private RPC network is disabled"
            );
        }
    }
    Ok(Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .redirect(redirect::Policy::none())
        .resolve_to_addrs(host, &addresses)
        .build()?)
}

impl Service<RequestPacket> for RotatingHttp {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), TransportError>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: RequestPacket) -> Self::Future {
        // `Http::call` clones the transport before sending, so cloning here
        // adds one `Url` clone per request and never holds the lock across
        // the network I/O.
        self.inner.read().clone().call(request)
    }
}

#[cfg(test)]
mod tests {
    use super::{RotatingHttp, Url};
    use alloy::transports::http::reqwest::Client;

    #[test]
    fn set_url_rotates_the_shared_transport_for_every_clone() {
        let transport =
            RotatingHttp::new(Client::new(), Url::parse("http://localhost:8545").unwrap());
        let boxed_clone = transport.clone();

        transport.set_url(Url::parse("http://localhost:9545").unwrap(), true).unwrap();

        // The provider's boxed clone observes the rotation.
        assert_eq!(boxed_clone.inner.read().url(), "http://localhost:9545/");
    }
}
