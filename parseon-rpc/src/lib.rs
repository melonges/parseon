//! Alloy-backed Ethereum JSON-RPC implementation of Parseon's block-source port.

mod fetch;
mod provider;

pub use provider::{JsonRpcBlockSource, JsonRpcBlockSourceFactory, RpcConfig};
