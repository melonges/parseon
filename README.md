<p align="center">
  <img width="128" height="128" src="https://github.com/user-attachments/assets/c7c2198d-0be3-42c4-ba22-aa4f6d7e7e84" alt="Parseon logo" />
</p>

<h1 align="center">Parseon</h1>

<p align="center">
  <strong>Turn EVM calldata into queryable data</strong><br />
  Parseon is a small, self-hosted multi-chain EVM indexer built in Rust
</p>

<p align="center">
  <img alt="Early development" src="https://img.shields.io/badge/status-early_development-151515?style=flat-square" />
  <img alt="Rust" src="https://img.shields.io/badge/Rust-151515?style=flat-square&logo=rust&logoColor=white" />
  <img alt="EVM" src="https://img.shields.io/badge/EVM-151515?style=flat-square&logo=ethereum&logoColor=white" />
</p>

> [!WARNING]
> **Parseon is in early development.** Only suitable for use by developers.

## The direction

Parseon is being built to make focused onchain indexing simple: describe the calls and events you care about, run one service, and own the resulting data.

The project is moving quickly. Issues, ideas, and early contributions are welcome.

## Register a chain and monitor

Parseon starts without an RPC endpoint. Register each EVM chain through the API; Parseon discovers its EIP-155 chain ID, verifies finalized-block support, and starts an isolated worker. RPC URLs are write-only and are never returned or logged.

```bash
curl -X POST http://127.0.0.1:8080/chains \
  -H 'content-type: application/json' \
  -d '{"rpc_url":"https://mainnet.base.org","enabled":true}'
```

Create a monitor using the discovered `chain_id`:

```bash
curl -X POST http://127.0.0.1:8080/monitors \
  -H 'content-type: application/json' \
  -d '{
    "chain_id":8453,
    "address":"0x0000000000000000000000000000000000000000",
    "signature":"function transfer(address to, uint256 value)",
    "start_block":0
  }'
```

Use `GET /status` for per-chain worker state and `GET /monitors?chain_id=8453` to list one chain's monitors. All indexing is finalized-only.

See the [roadmap](./roadmap.md) for planned milestones, the [terminology guide](./terminology.md) for domain language, and the [changelog](./CHANGELOG.md) for completed work.

## License

Licensed under either the [Apache License, Version 2.0](./LICENSE-APACHE) or the [MIT license](./LICENSE-MIT), at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Parseon by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
