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

See the [roadmap](./roadmap.md) for planned milestones, the [terminology guide](./terminology.md) for domain language, and the [changelog](./CHANGELOG.md) for completed work.

## Adapters

Parseon supports compile-time PostgreSQL or MongoDB storage, direct JSON-RPC endpoints including full eRPC gateway routes, a configurable in-memory block cache, and an optional best-effort webhook sink.

```text
parseon-server
├── parseon-core
├── parseon-rpc ─────────────> parseon-core
├── parseon-postgres ────────> parseon-core
├── parseon-mongodb ─────────> parseon-core
├── parseon-memory-cache ────> parseon-core
└── parseon-webhook-sink ────> parseon-core
```

Start PostgreSQL with `docker compose up -d`, or start the MongoDB development replica set and eRPC gateway with `docker compose --profile mongodb --profile erpc up -d`. Configure the selected backend through `STORAGE_URL` and set a non-empty `API_TOKEN`; protected API routes require `Authorization: Bearer <API_TOKEN>`. Register direct RPC or complete eRPC URLs through `POST /chains`. Private/loopback RPC destinations are rejected unless `ALLOW_PRIVATE_RPC_NETWORKS=true` is explicitly enabled for local development.

See [adapter configuration and guarantees](./docs/adapters.md) for feature builds, MongoDB requirements, eRPC smoke checks, the webhook JSON contract, and Compose profiles.

## Production deployment

Use [`compose.production.yml`](./compose.production.yml) or the [`deploy/helm/parseon`](./deploy/helm/parseon) chart. Both require an externally managed `API_TOKEN` and storage URL, keep databases private, and expose liveness/readiness probes. Read the [production operations runbook](./docs/operations.md) before upgrading, restoring, or exposing the API through an ingress.

## License

Licensed under either the [Apache License, Version 2.0](./LICENSE-APACHE) or the [MIT license](./LICENSE-MIT), at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Parseon by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
