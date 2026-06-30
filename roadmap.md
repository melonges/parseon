# Parseon Roadmap

Parseon is being built as a small, deterministic EVM indexing core with adapter-based integrations around it. Users should be able to describe the contract calls they care about, run one service, and own the resulting decoded data.

## Architecture direction

```text
parseon-core
  ABI decoding
  block scheduling
  monitor matching
  reorg and finality handling
  runtime filters
  parallel indexing engine

adapters
  storage: PostgreSQL first, MongoDB later
  rpc: JSON-RPC first, eRPC / Etherscan later
  cache: in-memory first, Redis later

server
  HTTP API
  OpenAPI / Swagger
  future GraphQL API
  future management frontend
```

The core should stay independent from a specific database, RPC provider, cache backend, or API surface. Adapters should customize how Parseon reads chain data, stores decoded results, and caches fetched blocks without changing the indexing logic itself.

The first goal is compile-time adapters with stable Rust traits. Runtime-loaded plugins may come much later, if the project actually needs that level of flexibility.

## v0.1 — Single-chain MVP

Current development baseline.

- Single-chain EVM indexing.
- Monitor CRUD API.
- ABI calldata decoding.
- PostgreSQL result storage.
- HTTP API with OpenAPI / Swagger UI.
- Docker-based local development.

## v0.2 — Core extraction and adapter boundaries

Prepare the architecture before adding more moving parts.

- Extract `parseon-core` domain models and indexing pipeline.
- Introduce a storage trait.
- Introduce a chain data source trait.
- Introduce a block cache trait.
- Keep PostgreSQL, JSON-RPC, and in-memory cache as first official implementations.
- Keep behavior compatible with the current MVP.

## v0.3 — Reorg handling and finality guarantees

Make returned data trustworthy and explicit.

- Store indexed block metadata: number, hash, parent hash, timestamp, and finality status.
- Support finalized-only and confirmation-depth indexing modes.
- Detect reorgs by validating parent hashes.
- Roll back decoded rows and monitor cursors on reorg.
- Expose chain progress and data finality through the API.
- Default result queries to finalized data where possible.

## v0.4 — Multi-chain indexing

Run one Parseon instance across multiple EVM chains.

- Add a chain registry.
- Make monitors chain-aware.
- Make result storage chain-aware.
- Run one worker per chain.
- Isolate chain state, errors, cache, and finality settings.
- Allow chains to be enabled, disabled, or updated without restarting the service.

## v0.5 — Parallel execution and performance

Scale indexing without duplicating RPC work.

- Add bounded concurrent block fetching.
- Add bounded concurrent receipt fetching.
- Reuse fetched blocks across monitors.
- Support RPC batching where available.
- Add optional `eth_getBlockReceipts` support where providers expose it.
- Add backpressure for RPC and database writes.
- Expose Prometheus-compatible metrics.

## v0.6 — Rich API and management surface

Prepare Parseon for a future frontend.

- Add richer chain, monitor, worker, and result endpoints.
- Add pause, resume, and reindex operations.
- Add cursor-based pagination.
- Add advanced result filtering.
- Keep OpenAPI first-class.
- Consider GraphQL after the HTTP model stabilizes.

## v0.7 — Optional adapters

Expand the ecosystem around the core.

- Add Redis block cache adapter.
- Add eRPC gateway adapter for multi-provider routing, failover, and RPC caching.
- Add Etherscan-style adapter for backfill, fallback reads, ABI discovery, or metadata enrichment.
- Experiment with MongoDB storage for document-oriented decoded results.
- Consider export sinks such as webhooks, Kafka, files, or ClickHouse.

## v0.8 — Runtime monitor filters

Let each monitor decide which decoded calls should be stored.

- Add a safe JSON filter DSL.
- Filter by transaction metadata, decoded parameters, status, block range, and sender / receiver.
- Add a filter test endpoint.
- Benchmark filter overhead.
- Consider WASM filters later for advanced users.

## v1.0 — Production self-hosted Parseon

Make Parseon reliable to operate as infrastructure.

- Stable database schema.
- Stable HTTP API.
- Documented consistency model.
- Multi-chain support.
- Reorg-safe indexing.
- Production Docker image.
- Production Docker Compose example.
- Helm chart.
- Prometheus / Grafana dashboards.
- Backup and restore documentation.
- Upgrade and migration guide.

## Current priorities

1. Extract a clean core and adapter traits.
2. Implement finality and reorg guarantees.
3. Add multi-chain workers.
4. Optimize parallel indexing.
5. Build richer APIs for management and querying.
6. Add Redis, eRPC, Etherscan, and other adapters only after the core is stable.
