# Parseon Roadmap

Parseon is being built as a small, deterministic EVM indexing core with adapter-based integrations around it. Users should be able to describe the contract calls they care about, run one service, and own the resulting decoded data.

> Terminology: this roadmap follows the naming conventions defined in [`terminology.md`](./terminology.md).

## Architecture direction

Parseon should keep a simple repository shape while the domain model is still moving. The near-term goal is **one binary crate with clear internal modules and stable traits**, not a large workspace full of premature crate boundaries.

```text
src/
  api/          HTTP API, OpenAPI / Swagger, future management surface
  abi/          function signature parsing and calldata decoding
  cache/        cache traits and in-memory implementation
  config/       CLI/env config loading
  core/         domain models: Chain, Monitor, Target, Cursor, DecodedCall
  db/           PostgreSQL storage implementation
  filter/       monitor Filter DSL and evaluator
  indexer/      indexing pipeline and persistence orchestration
  monitor/      monitor model and matching helpers
  rpc/          BlockSource trait and JSON-RPC implementation
  scheduler/    block range planning and batching
  worker/       chain worker runtime
```

The core logic should stay independent from a specific database, block source, cache backend, or API surface. Adapters should customize how Parseon reads chain data, stores decoded results, and caches fetched blocks without changing the indexing logic itself.

Use traits and modules first:

```text
Storage trait       -> PostgreSQL implementation first
BlockSource trait   -> JSON-RPC implementation first
BlockCache trait    -> in-memory implementation first
Sink trait          -> optional later
```

Avoid splitting into many crates until the core domain model, terminology, and trait boundaries have stabilized. Modules are cheap. Crates are expensive. Apparently even software architecture has taxes.

Separate crates may come later when there is a real reason:

- independent release cycle;
- independent dependency tree;
- independent test surface;
- external users;
- large optional integrations such as MongoDB, Redis, eRPC, or ClickHouse.

The first likely split, if needed, should be small:

```text
parseon-core     domain models, scheduling, reorg/finality, filters, decoded calls
parseon-server   CLI/config, HTTP API, OpenAPI, metrics, runtime wiring
```

Only after that should Parseon consider adapter crates such as `parseon-storage-postgres`, `parseon-cache-redis`, or `parseon-source-erpc`.

Runtime-loaded plugins are intentionally out of scope for early versions. Compile-time adapters with stable Rust traits are enough until actual users prove otherwise.

## v0.1 — Single-chain MVP

Initial development baseline.

- Single-chain EVM indexing.
- Monitor CRUD API.
- ABI calldata decoding.
- PostgreSQL result storage.
- HTTP API with OpenAPI / Swagger UI.
- Docker-based local development.

## v0.2 — Internal boundaries and adapter traits

Prepare the architecture before adding more moving parts, without slowing development with premature workspace crates.

Status: implemented in v0.2.0.

- Keep Parseon as one binary crate.
- Reorganize toward clear internal modules: `core`, `monitor`, `filter`, `cache`, `rpc`, `scheduler`, and `worker`.
- Introduce domain models for `Chain`, `Monitor`, `Target`, `Cursor`, and `DecodedCall`.
- Introduce a `Storage` trait.
- Introduce a `BlockSource` trait.
- Introduce a `BlockCache` trait.
- Keep PostgreSQL, JSON-RPC, and in-memory cache as first official implementations.
- Keep behavior compatible with the current MVP.
- Defer separate crates until boundaries prove stable.

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
- Make monitor targets chain-aware.
- Make result storage chain-aware.
- Run one worker per chain.
- Isolate chain state, errors, cache, and finality settings.
- Allow chains to be enabled, disabled, or updated without restarting the service.

## v0.5 — Parallel execution and performance

Scale indexing without duplicating block source work.

- Add bounded concurrent block fetching.
- Add bounded concurrent receipt fetching.
- Reuse fetched blocks across monitors.
- Support RPC batching where available.
- Add optional `eth_getBlockReceipts` support where block sources expose it.
- Add backpressure for block source calls and database writes.
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

Expand the ecosystem around the core after internal traits are stable.

- Add Redis block cache adapter.
- Add eRPC block source adapter for multi-provider routing, failover, and RPC caching.
- Add Etherscan-style block source adapter for backfill, fallback reads, ABI discovery, or metadata enrichment.
- Experiment with MongoDB storage for document-oriented decoded results.
- Consider sink adapters such as webhooks, Kafka, files, or ClickHouse.
- Reevaluate whether any adapter should become a separate crate based on dependency weight and reuse.

## v0.8 — Runtime monitor filters

Let each monitor decide which decoded calls should be stored.

- Add a safe JSON Filter DSL.
- Filter by transaction metadata, decoded parameters, status, block range, and sender / receiver.
- Add a filter test endpoint.
- Benchmark filter overhead.
- Consider WASM filters later for advanced users.

## v0.9 — Crate split evaluation

Split only if the project has enough stability and real pressure to justify it.

- Evaluate whether `parseon-core` should become a separate crate.
- Evaluate whether `parseon-server` should become a separate crate.
- Keep storage, cache, block source, and sink implementations as modules unless dependency weight or reuse requires a crate boundary.
- Avoid creating crates only to mirror folders.
- Document crate-boundary rules before moving code.

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

1. Implement finality and reorg guarantees.
2. Add multi-chain workers.
3. Optimize parallel indexing.
4. Build richer APIs for management and querying.
5. Add Redis, eRPC, Etherscan, MongoDB, and sink adapters only after the core traits are stable.
6. Reevaluate separate crates near v0.9, not before the domain model settles.
