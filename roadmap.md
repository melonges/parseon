# Parseon Roadmap

Parseon is being built as a small, deterministic EVM indexing core with adapter-based integrations around it. Users should be able to describe the contract calls they care about, run one service, and own the resulting decoded data.

> Terminology: this roadmap follows the naming conventions defined in [`terminology.md`](./terminology.md).

## Architecture direction

Parseon uses a small ports-and-adapters workspace. Core owns domain and application behavior; infrastructure crates implement core ports; the server is the composition root.

```text
parseon-server
├── parseon-core
├── parseon-rpc ──────────> parseon-core
├── parseon-postgres ─────> parseon-core
└── parseon-memory-cache ─> parseon-core
```

The core logic should stay independent from a specific database, block source, cache backend, or API surface. Adapters should customize how Parseon reads chain data, stores decoded results, and caches fetched blocks without changing the indexing logic itself.

The stable adapter boundaries are:

```text
IndexStorage / repository ports -> PostgreSQL implementation first
BlockSource port              -> JSON-RPC implementation first
BlockCache port               -> in-memory implementation first
Telemetry port                -> Prometheus implementation in the server
Sink port                     -> optional later
```

Core never depends on Axum, SQLx, Reqwest, Prometheus, the server, or adapter crates. Handlers invoke application services and return core-derived views. The production binary remains `parseon`.

Runtime-loaded plugins are intentionally out of scope for early versions. Compile-time adapters with stable Rust traits are enough until actual users prove otherwise.

## v0.1 — Single-chain MVP

Initial development baseline.

- Single-chain EVM indexing.
- Immutable monitor definitions with enable, disable, and delete operations.
- ABI calldata decoding.
- PostgreSQL result storage.
- HTTP API with OpenAPI / Swagger UI.
- Docker-based local development.

## v0.2 — Internal boundaries and adapter traits

Prepare the architecture before adding more moving parts, without slowing development with premature workspace crates.

Status: implemented in v0.2.0.

- Keep Parseon as one binary crate.
- Consolidate domain and indexing behavior under `core`, with cache, RPC, PostgreSQL, and HTTP implementations outside it.
- Introduce domain models for `Chain`, `Monitor`, `Target`, `Cursor`, and `DecodedCall`.
- Introduce a `Storage` trait.
- Introduce a `BlockSource` trait.
- Introduce a `BlockCache` trait.
- Keep PostgreSQL, JSON-RPC, and in-memory cache as first official implementations.
- Allow breaking internal and API schema changes while the project is in early development.
- Defer separate crates until boundaries prove stable.

## v0.3 — Finalized-only consistency and observability

Status: implemented in v0.3.0.

Use the block source's finalized head as a simple, explicit consistency boundary.

- Keep `finalized` as the only indexing mode.
- Probe the finalized head during startup and fail when the block source does not support it.
- Never schedule blocks above the finalized head.
- Expose the finalized head, worker state, latest successful poll, and latest error through `GET /status`.
- Treat all returned monitor results as finalized by contract, without per-row finality fields.
- Document that consistency depends on the configured block source faithfully implementing its finalized-head signal.

## v0.4 — EVM event indexing

Status: implemented in v0.4.0.

- Add finalized, log-native EVM event monitors alongside function-call monitors.
- Infer target kind from flat human-readable ABI signatures.
- Decode non-anonymous events with scalar parameters and preserve indexed metadata.
- Store decoded event parameters with only transaction hash, block number, and log index as result identity.
- Fetch event logs without unnecessary full-block or receipt requests.
- Keep call and event persistence with cursor progress atomic.

## v0.5 — Multi-chain indexing

Status: implemented in v0.5.0.

Run one Parseon instance across multiple EVM chains.

- Add a chain registry.
- Make monitors chain-aware.
- Make monitor targets chain-aware.
- Make result storage chain-aware.
- Run one worker per chain.
- Isolate chain state, errors, cache, and finality settings.
- Persist chain additions, updates, enablement changes, and deletions through the API for the next process startup.

## v0.6 — Parallel execution and performance

Status: implemented across v0.6.0 and v0.6.1.

Scale indexing without duplicating block source work.

- Add bounded concurrent block fetching.
- Add bounded concurrent receipt fetching.
- Reuse fetched blocks across monitors.
- Support RPC batching where available.
- Add optional `eth_getBlockReceipts` support where block sources expose it.
- Add backpressure for block source calls and database writes.
- Expose Prometheus-compatible metrics.

## v0.7 — Runtime monitor filters

Status: implemented in v0.7.0.

Let each monitor decide which successfully decoded results should be stored.

- Add a bounded, versioned JSON AST with boolean composition and typed scalar comparisons.
- Validate filters when monitors are created, persist canonical JSON, and compile it into typed Rust predicates before worker evaluation.
- Filter decoded parameters, block and transaction identity, call sender/receiver, and native event metadata without additional event RPC calls.
- Apply immutable filters consistently to successful calls and events before atomic result persistence.
- Add stateless filter validation and evaluation through `POST /filters/preview`.
- Benchmark compilation and call/event evaluation under bounded parallel execution.

Future filter-language work can add a textual expression frontend, arithmetic,
composite ABI values, table-state references, SQL compilation, and a typed map
stage while reusing the versioned source-to-typed-IR boundary introduced here.

## v0.8 — Optional adapters I

Expand the ecosystem around the core after internal traits are stable.

- Add Redis block cache adapter.
- Add eRPC block source adapter for multi-provider routing, failover, and RPC caching.
- Add MongoDB storage adapter for document-oriented decoded results.
- Add webhook sink adapter.
- Reevaluate whether any adapter should become a separate crate based on dependency weight and reuse.

## v0.9 — Optional adapters II

Continue expanding the adapter ecosystem.

- Add indexed-data adapters for services such as Etherscan, Blockscout, Alchemy, and Moralis, with room for providers such as QuickNode and GoldRush as demand proves useful.
- Use address- and contract-oriented APIs for faster historical backfills, fallback reads, ABI discovery, and metadata enrichment while keeping direct JSON-RPC as the canonical chain and finality source.
- Evaluate provider-specific ERC-20 and ERC-721 transfer APIs as optimized sources for transfer monitors; their indexed transfer results can avoid scanning unrelated blocks, transactions, receipts, and logs, reducing indexing latency and JSON-RPC traffic.
- Add PostgreSQL `NOTIFY` for committed monitor-result notifications.
- Add Kafka and ClickHouse sink adapters.
- Consider a file sink adapter.

## v0.10 — Rich API and management surface

Prepare Parseon for a future frontend.

- Add richer chain, monitor, worker, and result endpoints.
- Add pause, resume, and reindex operations.
- Add cursor-based pagination.
- Add advanced result filtering.
- Revisit call and event resource shapes.
- Add event-metadata searches and decoded-parameter filters.
- Revisit offset and cursor pagination design across both result kinds.
- Keep OpenAPI first-class.
- Consider GraphQL after the HTTP model stabilizes.

## v0.11 — Crate split evaluation

Status: implemented ahead of schedule.

- Extract `parseon-core`, `parseon-server`, `parseon-rpc`, `parseon-postgres`, and `parseon-memory-cache`.
- Keep domain and application behavior in core.
- Keep adapter dependency trees outside core.
- Preserve one production `parseon` binary.

## v0.12 — Provisional indexing and rollback engine

Add near-head indexing only together with the machinery required to make it correct.

- Add confirmation-depth and near-head provisional indexing.
- Store a canonical block metadata ledger with block number, hash, parent hash, timestamp, and finality status.
- Detect reorgs by comparing parent and canonical hashes.
- Atomically remove orphaned results, rewind all affected cursors including completed monitors, and replay the canonical branch.
- Promote provisional data to finalized as the block source's finalized head advances.
- Fail closed when no common ancestor exists within retained rollback history.
- Add API finality filtering and explicit `provisional`, `finalized`, and `reorged` states.

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

1. Optimize parallel indexing.
2. Add optional adapters only after the core traits are stable.
3. Build richer APIs for management and querying.
4. Evolve the filter language only after the JSON AST is proven in production.
5. Reevaluate adapter crates during v0.8 based on dependency weight and reuse.
6. Add provisional indexing and rollback only as a coherent v0.12 feature.
