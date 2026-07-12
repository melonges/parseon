# Changelog

All notable changes to Parseon are documented in this file.

## Unreleased

## 0.7.0 - 2026-07-12

### Added

- Immutable, ABI-aware JSON monitor filters with typed comparisons, boolean
  composition, canonical persistence, and call/event worker evaluation.
- Stateless filter validation and evaluation at `POST /filters/preview`.
- Criterion coverage for filter compilation and bounded call/event evaluation.

### Changed

- Keep successful transactions as the indexing boundary and expose only
  already-available transaction or log metadata to filters.
- Allow monitor creation to omit `start_block` and `end_block`, starting at the chain's current finalized head and continuing live as new finalized blocks arrive.
- Keep human-readable ABI signatures only during monitor creation; persist and
  return the fixed-size function selector or event topic instead.
- Match calls and events through a shared per-poll target index instead of
  scanning every monitor for each transaction and log.

## 0.6.4 - 2026-07-08

### Changed

- Carry chain IDs, block numbers, monitor IDs, addresses, selectors, topics, and transaction hashes as validated unsigned or fixed-size domain types.
- Store monitor addresses, selectors, topics, result transaction hashes, and decoded ABI addresses as binary PostgreSQL values.
- Validate listen addresses, poll intervals, batch sizes, cache capacity, and concurrency limits during configuration parsing.

### Breaking

- Reject negative numeric API values, zero monitor IDs, malformed addresses, and invalid fixed-size hashes during request decoding.
- Reset monitors and dynamic result tables when applying the typed-value migration; registered chains are retained.

## 0.6.3 - 2026-07-07

### Changed

- Replace the mutex-protected LRU block cache with Moka's concurrent cache using its default admission policy and predicate-based cursor eviction.

## 0.6.2 - 2026-07-07

### Added

- Dedicated `parseon-core`, `parseon-rpc`, `parseon-postgres`, `parseon-memory-cache`, and `parseon-server` workspace crates.
- Core application services, commands, views, and repository ports for infrastructure-independent orchestration.

### Changed

- Route HTTP handlers through core application services instead of accessing PostgreSQL repositories directly.
- Keep RPC, PostgreSQL, memory-cache, configuration, HTTP, and telemetry concerns behind explicit adapter boundaries.
- Preserve the existing `parseon` production binary and HTTP API while documenting the workspace architecture.

## 0.6.1 - 2026-07-06

### Added

- Automatic receipt fetching through direct requests, bounded JSON-RPC batches, or `eth_getBlockReceipts`.
- Prometheus-compatible runtime metrics at `GET /metrics` for RPC work, indexing lag, cache access, decoded results, and database commits.
- Per-chain RPC concurrency and receipt batch-size configuration.

### Changed

- Fall back to targeted receipt requests when an endpoint rejects batching or block receipts.
- Learn optional RPC capabilities independently for each registered endpoint until its worker is replaced.

## 0.6.0 - 2026-07-06

### Added

- Bounded concurrent block preparation with configurable per-chain concurrency.
- Process-wide database write backpressure shared by all chain workers.
- A repeatable worker-pipeline benchmark comparing serial and bounded execution.

### Changed

- Prepare blocks concurrently while preserving ascending, atomic block commits so cursors never advance beyond failed work.
- Fetch call data and event logs concurrently for blocks covered by both monitor kinds.

## 0.5.0 - 2026-07-06

### Added

- PostgreSQL-backed chain registry with create, read, update, and delete HTTP APIs.
- A runtime supervisor that reconciles registry changes and runs one isolated finalized-only worker per enabled chain.
- Aggregate per-chain status with starting, running, degraded, and disabled worker states.
- Chain-scoped monitor creation and optional `chain_id` filtering on monitor lists.

### Changed

- Start the HTTP server with an empty chain registry and discover each chain ID from its registered RPC endpoint.
- Scope monitor loading, cursor commits, block caches, source failures, and worker cancellation by chain.
- Keep registered RPC URLs write-only in HTTP responses and runtime diagnostics.
- Destructively reset v0.4 monitors and dynamic result tables for the chain-aware schema.

### Breaking

- Remove the `RPC_URL` CLI/environment setting; chains must be registered through `POST /chains`.
- Require `chain_id` in monitor creation requests and include it in monitor responses.
- Change `GET /status` from one chain object to `{ "mode": "finalized", "chains": [...] }`.

## 0.4.0 - 2026-07-05

### Added

- Finalized, log-native EVM event monitors inferred from `event` ABI signatures.
- Indexed-parameter metadata and decoding, including topic hashes for indexed dynamic values.
- Tagged call and event result responses with minimal transaction/log identity and decoded parameters.

### Changed

- Store results in ID-based `monitor_<id>_results` tables and identify monitors by kind, address, and signature hash.
- Keep result tables focused on decoded ABI values plus transaction hash, block number, and event log index.
- Fetch matching event logs once per block without fetching full blocks or receipts for event-only work.
- Destructively reset v0.3 monitor data and result tables for the new event-aware schema.

### Breaking

- Monitor responses now expose `kind` and exactly one of `selector` or `topic0`; result responses are tagged by kind.
- Result queries no longer expose sender or transaction-status filters.

## 0.3.0 - 2026-07-01

### Added

- Startup validation that requires the configured RPC endpoint to return a finalized head.
- `GET /status` with finalized indexing progress, worker state, latest successful poll time, and latest error.
- Runtime status coverage in router and OpenAPI tests.

### Changed

- Poll the finalized head even when no monitors are active so runtime status remains current.
- Remove chain-ID configuration and discover the single instance's chain ID from its RPC endpoint.
- Document finalized-only consistency as the v0.3 contract and defer provisional indexing and rollback to v0.10.

## 0.2.0 - 2026-06-30

### Added

- Storage-neutral domain models for chains, monitor targets, cursors, decoded calls, and decoded Solidity values.
- `Storage`, `BlockSource`, and chain-aware `BlockCache` adapter traits.
- Pure block scheduling and a service-free worker test seam.
- PostgreSQL, JSON-RPC, and in-memory implementations of the v0.2 adapter contracts.

### Changed

- Replaced the legacy watcher/coordinator structure with monitor, scheduler, indexer, and worker modules.
- Moved SQL value conversion and atomic block persistence behind `PostgresStorage`.
- Separated API response types from PostgreSQL records while preserving the existing HTTP and OpenAPI surface.
- Consolidated ABI parsing, monitor logic, scheduling, indexing, worker orchestration, and adapter ports under `core`.
- Replaced SQL-aware ABI parameter metadata with typed Solidity parameters; PostgreSQL now derives physical column names and types locally.

### Breaking

- Monitor `param_schema` values now contain only `name` and canonical `sol_type`; `sql_kind` and physical `column` fields were removed.
- Existing development monitor data must be reset before running this version.
