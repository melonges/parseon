# Changelog

All notable changes to Parseon are documented in this file.

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
