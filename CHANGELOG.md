# Changelog

All notable changes to Parseon are documented in this file.

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
