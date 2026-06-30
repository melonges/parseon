# Parseon Terminology

This document defines the preferred vocabulary for Parseon code, API design, documentation, and future architecture work.

Naming should stay boring and precise. Clever names are cute until they become migration pain, API confusion, and a future archaeology project with `grep`.

## Preferred vocabulary

| Term | Meaning | Notes |
| --- | --- | --- |
| `Chain` | One EVM network indexed by Parseon. | Ethereum, Base, Arbitrum, Optimism, etc. |
| `Monitor` | User-defined indexing rule for a contract function. | Prefer this over `Watcher`. |
| `Target` | What a monitor matches. | Usually `chain_id + contract address + function selector`. |
| `Filter` | Optional condition applied after a call is matched and decoded. | Prefer this over `Predicate`, `Where`, or `Condition`. |
| `Cursor` | Per-monitor indexing progress. | Last indexed or finalized block for a monitor. |
| `BlockSource` | Adapter that fetches blocks, transactions, and receipts. | Prefer this over generic `Provider` in core abstractions. |
| `Storage` | Adapter that persists chains, monitors, progress, and decoded results. | PostgreSQL first, MongoDB later. |
| `Cache` | Adapter that stores fetched blocks or receipts temporarily. | Memory first, Redis later. |
| `Worker` | Runtime task that indexes one chain. | One worker per chain in the multi-chain architecture. |
| `Scheduler` | Component that decides which block ranges to fetch next. | Owns batching and concurrency decisions. |
| `DecodedCall` | A matched transaction call with decoded ABI parameters. | Core/domain term. |
| `ResultRecord` | Storage-level persisted decoded call. | Internal persistence term. |
| `MonitorResult` | API representation of a persisted decoded call. | User-facing API term. |
| `Adapter` | Compile-time integration around the core. | Prefer this over `Plugin` for now. |
| `Sink` | Optional output destination for decoded data. | Kafka, webhook, ClickHouse, files, etc. |
| `Reorg` | Chain rewrite that invalidates already indexed blocks. | Must be handled by core logic. |
| `Finality` | Guarantee level of indexed data. | Exposed through API/status metadata. |

## Monitor, not Watcher

Use `Monitor` for the user-defined rule:

```text
Monitor = Target + block range + optional Filter + Cursor
```

A monitor is configuration and state. It is not the running task itself.

Prefer:

```http
POST /monitors
GET /monitors/{id}
GET /monitors/{id}/results
POST /monitors/{id}/reindex
```

Avoid using `Watcher` for public API, database entities, or new core models. `Watcher` sounds like a running process and collides with `Worker`.

Existing code may still contain `watcher/` during the MVP phase. New architecture work should move toward `monitor/` or `parseon-core::monitor`.

## Target

A monitor target defines what onchain call should be matched.

Typical target fields:

```text
chain_id
address
selector
signature
```

Recommended Rust-style shape:

```rust
pub struct MonitorTarget {
    pub chain_id: ChainId,
    pub address: Address,
    pub selector: Selector,
    pub signature: FunctionSignature,
}
```

## Filter

A filter is an optional condition applied after matching and decoding.

Use `Filter` for user-facing and internal naming. Avoid `Predicate` unless referring to implementation internals of an expression evaluator.

Example JSON filter DSL:

```json
{
  "and": [
    { "field": "tx.status", "op": "eq", "value": 1 },
    { "field": "params.value", "op": "gt", "value": "1000000000000000000" }
  ]
}
```

Filters should be deterministic, safe, and testable. Start with a JSON DSL before considering WASM or script-based filters.

## BlockSource, not Provider

Use `BlockSource` for core abstractions that read chain data.

Implementations may include:

```text
JsonRpcSource
ErpcSource
EtherscanSource
FallbackSource
```

Reasoning:

- `Provider` already has specific meaning in EVM/RPC libraries.
- `Source` works for JSON-RPC, eRPC, Etherscan, archive nodes, and fallback chains.
- Etherscan is not equivalent to JSON-RPC and should usually be used for backfill, fallback, ABI discovery, or metadata enrichment, not primary live indexing.

## Storage and Sink

Use `Storage` for primary state and queryable decoded results.

Examples:

```text
PostgresStorage
MongoStorage
```

Use `Sink` for optional output destinations that receive decoded data but do not own Parseon state.

Examples:

```text
KafkaSink
WebhookSink
ClickHouseSink
FileSink
```

Do not call Redis storage in core terminology. Redis is a cache unless it becomes an explicitly supported source of truth, which it should not by default.

## Cache

Use `Cache` for temporary fetched data.

Examples:

```text
MemoryBlockCache
RedisBlockCache
NoopBlockCache
```

Cache keys must be chain-aware:

```text
chain_id + block_number
chain_id + block_hash
chain_id + tx_hash
```

## DecodedCall, ResultRecord, MonitorResult

Use different names for different layers:

| Layer | Term |
| --- | --- |
| Core | `DecodedCall` |
| Storage | `ResultRecord` |
| API | `MonitorResult` |

Avoid calling decoded calls `Event`. EVM already has logs/events, and mixing calldata decoding with events will produce confusion with impressive efficiency.

## Finality status

Use `finality_status` for user-facing result/block status.

Preferred values for early versions:

```text
provisional
finalized
reorged
```

Internal lifecycle states may also include:

```text
indexed
failed
```

Default API result queries should prefer finalized data where the chain and source can provide that guarantee.

## Adapter, not Plugin

Use `Adapter` for now.

Parseon should start with compile-time adapters backed by stable Rust traits:

```text
Storage adapter
BlockSource adapter
Cache adapter
Sink adapter
```

Avoid runtime-loaded plugins in early versions. Dynamic loading in Rust adds ABI stability, versioning, async trait, panic-safety, and deployment complexity. That is a lot of machinery just to rename a dependency problem.

External plugin processes over gRPC or another protocol can be considered later if real user needs appear.

## Naming summary

Prefer:

```text
Monitor, not Watcher
Filter, not Predicate
Target, not Subscription
BlockSource, not Provider
Storage, not DB plugin
Cache, not Cache plugin
Worker, not Watcher
DecodedCall, not Event
MonitorResult, not Transaction
Cursor, not Offset
Adapter, not Plugin
Sink, not Export plugin
```

A good Parseon sentence should read like this:

> Parseon runs chain workers. Each worker reads blocks from a block source, matches transactions against monitor targets, decodes calldata into decoded calls, applies monitor filters, persists monitor results through storage, and uses cache adapters to reduce repeated block and receipt reads.
