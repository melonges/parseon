# Adapters

Parseon selects exactly one storage adapter at compile time. PostgreSQL is the default; the memory block cache is not feature-gated and can be disabled by setting `BLOCK_CACHE_SIZE=0`.

## Feature builds

```bash
# PostgreSQL
cargo build --release
cargo build --release --features webhook-sink

# MongoDB
cargo build --release --no-default-features --features mongodb-storage
cargo build --release --no-default-features --features mongodb-storage,webhook-sink
```

Enabling both `postgres-storage` and `mongodb-storage`, or enabling neither, is a compile error. All builds use `STORAGE_URL` and `STORAGE_WRITE_CONCURRENCY`. MongoDB builds also accept `STORAGE_DATABASE`, which defaults to `parseon`. Both drivers use their own connection-pool defaults.

The Dockerfile accepts the same comma-separated feature list through `PARSEON_FEATURES`:

```bash
docker build --build-arg PARSEON_FEATURES=mongodb-storage,webhook-sink -t parseon .
```

## MongoDB storage

MongoDB storage uses `chains`, `monitors`, `results`, and `counters` collections. Results share one document collection. Addresses and hashes are strings, byte values are BSON binary, 256-bit integers are decimal strings, and API reads retain Parseon's canonical JSON encoding.

Transactions require a replica set or sharded deployment. Parseon checks the deployment topology at startup and rejects standalone MongoDB before serving traffic. Monitor creation, atomic result/cursor commits, and chain/monitor cascade deletion use retryable transactions. Required unique identity and query-order indexes are created idempotently; there is no schema version or migration system. See the [MongoDB Rust transaction documentation](https://www.mongodb.com/docs/drivers/rust/current/crud/transactions/).

Start the persistent single-node development replica set and run the ignored integration test with:

```bash
docker compose --profile mongodb up -d
cargo test -p parseon-mongodb compose_crud -- --ignored --nocapture
```

Use `STORAGE_URL=mongodb://localhost:27017/?replicaSet=rs0`. There is intentionally no PostgreSQL importer, dual-write mode, or cross-storage migration path.

## JSON-RPC block source

The Alloy adapter requests full transaction objects and rejects a block response when its number differs from the request or the endpoint returns hashes instead of full transactions. Call monitors request receipts only for transactions that match an indexed address and selector. Receipt outcomes must remain in the requested hash order; Parseon validates that invariant before decoding.

For larger candidate sets, Parseon tries `eth_getBlockReceipts`, then JSON-RPC batches, then bounded individual receipt calls. An endpoint capability is cached as unsupported only when the RPC response identifies an incompatible method, parameters, or batch envelope. Authentication, transport, timeout, and rate-limit failures stop the poll without fanning out into more requests.

Event monitors use inclusive `eth_getLogs` ranges across each contiguous worker window. Address and topic pairs remain exact: Parseon groups compatible pairs into independent filters and executes those groups within `RPC_REQUEST_CONCURRENCY`. When a provider reports a range or result-size limit, the adapter bisects the range and retries each half. A limit on a single block is terminal because splitting cannot make that query smaller. Successful results are sorted by block and log index before core decoding.

`RPC_REQUEST_CONCURRENCY` applies to every physical request, including split log queries and receipt fallbacks. RPC transport details are omitted from runtime status and logs so registered write-only endpoint URLs and credentials are not exposed.

## eRPC gateway

eRPC is an external JSON-RPC gateway, not a Rust `BlockSource` adapter. Start the included development gateway with:

```bash
docker compose --profile erpc up -d
```

The development config pins the eRPC image to the `0.1.1` release, sets a 3 GiB container memory limit with `GOMEMLIMIT=2700MiB`, and registers the top 5 mainnet EVM chains by TVL from [chainlist.org](https://chainlist.org/rpcs.json) as explicit `networks`/`upstreams`, with an in-memory cache for finalized responses. The generator probes each candidate URL with `eth_getBlockByNumber(["latest", false])` and ranks endpoints by chainlist.org's algorithm — higher block height first, ties broken by lower latency — so dead, stale, and slow endpoints sink to the bottom and failures are dropped. Regenerate with `python3 scripts/gen_erpc.py --top 5` (pass `--filter-stale` to also drop endpoints >3 blocks behind or >5s slower than the chain leader, or `--top 0` to include every mainnet EVM chain). Register each complete [eRPC route](https://docs.erpc.cloud/operation/url), unchanged, through Parseon's chain API:

```bash
curl -X POST http://localhost:8080/chains \
  -H 'content-type: application/json' \
  -d '{"rpc_url":"http://localhost:4000/main/evm/8453","enabled":true}'
```

The bundled chainlist upstreams are public, rate-limited, and best-effort; the generator's probe filter weeds out dead, unauthorized, and chain ID-mismatched endpoints at config time, but transient 429s still appear in the bootstrap logs. eRPC retries in the background and routes around failed upstreams. Production deployments should replace them with multiple private providers/upstreams, credentials, limits, and failure policies in eRPC instead of relying on the [free public discovery catalog](https://docs.erpc.cloud/free).

Smoke-check chain ID, finalized head, block fetching, and JSON-RPC batching before registration:

```bash
curl http://localhost:4000/main/evm/8453 -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}'
curl http://localhost:4000/main/evm/8453 -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["finalized",false]}'
curl http://localhost:4000/main/evm/8453 -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["0x1",true]}'
curl http://localhost:4000/main/evm/8453 -H 'content-type: application/json' \
  -d '[{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]},{"jsonrpc":"2.0","id":2,"method":"eth_getBlockByNumber","params":["finalized",false]}]'
```

Metrics are exposed by eRPC on port `4001`. Its cache shape follows the [official cache configuration model](https://docs.erpc.cloud/config/example).

## Webhook sink

Build with `webhook-sink` and set `WEBHOOK_URL`. `WEBHOOK_CONCURRENCY` limits in-flight attempts and defaults to `16`.

Workers create a batch only when a committed block contains decoded results. Storage commits first; the adapter then starts one detached HTTP POST attempt. Any `2xx` response succeeds. There are no retries, redirects, authentication fields, delivery IDs, delivery timestamps, Prometheus metrics, or `/status` fields. Result array ordering is unspecified.

The adapter deliberately applies no request or connect timeout. When all permits are occupied, it immediately drops each new batch. A destination that accepts connections but never responds can therefore hold every permit indefinitely and keep the sink saturated until shutdown; this is intentional best-effort behavior. Shutdown cancels active attempts immediately. Logs report failure or saturation without the endpoint, response body, or secrets. Syntactically valid unsupported URL schemes are accepted at startup and reported only as send failures.

Payload contract:

```json
{
  "version": 1,
  "chain_id": 8453,
  "block_number": 123,
  "results": [
    {
      "kind": "call",
      "monitor_id": 7,
      "tx_hash": "0x…",
      "from": "0x…",
      "to": "0x…",
      "params": { "value": "42" }
    },
    {
      "kind": "event",
      "monitor_id": 8,
      "tx_hash": "0x…",
      "emitter": "0x…",
      "log_index": 3,
      "params": { "owner": "0x…" }
    }
  ]
}
```
