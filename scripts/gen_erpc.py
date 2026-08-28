#!/usr/bin/env python3
"""Generate erpc.yaml from chainlist.org/rpcs.json.

Filter:
  - mainnet only (isTestnet/testnet false)
  - has networkId (EVM signal)
  - has >=1 HTTP/HTTPS RPC URL
  - top N chains by TVL (default 15)
  - (default) each URL is probed with eth_getBlockByNumber(["latest", false])
    and ranked by chainlist.org's algorithm: higher block height first, ties
    broken by lower latency. URLs that fail or return no block are dropped.
    Pass --filter-stale to also drop "red" endpoints (>3 blocks behind the
    leader or >5s slower than the leader for that chain).

Each chain becomes a `networks[]` entry; each surviving HTTP/HTTPS RPC URL
becomes an `upstreams[]` entry, ordered best-first. Shared tuning lives on
`upstreamDefaults`/`networkDefaults` to keep the YAML compact.

The probe mirrors DefiLlama/chainlist hooks/useRPCData.js +
components/RPCList/index.js.
"""
from __future__ import annotations

import argparse
import ipaddress
import json
import re
import socket
import ssl
import sys
import time
import urllib.error
import urllib.request
from urllib.parse import parse_qsl, urlsplit
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

SRC_DEFAULT = Path("rpcs.json")
DST_DEFAULT = Path("erpc.yaml")
CREDENTIAL_QUERY_KEYS = {"api_key", "apikey", "access_token", "token", "secret", "password"}
ALIAS_RE = re.compile(r"[^a-zA-Z0-9_-]+")
DEFAULT_HEADERS = {"content-type": "application/json", "user-agent": "parseon-gen-erpc/1.0"}
# The probe also checks eth_chainId so an endpoint cannot be assigned to the wrong network.
CHAIN_ID_BODY = json.dumps(
    {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1}
).encode()
RPC_BODY = json.dumps(
    {"jsonrpc": "2.0", "method": "eth_getBlockByNumber", "params": ["latest", False], "id": 1}
).encode()
# One shared SSL context so we don't re-init per request.
_SSL_CTX = ssl.create_default_context()


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


_OPENER = urllib.request.build_opener(
    _NoRedirect(), urllib.request.HTTPSHandler(context=_SSL_CTX)
)


def endpoint_safety(url: str) -> str | None:
    """Return a rejection reason for URLs that do not resolve publicly."""
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"}:
        return "scheme"
    if not parsed.hostname:
        return "host"
    try:
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        addresses = socket.getaddrinfo(parsed.hostname, port, type=socket.SOCK_STREAM)
    except (OSError, ValueError):
        return "dns"
    for address in addresses:
        ip = ipaddress.ip_address(address[4][0])
        if ip.version == 6 and ip.ipv4_mapped is not None:
            ip = ip.ipv4_mapped
        if not ip.is_global:
            return "private-address"
    return None


def http_rpcs(entry: dict) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for r in entry.get("rpc") or []:
        if not isinstance(r, dict):
            continue
        url = (r.get("url") or "").strip().replace("\u200b", "")
        if not (url.startswith("http://") or url.startswith("https://")):
            continue
        # Never copy credentials from chainlist or a local source into generated config.
        # Operators must inject private upstream URLs through their deployment secret.
        parsed = urlsplit(url)
        query_keys = {key.lower() for key, _ in parse_qsl(parsed.query, keep_blank_values=True)}
        if parsed.username or parsed.password or query_keys & CREDENTIAL_QUERY_KEYS:
            continue
        if "API_KEY" in url or "${" in url:
            continue
        if endpoint_safety(url) is not None:
            continue
        if url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def slug(s: str) -> str:
    s = ALIAS_RE.sub("-", s or "").strip("-")
    return s.lower() or "chain"


def yaml_quote(s: str) -> str:
    # Use single quotes; double any embedded single quote.
    return "'" + s.replace("'", "''") + "'"


def _post(url: str, body: bytes, timeout: float) -> tuple[bytes | None, int | None, str | None]:
    req = urllib.request.Request(url, data=body, headers=DEFAULT_HEADERS, method="POST")
    t0 = time.monotonic()
    try:
        with _OPENER.open(req, timeout=timeout) as resp:
            if resp.status != 200:
                return None, None, f"http {resp.status}"
            return resp.read(65536), int((time.monotonic() - t0) * 1000), None
    except urllib.error.HTTPError as e:
        return None, None, f"http {e.code}"
    except (urllib.error.URLError, socket.timeout, TimeoutError, OSError, ssl.SSLError) as e:
        return None, None, f"transport:{type(e).__name__}"
    except Exception as e:
        return None, None, f"other:{type(e).__name__}"


def probe(url: str, chain_id: int, timeout: float) -> tuple[int | None, int | None, str]:
    """Check endpoint chain identity and latest block height."""
    safety = endpoint_safety(url)
    if safety is not None:
        return None, None, f"unsafe:{safety}"
    raw, _, failure = _post(url, CHAIN_ID_BODY, timeout)
    if failure is not None or raw is None:
        return None, None, failure or "empty-response"
    try:
        chain_doc = json.loads(raw)
        actual_chain_id = int(chain_doc["result"], 16)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None, None, "bad-chain-id"
    if actual_chain_id != chain_id:
        return None, None, f"wrong-chain:{actual_chain_id}"

    raw, latency_ms, failure = _post(url, RPC_BODY, timeout)
    if failure is not None or raw is None:
        return None, None, failure or "empty-response"
    try:
        doc = json.loads(raw)
    except json.JSONDecodeError:
        return None, None, "non-json"
    if not isinstance(doc, dict) or "error" in doc:
        err = doc.get("error") if isinstance(doc, dict) else None
        msg = err.get("message", "") if isinstance(err, dict) else ""
        return None, None, f"rpc-error:{msg[:40]}"
    result = doc.get("result")
    if not isinstance(result, dict):
        return None, None, "no-result"
    number = result.get("number")
    if not isinstance(number, str):
        return None, None, "no-number"
    try:
        height = int(number, 16)
    except ValueError:
        return None, None, f"bad-number:{number[:16]}"
    return height, latency_ms, "ok"


def probe_all(
    chains: list[dict], timeout: float, workers: int
) -> dict[int, list[tuple[str, int | None, int | None, str]]]:
    """Probe every (chainId, url) pair concurrently.

    Returns mapping: chainId -> list of (url, height, latency_ms, reason).
    """
    tasks: list[tuple[int, str]] = []
    for c in chains:
        for url in c["rpcs"]:
            tasks.append((c["chainId"], url))

    results: dict[int, list[tuple[str, int | None, int | None, str]]] = {
        c["chainId"]: [] for c in chains
    }
    with ThreadPoolExecutor(max_workers=workers) as pool:
        fut_to_task = {pool.submit(probe, url, cid, timeout): (cid, url) for cid, url in tasks}
        for fut in as_completed(fut_to_task):
            cid, url = fut_to_task[fut]
            try:
                height, latency, reason = fut.result()
            except Exception as e:
                height, latency, reason = None, None, f"exc:{type(e).__name__}"
            results[cid].append((url, height, latency, reason))
    return results


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", type=Path, default=SRC_DEFAULT)
    ap.add_argument("--dst", type=Path, default=DST_DEFAULT)
    ap.add_argument("--top", type=int, default=15, help="number of chains by TVL (0 = all)")
    ap.add_argument(
        "--probe/--no-probe",
        dest="probe",
        default=True,
        help="POST eth_getBlockByNumber to each URL, rank by chainlist.org's "
        "height-then-latency algorithm, and drop failures (default: on)",
    )
    ap.add_argument(
        "--probe-timeout", type=float, default=5.0, help="per-endpoint probe timeout in seconds"
    )
    ap.add_argument(
        "--probe-workers", type=int, default=32, help="concurrent probe requests"
    )
    ap.add_argument(
        "--filter-stale",
        action="store_true",
        help="also drop endpoints >3 blocks behind the chain leader or >5s slower "
        "(chainlist.org's 'red' trust tier)",
    )
    args = ap.parse_args()

    data = json.loads(args.src.read_text())
    mainnets = [e for e in data if not (e.get("isTestnet") or e.get("testnet"))]

    chains: list[dict] = []
    for e in mainnets:
        if e.get("networkId") is None:
            continue
        cid = e.get("chainId")
        if not isinstance(cid, int) or cid <= 0:
            continue
        rpcs = http_rpcs(e)
        if not rpcs:
            continue
        chains.append({
            "chainId": cid,
            "name": e.get("name") or f"chain-{cid}",
            "shortName": e.get("shortName") or "",
            "tvl": e.get("tvl") or 0,
            "rpcs": rpcs,
        })

    # Deduplicate by chainId (keep first occurrence with most RPCs)
    by_cid: dict[int, dict] = {}
    for c in chains:
        cur = by_cid.get(c["chainId"])
        if cur is None or len(c["rpcs"]) > len(cur["rpcs"]):
            by_cid[c["chainId"]] = c
    chains = list(by_cid.values())

    # Rank by TVL descending; ties broken by RPC count (more = more popular).
    chains.sort(key=lambda c: (c["tvl"], len(c["rpcs"])), reverse=True)
    if args.top > 0:
        chains = chains[: args.top]
    chains.sort(key=lambda c: c["chainId"])

    used_alias: set[str] = set()
    for c in chains:
        base = slug(c["shortName"]) or slug(c["name"])
        if not base or base in used_alias:
            base = f"chain{c['chainId']}"
        if base in used_alias:
            base = f"chain{c['chainId']}-{c['shortName']}"
        cand = base
        i = 2
        while cand in used_alias:
            cand = f"{base}-{i}"
            i += 1
        used_alias.add(cand)
        c["alias"] = cand

    if args.probe:
        total = sum(len(c["rpcs"]) for c in chains)
        print(
            f"Probing {total} endpoints across {len(chains)} chains "
            f"(timeout={args.probe_timeout}s, workers={args.probe_workers})...",
            file=sys.stderr,
        )
        results = probe_all(chains, args.probe_timeout, args.probe_workers)
        kept_total = 0
        dropped_total = 0
        for c in chains:
            # Build (url, height, latency) list for this chain.
            probed: list[tuple[str, int, int]] = []
            for url, height, latency, reason in results[c["chainId"]]:
                if height is None or latency is None:
                    dropped_total += 1
                    continue
                probed.append((url, height, latency))

            if not probed:
                c["rpcs"] = []
                continue

            # Chainlist.org sort: higher height first, ties broken by lower latency.
            probed.sort(key=lambda t: (-t[1], t[2]))

            if args.filter_stale and probed:
                best_height = probed[0][1]
                best_latency = probed[0][2]
                filtered: list[tuple[str, int, int]] = []
                for url, height, latency in probed:
                    # "red" = >3 blocks behind leader OR >5000ms slower than leader.
                    if best_height - height > 3 or latency - best_latency > 5000:
                        dropped_total += 1
                        continue
                    filtered.append((url, height, latency))
                probed = filtered

            kept_total += len(probed)
            c["rpcs"] = [url for url, _, _ in probed]

        # Drop chains with no surviving RPCs.
        before = len(chains)
        chains = [c for c in chains if c["rpcs"]]
        dropped_chains = before - len(chains)
        chains.sort(key=lambda c: c["chainId"])
        print(
            f"Probe kept {kept_total}/{kept_total + dropped_total} endpoints; "
            f"dropped {dropped_chains} chain(s) with no survivors.",
            file=sys.stderr,
        )

    print(f"Generating {args.dst} with {len(chains)} chains, "
          f"{sum(len(c['rpcs']) for c in chains)} upstreams", file=sys.stderr)

    lines: list[str] = []
    lines.append("# Generated from https://chainlist.org/rpcs.json.")
    lines.append("# Top N mainnet EVM chains by TVL with at least one HTTP/HTTPS RPC.")
    lines.append("# Endpoints probed with eth_getBlockByNumber and ranked by chainlist.org's")
    lines.append("# height-then-latency algorithm; failures dropped.")
    lines.append("# Credential-bearing URLs are intentionally excluded; inject private endpoints at deploy time.")
    lines.append(f"# Regenerate with: python3 scripts/gen_erpc.py --top {args.top} [--no-probe] [--filter-stale] [--src rpcs.json] [--dst erpc.yaml]")
    lines.append("")
    lines.append("logLevel: warn")
    lines.append("")
    lines.append("metrics:")
    lines.append("  enabled: true")
    lines.append("  port: 4001")
    lines.append("")
    lines.append("database:")
    lines.append("  evmJsonRpcCache:")
    lines.append("    connectors:")
    lines.append("      - id: memory")
    lines.append("        driver: memory")
    lines.append("    policies:")
    lines.append("      - connector: memory")
    lines.append("        finality: finalized")
    lines.append("")
    lines.append("projects:")
    lines.append("  - id: main")
    lines.append("    upstreamDefaults:")
    lines.append("      autoIgnoreUnsupportedMethods: true")
    lines.append("      evm:")
    lines.append("        # State-poller cadence for public RPCs; raise if you add many chains.")
    lines.append("        statePollerInterval: 10m")
    lines.append("      failsafe:")
    lines.append("        - matchMethod: \"*\"")
    lines.append("          timeout:")
    lines.append("            duration: 30s")
    lines.append("            quantile: 0.9")
    lines.append("            minDuration: 1s")
    lines.append("            maxDuration: 30s")
    lines.append("          retry:")
    lines.append("            maxAttempts: 3")
    lines.append("            delay: 200ms")
    lines.append("    networkDefaults:")
    lines.append("      multiplexing: true")
    lines.append("      evm:")
    lines.append("        getLogsSplitOnError: true")
    lines.append("        getLogsMaxAllowedRange: 10000")
    lines.append("    networks:")
    for c in chains:
        lines.append(f"      - architecture: evm")
        lines.append(f"        evm:")
        lines.append(f"          chainId: {c['chainId']}")
        lines.append(f"        alias: {c['alias']}")
    lines.append("    upstreams:")
    for c in chains:
        for idx, url in enumerate(c["rpcs"]):
            lines.append(f"      - id: chain{c['chainId']}-{idx}")
            lines.append(f"        endpoint: {yaml_quote(url)}")
            lines.append(f"        evm:")
            lines.append(f"          chainId: {c['chainId']}")

    args.dst.write_text("\n".join(lines) + "\n")
    print(f"Wrote {args.dst} ({len(lines)} lines)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
