# Kalshi Orderbook Collector

A production-quality Rust service that connects to [Kalshi](https://kalshi.com),
subscribes to all live prediction markets, and maintains real-time L2 orderbooks.
Output is emitted as SBE-encoded binary messages (snapshots + deltas) to stdout.

## Features

| Feature | Description |
|---------|-------------|
| **Base challenge** | Live orderbook collection for all Kalshi markets |
| **Extension 1** | Redundant WebSockets per bucket; automatic failover with snapshot re-emission |
| **Extension 2** | Per-symbol idle detection + resubscription; periodic full socket reconnect |
| **Extension 3** | Generic `ExchangeAdapter` trait; Kalshi is a pluggable adapter |
| **Extension 4** | Latency research documented in [LATENCY.md](LATENCY.md) |

## Architecture

```
main.rs
  └── Collector
        ├── auth.rs           — Ed25519 request signing
        ├── kalshi.rs         — Kalshi REST + WebSocket adapter
        │     ├── fetch_open_markets()   — paginated REST polling
        │     ├── WsCommand / WsEnvelope — WebSocket message types
        │     └── decode_snapshot/delta  — JSON → canonical types
        ├── orderbook.rs      — In-memory BTreeMap L2 orderbook
        ├── collector.rs      — Bucket/socket management, event loop
        │     ├── BucketState            — group of markets + sockets
        │     ├── SocketHandle           — command channel to ws_task
        │     └── ws_task()              — single WS connection task
        └── output.rs         — AlignedSbeWriter wrapper for stdout
```

### Key design decisions

**Load balancing**: Markets are split into buckets of ≤ 200 symbols each.
Each bucket runs `SOCKETS_PER_BUCKET = 2` WebSocket connections (primary +
shadow). The primary is the source of truth; the shadow provides failover.

**Kalshi binary market model**: Kalshi only sends bids. We reconstruct a
conventional two-sided book:
- YES bids → **Buy (bid)** side at price `P`
- NO bids  → **Sell (ask)** side at complemented price `1 - P`

**Fixed-point arithmetic**: All prices and quantities are stored as `i64` with
`1_000_000 = $1.00` or `1 share`. This avoids floating-point rounding and
matches the SBE schema.

**Failover** (Extension 1): On primary socket disconnect, the collector:
1. Promotes the shadow socket to primary.
2. Emits a snapshot from the current in-memory book state (for output continuity).
3. Requests a fresh `get_snapshot` on the new primary to resynchronise.
4. Respawns a new shadow socket.

**Subscription monitoring** (Extension 2):
- Every 30 s, check each market's last-update timestamp.
- If a market hasn't received any update in 90 s, it is unsubscribed and
  resubscribed (handles silent exchange-side drops).
- Every 10 minutes, each socket is fully replaced with a fresh connection
  (handles cumulative server-side memory leaks / drift).

## Setup

### Prerequisites

- Rust stable (edition 2024, ≥ 1.85)
- A Kalshi account with an **Ed25519** API key

### Generate a Kalshi API key

1. Go to [kalshi.com/account/profile](https://kalshi.com/account/profile)
2. Under "API Keys", click **Create New API Key**
3. Select **Ed25519** (recommended)
4. Save the **Key ID** and download the **private key PEM**

### Configure environment variables

```bash
export KALSHI_API_KEY_ID="your-key-uuid-here"
export KALSHI_PRIVATE_KEY="/path/to/kalshi.key"
# OR inline:
# export KALSHI_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
```

### Build & run

```bash
# Optimized release build
cargo build --release

# Live exchange mode (requires Kalshi API credentials):
export KALSHI_API_KEY_ID="your-key-uuid-here"
export KALSHI_PRIVATE_KEY="/path/to/kalshi.key"
cargo run --release 2>collector.log > orderbook.sbe

# Or test run with synthetic generator (no credentials needed):
MOCK=1 cargo run --release 2>collector.log > orderbook.sbe
# or: cargo run --release -- --mock
```

Logs go to stderr; SBE binary data goes to stdout (can be piped or redirected).

Press `Ctrl-C` (SIGINT) for a clean shutdown (closes all WebSockets cleanly and flushes the SBE writer).

### Run tests

```bash
cargo test
```

The test suite covers:
- **End-to-End Pipeline** (`tests/collector_e2e_test.rs`): Runs full `Collector` with streaming synthetic orders, graceful SIGINT shutdown, and verifies SBE decode with `AlignedSbeReader`.
- **SBE Encoding Round-trip** (`tests/integration_tests.rs`): Validates schema encoding/decoding for snapshots and single/batch deltas.
- **Orderbook State Machine** (`tests/orderbook_tests.rs`): Ingestion, delta application, monotonic clock validation, level removals, edge cases.
- **Protocol & Fixed-point Parsing** (`tests/kalshi_parsing_tests.rs`): Fixed-point conversion, cent/sub-cent precision, NO-bid complementation math.

## Output format

The output file contains SBE-encoded messages in the `challenge_orderbook` schema:

| Message | Trigger | Fields |
|---------|---------|--------|
| `Snapshot` | On initial subscribe + every 60 s | `symbol`, `received_at`, `exchange_timestamp`, `bids[]`, `asks[]` |
| `PriceLevelUpdate` | On every delta from exchange | `symbol`, `backtest_timestamp`, `exchange_timestamp`, `price_level`, `quantity`, `side` |

Use `AlignedSbeReader<OrderbookSchema>` from the template library to decode.

## Configuration

Key tuning constants are in [`src/collector.rs`](src/collector.rs):

| Constant | Default | Description |
|----------|---------|-------------|
| `MAX_MARKETS_PER_SOCKET` | 200 | Markets per WS connection |
| `SOCKETS_PER_BUCKET` | 2 | Redundancy factor |
| `SNAPSHOT_INTERVAL` | 60 s | Full snapshot emission frequency |
| `DISCOVERY_INTERVAL` | 30 s | REST poll for new/closed markets |
| `SYMBOL_IDLE_THRESHOLD` | 90 s | Idle timeout before resubscription |
| `SOCKET_RECONNECT_INTERVAL` | 600 s | Full socket recycle period |
