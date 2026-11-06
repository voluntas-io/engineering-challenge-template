# Prediction-market order-book collector

A Rust collector for live prediction-market order books. It discovers markets,
maintains one normalized book per outcome symbol, writes the supplied FIX SBE
schema, and can record the exact public websocket frames. Kalshi and Polymarket
adapters share the same core.

This is a market-data collector, not a trading system. It does not submit
orders, hold positions, or run strategy or pre-trade risk. Kalshi live
validation is blocked without credentials; Polymarket full-universe discovery is
verified, but simultaneous subscription at that fan-out is not claimed.

## Review path

1. [Architecture](docs/architecture.md) — current topology and failure model.
2. [Requirement-to-test matrix](docs/requirement-test-matrix.md) — exact test evidence per requirement.
3. [Verification](docs/verification.md) — gate results, exact commits, limits.
4. [Design decisions](docs/design-decisions.md) — why the boundaries exist.

Implementation contracts: [replay format](docs/replay-format.md),
[raw-frame journal](docs/raw-frame-journal.md), [latency](docs/latency.md),
and [operating envelope](docs/operating-envelope.md).

## What it does

- Discovers live markets continuously and adds or removes symbols without a
  restart.
- Keeps a book per symbol, applies snapshots and deltas, and rejects invalid
  states.
- Writes a full snapshot every 60 seconds and on failover, recovery, removal,
  and shutdown; writes deltas as they arrive.
- Assigns symbols to fixed shards and opens `replicas` sockets per shard. A
  backup is promoted only when its book is valid and fresh, and promotion is
  anchored by a full snapshot.
- Handles SIGINT and SIGTERM: stops discovery, emits final snapshots, closes
  websockets, flushes and syncs output, and writes sidecars.
- Exposes Prometheus text metrics, `/healthz`, and `/readyz` when configured.
- Provides deterministic replay, offline SBE inspect and verify, and an optional
  checksummed raw-frame journal with `journal verify` and `journal recover`.

## Quick start (Polymarket, no credentials)

```bash
cargo run --release -- --exchange polymarket --max-markets 20 \
  --output captures/polymarket.sbe \
  --metadata captures/polymarket-markets.json \
  --report captures/polymarket-report.json \
  collect
```

Add `--journal-dir captures/journal` to record raw frames, then verify:

```bash
cargo run --release -- journal verify captures/journal/<run-id>
```

Other commands: `replay`, `inspect`, `verify`, `latency`, `discover`, `journal`.
See `config.example.toml` and `config.polymarket.example.toml`.

## Build and test

Rust is pinned in `rust-toolchain.toml` (1.99.1).

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
cargo test --doc --locked
cargo build --release --locked
python3 scripts/check_architecture.py --check
python3 scripts/check_matrix_tests.py
python3 -m unittest discover -s scripts -p 'test_*.py'
```

The public Polymarket network test is opt-in:

```bash
RUN_LIVE_TESTS=1 cargo test --test live_polymarket -- --ignored --nocapture
```

## Scope and provenance

`sbe_schemas/` is the supplied challenge schema. `src/util/build.rs` generates
the SBE bridge from it. The aligned reader and writer helpers are
challenge-compatible implementations; changes require template-reader
compatibility tests. The rest of `src/` is the project implementation.
Dependency licenses and the pinned graph are in `Cargo.lock`.

Measured limits, including the 100,000-symbol scale run, are in
[`docs/operating-envelope.md`](docs/operating-envelope.md). This is **not a production certification** for order entry or execution.
