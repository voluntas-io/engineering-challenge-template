# Prediction-market order-book collector

A Rust collector for live prediction-market order books. It writes the supplied
FIX SBE schema, maintains a book per symbol, discovers markets continuously, and
load-balances symbols across redundant websocket sources.

Polymarket is the production-development focus. The current application is a
market-data collector, not a complete trading system: authenticated order entry,
account/position reconciliation, strategy evaluation, and independent pre-trade
risk controls are not yet implemented. The Kalshi adapter remains part of the
challenge baseline and regression surface.

See the [requirement-to-test matrix](docs/requirement-test-matrix.md) and
[final verification](docs/final-verification-2026-10-25.md) for the exact
offline gate results.

The [Event-Market Platform engineering charter](docs/design/event-market-platform-v1/README.md)
is the target architecture and engineering contract: canonical data model,
unified input log and deterministic replay, state machines, security and
operations, and proposed ADRs. The
[Polymarket production programme](docs/production-roadmap-polymarket.md) is the
active phased implementation plan and gate register for applying that charter.
Neither document is a claim that live order submission is enabled.

The [software-assurance baseline](docs/assurance/README.md) defines the controlled
engineering lifecycle, Rust correctness rules, financial-market controls,
terminology and verification policy that apply to all subsequent work. It is a
design baseline, not a certification or production-readiness claim.

## What it does

- Discovers live, non-MVE Kalshi markets and subscribes to every selected ticker.
- Reconciles discovery every 30 seconds by default, adding and removing symbols
  without restarting the process.
- Assigns symbols to a fixed number of shards at startup, then keeps the
  assignment stable. If `collector.shards = 0`, the initial shard count is
  derived from the symbol count and `max_symbols_per_socket`. The per-socket
  bound is enforced: a run that cannot keep every socket at or below it fails
  at startup instead of oversubscribing a connection.
- Opens `collector.replicas` websocket connections per shard. Every replica
  maintains a full shadow book; one source is active per symbol.
- Writes a full snapshot every 60 seconds and on failover, recovery, removal,
  and shutdown. Deltas are written as they arrive.
- Recovers from sequence gaps by reconnecting/resubscribing and requesting fresh
  snapshots. A backup source is promoted only when its book is valid and at
  least as new as the last emitted clock (when the exchange supplies one).
  Kalshi clock regressions are treated as maintenance epochs; a still-valid
  lower-clock shadow is reconnected instead of being promoted.
- Handles SIGINT and SIGTERM: stops discovery, emits final snapshots, closes
  websockets, flushes and syncs the SBE file, and writes the sidecars.
- Exposes dependency-free Prometheus text metrics and `/healthz` when
  configured; `/healthz` reports `starting`, `ok`, or `degraded` with
  connected and total source counts.
- Provides deterministic JSONL replay, an offline SBE inspector, and a `verify`
  command that proves a capture's deltas reconstruct the snapshots that follow
  them.
- Optionally records every received public data frame, including decode rejects,
  in a separate checksummed raw-frame journal with per-source epochs and receive
  times. The SBE schema and output contract are unchanged.

## Build and test

Rust is pinned in `rust-toolchain.toml` (1.99.1).

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
cargo test --doc --locked
cargo build --release --locked
```

The offline run on `polymarket-continuity-failure` at commit
`4362283d2fc2bb813c8feef391c3f882db0f8222` has **220 passing tests and 3
ignored opt-in tests**: 209 library tests, 9 mock-exchange end-to-end tests,
and 2 replay integration tests. The ignored tests are one parameterized scale
scenario (run separately at 1k/5k/25k/100k symbols) and two public-feed live
tests. Rust CI run
[40956405372](https://github.com/kj111422/engineering-challenge-template/actions/runs/40956405372)
records the same counts, and architecture validation run
[40956405414](https://github.com/kj111422/engineering-challenge-template/actions/runs/40956405414)
passed on that commit. The commands, counts, and limits are recorded in
[`docs/verification-2026-10-30-continuity.md`](docs/verification-2026-10-30-continuity.md).
The earlier [`docs/correctness-audit-2026-10-26.md`](docs/correctness-audit-2026-10-26.md)
and the submitted baseline's 127-test run in
[`docs/final-verification-2026-10-25.md`](docs/final-verification-2026-10-25.md)
remain the evidence for the commits they describe.

The offline suite covers decoding, fixed-point arithmetic, order books,
sharding/failover/snapshot scheduling, maintenance clock resets, malformed
discovery rejection, SBE sentinel validation, an end-to-end replay fixture, a
loopback mock exchange that drives the real coordinator, adapters, sessions,
writer, and SBE reader, and a scale harness that exercises 1,000–100,000
symbols. The public Polymarket network test is opt-in:

```bash
RUN_LIVE_TESTS=1 cargo test --test live_polymarket -- --ignored --nocapture
```

The current `main` commit (`c0f24b7`) has passing GitHub Actions runs for
[Rust CI](https://github.com/kj111422/engineering-challenge-template/actions/runs/42040684945)
and [architecture validation](https://github.com/kj111422/engineering-challenge-template/actions/runs/42040684847).
The separate live public-feed smoke test last passed in
[run 40168059177](https://github.com/kj111422/engineering-challenge-template/actions/runs/40168059177)
on an earlier source commit; treat that as live-feed evidence for that commit,
not as a live test of current `main`.

## Quick start: Kalshi

Kalshi websocket order-book subscriptions require an API key and a private key.
The collector supports Ed25519 PKCS#8, RSA PKCS#8, and RSA PKCS#1 PEM keys.
Credentials are read from the TOML file, environment, or CLI; no secrets are
written to reports or logs. The built-in environment default is `production`
because the collector targets live markets; use `--kalshi-environment demo`
for the demo exchange and demo credentials. Startup logs state the exchange,
environment, output, metadata, and report destinations.

```bash
export KALSHI_API_KEY_ID="your-key-id"
export KALSHI_PRIVATE_KEY_PATH="/absolute/path/to/kalshi-private-key.pem"

cargo run --release -- \
  --exchange kalshi \
  --kalshi-environment production \
  --output captures/kalshi.sbe \
  --metadata captures/kalshi-markets.json \
  --report captures/kalshi-report.json \
  collect
```

`KALSHI_PRIVATE_KEY_PEM` can be used instead of a path when a secret manager
injects the PEM directly. For the simulated exchange, add
`--kalshi-environment demo` and use demo credentials.

Discovery-only access does not need credentials:

```bash
cargo run --release -- --exchange kalshi --max-markets 10 discover
```

### Kalshi mapping

- Subscribes to `orderbook_delta`; `orderbook_snapshot` establishes the book
  and `orderbook_delta` applies changes.
- `market_lifecycle_v2` is subscribed once per connection for created,
  activated, deactivated, close-date, determined, settled, and metadata events.
- YES bids map to `Side::Buy`.
- NO bids map to YES asks at `1.00 - no_price` and `Side::Sell`.
- Modern `*_dollars_fp` fields and legacy cent/contract fields are both decoded.
- Per-subscription `seq` is validated. A gap drops the socket and resubscribes.
- `update_subscription` is used for add/delete and `get_snapshot` for recovery.
- `price_ranges` is consumed as the authoritative fixed-point grid; every band
  and its step are retained. `price_level_structure` is stored as a label only.
- `tick_size_change` / `price_level_structure_updated` lifecycle events update
  the in-memory and sidecar tick metadata. The supplied SBE schema is unchanged.

## Quick start: Polymarket public fallback

No credentials are required.

```bash
cargo run --release -- \
  --exchange polymarket \
  --max-markets 20 \
  --output captures/polymarket.sbe \
  --metadata captures/polymarket-markets.json \
  --report captures/polymarket-report.json \
  collect
```

Polymarket uses Gamma for discovery and the public CLOB market channel for data.
`book` events are snapshots; `price_change` events carry absolute per-level
sizes. There is no exchange sequence number, so protocol faults are recovered by
reconnecting and resubscribing, which yields fresh books.

Discovery uses Gamma's keyset pagination (`GET /markets/keyset`): every page is
fetched with `closed=false`, a `limit` of at most 100 records, and the previous
page's `next_cursor` as `after_cursor`. Offset-style pagination is never used,
because Gamma rejects deep offsets with HTTP 422, so this is also how the full
universe is enumerated. Cursor cycles are rejected, repeated cursors fail the
round, rate-limit responses are retried with `Retry-After`, and records missing
an explicit `acceptingOrders == true` are excluded under the default strict
policy (a missing value is unconfirmed, not tradable; set
`polymarket.require_accepting_orders = false` to keep unknown records while
retaining the other filters).

## Raw-frame journal

Set `journal.directory` (or `--journal-dir`, or `COLLECTOR_JOURNAL_DIR`) to keep
the exact payload and receive metadata for every public data frame:

```bash
cargo run --release -- --exchange polymarket --max-markets 20 \
  --output captures/polymarket.sbe \
  --journal-dir captures/journal \
  collect
```

Each run writes a new directory containing an atomically updated manifest and
append-only segments. Records preserve the transport frame kind, source shard
and replica, connection epoch, per-source receive sequence, receive time,
decode result, and exact bytes. Rejected frames are retained with a stable
category. Finalized segments are checksummed and listed in the manifest; an
interrupted run leaves a `.partial` segment and an open manifest.

Verify a completed run with:

```bash
cargo run --release -- journal verify captures/journal/<run-id>
```

The command checks segment headers, footers, record checksums, counts,
provenance, per-source ordering, unlisted files, and partial files, then exits
non-zero unless the run is complete. Queue saturation, write/sync failures, and
finalization timeouts fail the run instead of dropping frames. Details and the
on-disk format are in
[`docs/raw-frame-journal.md`](docs/raw-frame-journal.md).

## Commands

```text
collect    Live collection (default)
replay     Replay a normalized JSONL fixture through the same core
inspect    Decode an aligned SBE capture and print a JSON summary
verify     Rebuild each book from deltas and check it against every snapshot
journal    Verify a raw-frame journal run
latency    Measure discovery RTT, exchange-to-local timestamp latency, and
           cross-connection arrival skew
discover   Print the current live market set as JSON
```

Useful global flags include `--config`, `--output`, `--chunk-size`, `--shards`,
`--replicas`, `--snapshot-interval-secs`, `--discovery-interval-secs`,
`--max-markets`, `--duration-secs`, `--metrics-listen`, `--metadata`,
`--report`, `--journal-dir`, and `--log-json`. Transport flags apply to every
websocket connection: `--no-tcp-nodelay`, `--source-ip`, `--source-port`, and
`--connect-race` (see [Latency experiments](#latency-experiments-extension-4)).

Examples:

```bash
# 60-second smoke run with a 16 KiB aligned chunk
cargo run --release -- --exchange polymarket --max-markets 10 \
  --duration-secs 60 --chunk-size 16384 --output /tmp/poly.sbe collect

# Inspect a capture; auto-detection is convenient for ad-hoc files
cargo run --release -- inspect /tmp/poly.sbe --read-chunk-size auto --limit 3

# Verify the capture: rebuild each book from deltas and compare it with the
# next snapshot; a non-zero exit means the capture is not self-consistent
cargo run --release -- verify /tmp/poly.sbe

# Replay the checked-in deterministic fixture
cargo run --release -- --output /tmp/replay.sbe replay fixtures/sample_replay.jsonl

# One measured public-feed latency experiment
cargo run --release -- --exchange polymarket --max-markets 3 \
  --duration-secs 15 latency --out /tmp/polymarket-latency.json
```

## Architecture

```text
Exchange discovery ──► CollectorCore ──► serialized SBE writer
       │                  ▲   │
       │                  │   └── subscription / snapshot / reconnect controls
       ▼                  │
  source workers ─────────┘
  (one per shard replica)
```

- `src/exchange/` contains exchange-specific discovery, authentication, wire
  decoding, and `MarketDataSession` implementations.
- `src/session.rs` is the generic worker supervisor: connect, reconcile
  subscriptions, read/decode frames, heartbeat, periodic refresh, idle timeout,
  reconnect backoff, and status reporting.
- `src/collector/state.rs` is a synchronous, deterministic state machine. It
  owns shard assignment, per-source shadow books, active-source selection,
  sequence-reset handling, snapshot scheduling, removal snapshots, and stats.
  It has no network or wall-clock dependency; replay drives it with fixture
  timestamps.
- `src/coordinator.rs` owns live discovery, worker spawning, the event loop,
  signal handling, final snapshots, and sidecar/report writing.
- `src/writer.rs` owns exactly one writer thread. All SBE messages go through a
  bounded channel, preserving total write order.
- `src/util/aligned_sbe_writer.rs` and `aligned_sbe_reader.rs` implement the
  supplied aligned-chunk format. Messages never cross a chunk boundary; unused
  tail space is zero-padded.

### Sharding and redundancy

The shard count is fixed for the lifetime of a run. New symbols are assigned to
the least-loaded shard, so discovery churn does not move existing symbols. Every
shard has `replicas` websocket connections; all replicas receive the same symbol
set and maintain independent shadow books.

One source per symbol is active. A replica can take over only when its book is
valid and, if clocks are available, not older than the last emitted state. This
prevents a stale backup from silently regressing the output. If no backup is
ready, the core requests fresh snapshots and waits.

`collector.max_markets` counts selected venue market records: Gamma records for
Polymarket and market tickers for Kalshi. One Polymarket record can contribute
several outcome tokens, and the cap is applied between records so a condition's
outcome set is never split. Discovery telemetry reports selected records and
outcome symbols separately.

Before any socket opens, the coordinator builds a capacity plan from the
selected universe. The plan derives or validates the shard count, then rejects a
run whose total connections, largest initial subscription frame, estimated book
memory, or estimated snapshot rate exceeds its configured ceiling. The plan is
written to the run report. These are planning bounds, not measured claims; see
`docs/operating-envelope.md`.

### Snapshot policy

- First exchange snapshot for a symbol is written immediately.
- Periodic local snapshots are scheduled every 60 seconds, spread
  deterministically across the interval using a stable symbol hash.
- Failover and recovery write a fresh full snapshot before deltas resume.
- Removed symbols receive an empty snapshot if they had been emitted.
- Graceful shutdown writes one final snapshot per live active book.

### Units and timestamps

The SBE schema uses millionths:

- price `1_000_000` = $1.00
- quantity `1_000_000` = one contract/share

`received_at` / `backtest_timestamp` is the local wall-clock time captured
immediately after a websocket frame is read and before decoding. The
`exchange_timestamp` is the best exchange-provided timestamp: Kalshi
`sending_ts_ms` / `ts_ms`, or Polymarket `timestamp`. When no exchange timestamp
exists, the local receive time is used.

### Ordering and timestamp contract

- Receive timestamps are captured once per frame and serialized unchanged. The
  writer does not clamp or rewrite them.
- File order is the single writer's processing order across all sources. It is
  **not** globally sorted by timestamp. A frame from one source can be processed
  after a later-timestamped frame from another source, so cross-symbol receive
  timestamps may regress.
- Per symbol, the first full snapshot anchors the book; later deltas apply to
  the most recent snapshot in file order. Failover, recovery, removal, periodic,
  and final snapshots are emitted before any resumed deltas.
- Generated snapshots are stamped with the wall-clock time at which they are
  produced, so they share one timeline with event receive times. A failover
  snapshot therefore carries the status-processing time, which never precedes
  the receive time of any update included in the replacement book; the lost
  source's last-frame time stays in its source status, and the shadow's last
  exchange timestamp is preserved as the snapshot's exchange timestamp.
- Durations, retries, and timeouts use monotonic timers (`Instant`/Tokio) and
  are never serialized. Every serialized timestamp — event receive times and
  generated snapshots — comes from the wall clock; a wall-clock step moves the
  timeline for both consistently, with no clamping. Replay uses fixture
  timestamps so the core remains deterministic.
- No bounded reordering buffer is implemented. Global chronological output is
  not a contract; sorting a capture by timestamp is not equivalent to
  replaying it, so a consumer that needs a sorted stream must buffer
  downstream with an explicit delay.

Deterministic tests:
`collector::state::tests::snapshot_anchors_delta_application_in_per_symbol_order`,
`collector::state::tests::failover_snapshot_anchors_resumed_deltas_and_preserves_receive_times`,
`collector::state::tests::cross_symbol_output_order_is_processing_order_and_not_globally_sorted`,
and the E2E per-symbol anchor helpers.

## Output

The output file is an aligned SBE stream with 64 MiB chunks by default, the
same chunk size the template's `AlignedSbeReader::open()` expects. A periodic
flush writes only the encoded bytes, without padding; the final chunk is
zero-padded on a graceful close, so the closed capture is readable by both the
supplied helper and `inspect`. The same format is used for stdout
(`--output -`): redirect it to a file (`> capture.sbe`) when the template
reader will consume it.

A live capture may be read while it is being written: complete chunks decode
normally, and the partial final chunk decodes up to its last complete message.
It is safe to inspect with:

```bash
cargo run --release -- inspect captures/polymarket.sbe --read-chunk-size auto
```

Two optional JSON sidecars are written atomically:

- `--metadata`: current market set, namespaced SBE symbol, native exchange id,
  title/outcome, close time, tick size, full `price_ranges`, and the
  human-readable `price_level_structure` when supplied.
- `--report`: run mode, environment, start/end timestamps, shutdown reason,
  non-secret configuration, capacity plan, discovery telemetry, catalog source,
  persistence result, coverage gauges, writer counters, and final source
  status.

Existing capture, metadata, report, catalog, manifest, and segment files are
startup errors. `output.overwrite = true` (or `--overwrite`) is required to
replace them. The check runs before discovery or writer creation, so a rejected
startup leaves the previous run untouched.

### Rotation and durability

`output.rotate_bytes` and `output.rotate_secs` bound each segment by size or
time. Rotation happens only between messages, so a message is never split.
Segments are staged as `<path>.<sequence>.sbe.partial`, finalized with a
SHA-256 checksum, fsynced, and atomically renamed to `<path>.<sequence>.sbe`.
Each finalization updates `<path>.manifest.json`, which records the schema
version, chunk size, creation time, completion flag, segment names, byte counts,
checksums, and finalization times. The run report carries the manifest path.

`output.min_free_bytes` stops the writer before the output filesystem fills.
The watermark is checked with `df -Pk` at most once every five seconds.
`output.fsync_interval_secs` periodically persists the active file without
padding its final chunk; zero syncs only on rotation and shutdown.

`inspect` and `verify` operate on one aligned file. For a rotated capture,
run them on each finalized segment independently or concatenate segments only
when the consumer can start decoding at a message boundary. The manifest is the
authority for segment order and integrity.

### Catalog checkpoints

`collector.catalog_path` persists the last complete, accepted discovery round as
a versioned JSON checkpoint with the exchange name, generation time, selected
record and outcome counts, and a SHA-256 digest over the market list. A
checkpoint is written only after a full discovery round has been accepted; a
partial page set, repeated cursor, or failed validation never replaces it.

`collector.catalog_mode` selects startup behavior:

| Mode | Behavior |
|---|---|
| `off` | Always discover; never read a checkpoint. |
| `warm` | Use a fresh, verified checkpoint, otherwise discover. |
| `require` | Require a fresh, verified checkpoint; fail startup otherwise. |
| `stale` | Require any verified, non-empty checkpoint regardless of age. |

A checkpoint from another exchange, with a failed checksum, an invalid market,
or a count mismatch is rejected. `warm` falls back to discovery; `require` and
`stale` fail startup. A warm checkpoint's generation time is preserved across
metadata writes, so a restart cannot make an old catalog look fresh.

## Metrics and readiness

```bash
cargo run --release -- --metrics-listen 127.0.0.1:9090 ...
curl -s http://127.0.0.1:9090/metrics
curl -s http://127.0.0.1:9090/healthz
curl -s http://127.0.0.1:9090/livez
curl -s http://127.0.0.1:9090/readyz
```

Metrics include symbols, anchored and stale symbols, expected and sent
subscriptions, shard/source coverage, snapshots, deltas, rejected deltas,
sequence resets, source switches, stale probes, writer queue depth, bytes,
segments, persistence state, catalog and discovery ages, discovery page/record/
outcome/malformed/retry counters, readiness, and per-source status.

`/livez` reports process liveness and stays `200` while the HTTP server can
answer. `/readyz` returns `200` only when every invariant holds and `503`
otherwise, with a JSON check for each: writer health and zero write errors,
source coverage on every shard, anchored-symbol fraction, stale books, catalog
age, persistence failure, and fatal state. `/healthz` remains source-oriented
and includes the readiness boolean.

`docs/metric-runbook.md` lists the alert signals and the first diagnostic step
for each.

## Replay, inspection, and verification

`fixtures/sample_replay.jsonl` is a deterministic fixture covering discovery,
two replicas, snapshots, absolute deltas, a source loss and failover, a sequence
reset and recovery, a lifecycle removal, periodic snapshots, and finalization.
The complete record schema is documented in
[docs/replay-format.md](docs/replay-format.md).

`inspect` proves that every message decodes. `verify` goes further: it
reconstructs each symbol's book from the captured deltas and compares that state
with the next full snapshot in the file. A mismatch means the capture does not
describe one complete book history (a lost, duplicated, or out-of-order delta),
so the command exits non-zero and reports the offending symbols with samples.
`unknown_messages` (the schema allows template ids this collector never emits)
and deltas that arrive before their symbol's first snapshot also fail
verification; the check is strict because a silently inconsistent backtest is
worse than a failed capture. An empty snapshot is treated as a
retirement/empty-book boundary rather than a mismatch: it carries no level
evidence, so it cannot prove that a suppressed update was observed. Only a
later non-empty snapshot can expose a gap.

### Capture completeness and the mock exchange

`tests/e2e_mock_exchange.rs` starts loopback Gamma keyset and Polymarket
WebSocket servers, drives `coordinator::run_with_shutdown`, and reads the
resulting SBE file through the same reader used by `inspect`/`verify`. The mock
exchange owns the authoritative book and a full delivered-update history. The
seven scenarios are:

1. steady state: every delivered update and resulting book matches the mock
   history, not only the final book;
2. one socket loss: the failover snapshot anchors the shadow book and resumed
   deltas apply to it;
3. full outage: updates during the uncovered interval are absent and `verify`
   reports the mismatch instead of claiming completeness;
4. discovery churn: a removed symbol ends with an empty retirement snapshot and
   a new symbol starts with its own snapshot;
5. silent subscription: suppressed updates trigger a stale probe that cycles
   the subscription on the live connection, and the repair book arrives as a
   fresh exchange snapshot;
6. negative control: a missing update is detected when a later non-empty
   snapshot exposes it;
7. subscription scale: 1,000 symbols, 4 shards, 2 replicas, 8 WebSockets, 250
   symbols per socket, and 2,000 subscription entries.

The mock exchange is the completeness oracle for these scenarios; `verify`
checks only capture self-consistency. The full-outage and negative-control
tests assert that `verify` is invalid.

The scale test is the tested operating envelope. The 548,942-token sweep is
discovery evidence, not subscription evidence at that fan-out; configured
bounds are not measured live capacity.

## Latency experiments (Extension 4)

The `latency` command measures:

1. **Discovery RTT** — wall-clock time around one discovery call.
2. **Exchange-to-local timestamp latency** — `local_receive_us -
   exchange_timestamp_us` per event, reported as percentiles. This includes
   exchange/local clock offset and the venue's own timestamp semantics.
3. **Connection setup** — per-replica `replica_connect_ms` (TCP + TLS +
   websocket handshake, including any racing) and `replica_first_frame_ms`
   (subscription to first frame).
4. **Cross-connection arrival skew** — with `--replicas N`, every replica
   subscribes to the same symbols and identical events (same symbol, exchange
   timestamp, exchange clock, and event shape) are matched across connections.
   Each delta is measured against the earliest arrival, which is
   clock-independent: it compares when the same update reached two sockets on
   this host. Per-replica `replica_events` and `replica_first_arrivals` show
   whether one connection is consistently ahead.

The first two seconds after all subscriptions are established are excluded so
initial snapshot dumps do not dominate the skew distribution. Events only one
replica observes are counted as abandoned rather than silently dropped.

Transport experiments are first-class configuration (`[network]` in TOML or the
matching CLI flags) and apply to `collect` as well as `latency`:

- `tcp_nodelay` (default `true`) sets `TCP_NODELAY` on every websocket socket;
  the underlying library does not do this for you.
- `source_ip` / `source_port` bind the local side of every connection before
  connecting, which is how source-port pinning is measured. The configured port
  is a base: connection `shard * 256 + replica` binds `base + ordinal`, so
  parallel connections never collide. A fixed source port cannot be combined
  with racing.
- `connect_race` opens that many full TCP + TLS + websocket handshakes
  concurrently and keeps the first success; the losers are closed. Racing
  covers the entire handshake through the websocket upgrade, so the winner is
  a usable connection.

An authentication rejection (HTTP 401/403) is never raced away: it fails
immediately so credential errors stay diagnosable.

## Measured latency

Four Extension 4 experiments were measured on this machine (macOS, arm64) on
2026-10-24, each with five Polymarket tokens, two replicas, and a 60-second
window; the raw reports are checked in under `measurements/`:

| Metric | Baseline | `--connect-race 4` | `--source-port 51800` | `--no-tcp-nodelay` |
|---|---:|---:|---:|---:|
| Connect (per replica) | 1487 / 1007 ms | 636 / 759 ms | 3167 / 5313 ms | 1164 / 1432 ms |
| Matched cross-connection events | 176 | 216 | 354 | 396 |
| Arrival skew p50 | 65 µs | 59 µs | 67,845 µs | 10,543 µs |
| Arrival skew p95 | 27,004 µs | 5,496 µs | 74,093 µs | 15,280 µs |

These are four single 60-second observations on one network path, not
controlled benchmarks across networks or colocated hosts, and not causal
identification. The cross-connection arrival skew is the
clock-independent number: two sockets on this host usually saw the same update
within tens of microseconds, and racing four full handshakes selected a faster
connection in these runs. Source-port pinning was the *worst* configuration in
this sample; one hypothesis is that the venue's CDN hashes the source port onto
a worse origin path, but that mechanism was not isolated. Disabling
`TCP_NODELAY` correlated with a skew p50 of 10.5 ms instead of 65 µs; a
Nagle/delayed-ACK interaction is a hypothesis, not a proven cause. Timestamp
latency (`local_receive_us - exchange_timestamp_us`) remains dominated by
exchange/local clock offset and venue timestamp semantics. The methodology, all
caveats, and the full findings are in [docs/latency.md](docs/latency.md).

A 20-second public smoke capture with 4 symbols and 2 replicas produced 58 SBE
messages (8 snapshots and 50 deltas), with zero writer errors and zero rejected
deltas. The measured run report is checked in at
[measurements/polymarket-smoke-report.json](measurements/polymarket-smoke-report.json);
the exact reproduction command is in
[measurements/README.md](measurements/README.md).

## Configuration

See `config.example.toml` (Kalshi) and `config.polymarket.example.toml`.
Precedence is defaults, TOML, environment, then explicit CLI flags. Important
settings:

| Setting | Default | Meaning |
|---|---:|---|
| `collector.shards` | 4 | Fixed shard count; `0` derives it |
| `collector.replicas` | 2 | Redundant sockets per shard |
| `collector.max_symbols_per_socket` | 5000 | Enforced per-socket bound; used to derive shards when `collector.shards = 0` |
| `collector.max_shards` | 64 | Upper bound when deriving shards automatically |
| `collector.snapshot_interval_secs` | 60 | Full local snapshot cadence |
| `collector.discovery_interval_secs` | 30 | Discovery reconciliation cadence |
| `collector.reconnect_after_secs` | 1800 | Periodic socket refresh |
| `collector.connection_idle_timeout_secs` | 45 | No-frame reconnect threshold |
| `collector.stale_probe_after_secs` | 120 | Quiet-symbol probe threshold |
| `collector.probe_interval_secs` | 30 | Probe spacing |
| `collector.max_probes` | 3 | Probes before reconnect/failover |
| `collector.final_snapshots` | true | Final snapshots on shutdown |
| `collector.writer_queue` | 16384 | Bounded writer queue |
| `collector.flush_interval_secs` | 1 | Output-visibility bound: every interval with traffic ends with an unpadded flush, so a downstream reader sees an accepted delta within about one second |
| `collector.max_markets` | 0 | Cap on selected venue market records; one Polymarket record keeps its full outcome set |
| `collector.max_total_connections` | 64 | Startup ceiling on `shards * replicas` sockets |
| `collector.max_initial_subscription_bytes` | 1048576 | Startup ceiling on the largest initial subscription frame |
| `collector.max_estimated_book_memory_mb` | 1024 | Startup ceiling on estimated resident book memory |
| `collector.max_snapshot_bytes_per_sec` | 16777216 | Startup ceiling on estimated snapshot output |
| `collector.connect_concurrency` | 8 | Maximum simultaneous websocket handshakes across all workers |
| `collector.catalog_path` | none | Last-known-good discovery checkpoint |
| `collector.catalog_mode` | `off` | `off`, `warm`, `require`, or `stale` |
| `collector.catalog_max_age_secs` | 3600 | Maximum accepted age for `warm` and `require` |
| `collector.allow_empty_catalog` | false | Accept a complete discovery round that retires every symbol |
| `collector.readiness_min_anchored_fraction` | 0.99 | Minimum anchored-symbol fraction for `/readyz` |
| `collector.readiness_max_catalog_age_secs` | 3600 | Maximum catalog age for `/readyz` |
| `collector.readiness_max_stale_secs` | 120 | Maximum time since a book's last valid update for `/readyz` |
| `journal.directory` | none | Root for raw-frame runs; unset disables the journal |
| `journal.queue_capacity` | 4096 | Bounded queue between source workers and the journal writer |
| `journal.enqueue_timeout_ms` | 250 | Maximum wait for queue space before the run fails |
| `journal.flush_interval_ms` | 1000 | Bound on records held in the process buffer |
| `journal.sync_interval_ms` | 5000 | Interval between storage syncs in `interval` mode |
| `journal.segment_max_bytes` | 268435456 | Size rotation trigger; `0` disables |
| `journal.segment_max_secs` | 300 | Time rotation trigger; `0` disables |
| `journal.sync_mode` | `interval` | `interval` or `always` (fsync every record) |
| `network.tcp_nodelay` | true | `TCP_NODELAY` on every websocket socket |
| `network.source_ip` | none | Local IP to bind before connecting |
| `network.source_port` | none | Base local source port; connection `shard*256+replica` binds `base + ordinal`; not combinable with `connect_race > 1` |
| `network.connect_race` | 1 | Simultaneous full handshakes; the first success is kept |
| `network.connect_timeout_secs` | 30 | Deadline for one full connection attempt, including every raced candidate |
| `polymarket.discovery_page_size` | 100 | Gamma keyset page size; the endpoint accepts at most 100 |
| `polymarket.require_accepting_orders` | true | Only an explicit `acceptingOrders == true` is tradable |
| `output.chunk_size` | 67108864 | Power-of-two chunk; defaults to 64 MiB for template-reader compatibility. Padding is written only on chunk rotation and at close |
| `output.overwrite` | false | Replace existing capture, sidecars, catalog, manifest, and segments |
| `output.rotate_bytes` | 0 | Size rotation threshold; zero disables it |
| `output.rotate_secs` | 0 | Time rotation threshold; zero disables it |
| `output.min_free_bytes` | 0 | Free-space watermark checked at most every five seconds |
| `output.fsync_interval_secs` | 0 | Periodic fsync cadence; zero syncs only on rotation and shutdown |
| `output.path` | `-` | File or binary stdout |

## Repository layout

```text
src/
  cli.rs                      CLI definitions and overrides
  config.rs                   TOML/env/CLI configuration and validation
  capacity.rs                 startup capacity plan and shard derivation
  catalog.rs                  versioned, checksummed discovery checkpoint
  persistence.rs              latest-wins metadata/catalog writer
  coordinator.rs              live orchestration, discovery, signals, reports
  collector/
    mod.rs                    collector module re-exports
    state.rs                  deterministic core, sharding, shadow books, failover
  exchange/
    mod.rs                    exchange module re-exports
    kalshi/
      mod.rs                  Kalshi module re-exports
      auth.rs                 Ed25519/RSA authentication and challenge signing
      codec.rs                SBE decode: snapshots, deltas, lifecycle, control
      discovery.rs            REST discovery with keyset pagination, filters, retry
      session.rs              Kalshi websocket session, seq validation, reconnect
    polymarket/
      mod.rs                  Polymarket module re-exports
      codec.rs                CLOB book/price_change decode, absolute sizes
      discovery.rs            Gamma keyset discovery, outcome expansion, cursor cycle detection
      session.rs              public CLOB websocket session, snapshot-driven recovery
  fixed.rs                    fixed-point decimal (1e6) arithmetic and parsing
  inspect.rs                  aligned SBE inspection CLI
  journal.rs                  checksummed raw-frame journal: segments, manifest, verification
  latency.rs                  measured latency, connection setup, cross-replica skew
  lib.rs                      crate root, public API, test modules
  main.rs                     binary entry, command dispatch
  metrics.rs                  Prometheus text exporter, /healthz, /readyz, /livez
  model.rs                    core domain types: Symbol, Side, PriceLevel, Book
  orderbook.rs                in-memory order book: apply snapshots/deltas, best bid/ask
  replay.rs                   normalized JSONL replay through core state machine
  report.rs                   atomic run-report and metadata sidecars
  session.rs                  generic source supervisor: connect, heartbeat, controls, reconnect
  side.rs                     Side::Buy/Side::Sell with serialization
  time.rs                     wall-clock timestamp helpers and conversions
  util/
    mod.rs                    util module re-exports
    aligned_sbe_reader.rs     chunk-aligned SBE reader with auto-detection
    aligned_sbe_writer.rs     chunk-aligned SBE writer, padding, rotation
    build.rs                  build-time SBE code generation from YAML
    dtos.rs                   generated DTOs from supplied schema
    orderbook_schema.rs       schema constants, template IDs
    sbe_yaml.rs               schema YAML parsing for codegen
  verify.rs                   capture self-consistency verification CLI
  writer.rs                   serialized SBE writer, rotation, watermark, fsync
  ws.rs                       websocket transport: TCP_NODELAY, source binding, racing
fixtures/                     deterministic replay fixture (sample_replay.jsonl)
tests/
  e2e_mock_exchange.rs        loopback Gamma+CLOB mock exchange, 9 scenarios
  live_polymarket.rs          opt-in live Polymarket public feed tests
  replay_fixture.rs           deterministic fixture replay + verify integration
docs/
  architecture.md             source traceability matrix, Mermaid diagrams
  correctness-audit-2026-10-26.md  test counts, CI run links, evidence
  final-verification-2026-10-25.md baseline 127-test verification
  integration-audit-2026-10-26.md  integration audit
  latency.md                  Extension 4 methodology, caveats, raw data
  metric-runbook.md           alert signals and first diagnostic step per metric
  operating-envelope.md       planning bounds, derived shard count, defaults
  raw-frame-journal.md        raw-frame journal format, durability, verification
  replay-format.md            JSONL replay record schema
  requirement-test-matrix.md  requirement-to-test traceability
deploy/                       deployment unit and operational notes
measurements/                 exact measured reports from public runs
scripts/
  check_architecture.py       architecture traceability gate (source files, tests, diagrams)
  render_architecture_diagrams.py  Mermaid -> SVG diagram renderer
```

## Known limitations and notes

- The full Polymarket universe is not a supported default envelope. A measured
  universe of 548,942 outcome tokens needs 110 sockets at 5,000 symbols per
  socket, above the default `max_shards = 64`; the startup capacity gate rejects
  that configuration. Raising the limits requires measured memory, network, and
  output evidence for the target host.
- The 24-hour continuous soak with injected faults has not been executed. The
  mock scale runs and fault-injection tests cover bounded scenarios; see
  `docs/operating-envelope.md` for the exact evidence and the remaining gate.
- Kalshi live validation requires credentials that are not checked into this
  repository. The authenticated path is implemented and covered by codec/auth
  and mock-websocket tests; a live run is the next validation step once
  credentials are supplied.
- No geographic or access restriction was bypassed. Kalshi live validation
  remains blocked without authorized credentials; Polymarket is used only as
  the permitted public fallback.
- Kalshi endpoint reachability depends on the environment. The adapter
  supports production (the default) and demo; select demo mode for
  simulated validation.
- Polymarket is a public, best-effort feed with no sequence numbers. Protocol
  faults recover by reconnect-and-resubscribe; quiet-symbol probes cycle the
  subscription on the live connection instead. Its event timestamps are not a
  send-time clock.
- `inspect --read-chunk-size auto` streams candidate chunks from disk instead of
  reading the whole file into memory, and never considers a chunk larger than
  256 MiB; captures written with a larger chunk need an explicit
  `--read-chunk-size`. Auto-detection may select a smaller power-of-two chunk
  that also validates the file, so pass the writer's exact `--chunk-size` when
  the exact chunk size matters.

## License

MIT. The supplied SBE schema is unchanged.
