# Orderbook collector: redundancy, recovery, and SBE output

I built an exchange-adapter-based L2 orderbook collector with redundant WebSocket
connections, subscription monitoring, and aligned SBE output.

## What it does

- I discover open, non-MVE markets over REST, then poll again every 30 seconds.
- I split markets into buckets of up to 200, each with a primary and a shadow
  WebSocket. Every socket feeds one central loop that owns state changes.
- I keep fixed-point books per market. A YES bid stays a bid at price P, and a NO
  bid becomes an ask at 1 − P.
- I write absolute level sizes in `PriceLevelUpdate` messages and emit a full
  snapshot for every market every 60 seconds.

## How it recovers

- **Failover and recycling:** I promote the other socket, emit the in-memory book,
  hold back the bucket's deltas, and let a fresh exchange snapshot replace each
  book. Sockets are recycled every 10 minutes on a staggered schedule.
- **Sequence gaps:** I check `seq` on each connection. A gap on the primary triggers
  the same recovery. A gap on the shadow is covered by the snapshot at promotion.
- **Clock resets:** Kalshi documents that a market's clock can decrease during
  maintenance. A lower-clock delta is rejected and requests a snapshot, and a
  primary snapshot always replaces the book.
- **Silent connections:** I replace a connection after 30 seconds with no frames
  from the server.
- **Quiet markets:** After 90 seconds without an update, I request a snapshot and
  leave the subscription in place.

This is redundancy and recovery, not lossless capture. Changes during a recovery
reach the output through the fresh snapshot, not as individual updates.

## Failure behavior

- Live mode exits with an error if credentials are missing. Synthetic data requires
  an explicit `--mock` or `MOCK=1`.
- I retry initial market discovery with backoff and never subscribe to invented
  tickers.
- The process exits with a non-zero status if the collector stops with an error.

## Validation

I ran these against this branch:

- `cargo test --all-targets`: 116 tests passed, and all 29 benchmark cases completed
  in the debug profile.
- `cargo clippy --all-targets -- -D warnings`: clean.
- `cargo fmt --check`: clean.
- `cargo build --release`: passed.
- In a test with default timings, two fully silent connections were replaced after
  30 seconds.
- A 15-second mock run decoded to 6 snapshots and 300 updates, with no negative
  quantities.

The property tests and benchmarks use small in-repo harnesses that mirror the
`proptest` and Criterion APIs. Benchmark completion is not a performance claim.

## Live testing

I ran an earlier version against Kalshi production: it connected, ingested live
markets, and wrote SBE frames to disk. I have not repeated that run with the recovery
changes on this branch, so `get_snapshot` and `seq` handling are verified against
Kalshi's documentation and in-process tests only.

## Known limitations

- The event channels are unbounded, so a slow central loop grows memory.
- Each 60-second flush pads a 64 MiB chunk, which is about 90 GiB a day.
- Market discovery still calls Kalshi directly rather than going through
  `ExchangeAdapter`, so a second exchange needs one more step.
- `src/main.rs` still uses `unsafe` to set environment variables, before Tokio starts.
- `LATENCY.md` records research and planned experiments, not a demonstrated latency
  improvement.
