# Orderbook collector: redundancy, recovery, and SBE output

Implements an exchange-adapter-based L2 orderbook collector with redundant
WebSocket connections, subscription monitoring, and aligned SBE output.

- Full orderbook snapshots are emitted every 60 seconds. Each socket is recycled
  every 600 seconds (10 minutes), with staggered replacement.
- Failover promotes the backup, emits the current in-memory book, requests a
  fresh exchange snapshot, and replaces the failed connection. This provides
  redundancy and recovery; it does not guarantee zero message loss.
- Events from non-primary sockets are ignored. Primary events with clocks older
  than the current book are rejected before state mutation and SBE output.
  Equal clocks remain accepted. This does not guarantee duplicate suppression
  or sequence continuity across connections.
- The aligned SBE writer now encodes into initialized vector memory without an
  unsafe block, and discards partial bytes on encoding errors or length mismatch.
  Environment initialization still uses `unsafe` in `src/main.rs`, before Tokio
  worker threads start. The implementation is not described as 100% safe Rust.

## Validation

Independently executed against this working tree:

- `cargo test --all-targets`: 102 tests passed; all 29 benchmark cases completed
  using the repository's local Criterion-compatible harness (debug profile).
- Includes regressions decoding SBE output to verify stale deltas/snapshots and
  non-primary updates are absent, and checking writer rollback on errors.
- `cargo test --doc`: passed (no documentation tests).
- `cargo build --release`: passed.
- `git diff --check`: passed.

The original 94-test / 29-microbenchmark figures were author-reported. The counts
above come from this run; benchmark completion is not a release-performance claim.
Strict `cargo clippy --all-targets -- -D warnings` remains blocked by 12 existing
style lints in collector, Kalshi, and mock modules. Repository-wide formatting
also has existing differences. Live exchange connectivity and lossless failover
have not been established by these offline tests. Latency measurements in
LATENCY.md remain author-reported, not independently reproduced.
