# Repository audit behind this design

**Reviewed ref:** `polymarket-continuity-failure` at `e19a85c204d0eaf54d862502035947d21d12f149`  
**Main ref at review:** `9ab6cd5a584fea58c0086efa50d55c796e9aac4e`  
**Review date:** 2026-11-02

This is a source inspection and GitHub metadata review. It is not a new local compile, live exchange test or soak run.

## Pinned source locations

- [`src/journal.rs`](https://github.com/kj111422/engineering-challenge-template/blob/e19a85c204d0eaf54d862502035947d21d12f149/src/journal.rs) — raw journal, segment writer, manifest, verification, `AdmissionGuard`, `close_admission_and_wait`, shutdown and abort.
- [`src/session.rs`](https://github.com/kj111422/engineering-challenge-template/blob/e19a85c204d0eaf54d862502035947d21d12f149/src/session.rs) — `journal_ingress`, per-source receive fence, protocol/decode failure handling, `RecoveryReady` worker event.
- [`src/coordinator.rs`](https://github.com/kj111422/engineering-challenge-template/blob/e19a85c204d0eaf54d862502035947d21d12f149/src/coordinator.rs) — bootstrap/periodic discovery, event handling, timer ticks, source-status flow, recovery acknowledgement, shutdown and journal creation.
- [`src/replay.rs`](https://github.com/kj111422/engineering-challenge-template/blob/e19a85c204d0eaf54d862502035947d21d12f149/src/replay.rs) — normalized JSONL replay record enum and runner.
- [`docs/replay-format.md`](https://github.com/kj111422/engineering-challenge-template/blob/e19a85c204d0eaf54d862502035947d21d12f149/docs/replay-format.md) — normalized replay schema currently documented.
- [`src/model.rs`](https://github.com/kj111422/engineering-challenge-template/blob/e19a85c204d0eaf54d862502035947d21d12f149/src/model.rs) — `MarketInfo`, `EventMeta`, source IDs and shared normalized data types.
- [`src/collector/state.rs`](https://github.com/kj111422/engineering-challenge-template/blob/e19a85c204d0eaf54d862502035947d21d12f149/src/collector/state.rs) — large synchronous core, per-source books, recovery requests, continuity incident state and direct continuity setter.
- [`src/exchange/polymarket/discovery.rs`](https://github.com/kj111422/engineering-challenge-template/blob/e19a85c204d0eaf54d862502035947d21d12f149/src/exchange/polymarket/discovery.rs) — Polymarket Gamma fields and CTF/position identifier selection.
- [`src/exchange/kalshi/codec.rs`](https://github.com/kj111422/engineering-challenge-template/blob/e19a85c204d0eaf54d862502035947d21d12f149/src/exchange/kalshi/codec.rs) — Kalshi native feed decoding and normalized side representation.
- [`docs/raw-frame-journal.md`](https://github.com/kj111422/engineering-challenge-template/blob/e19a85c204d0eaf54d862502035947d21d12f149/docs/raw-frame-journal.md) — current journal contract and limitations.
- [`docs/production-roadmap-polymarket.md`](https://github.com/kj111422/engineering-challenge-template/blob/e19a85c204d0eaf54d862502035947d21d12f149/docs/production-roadmap-polymarket.md) — current phased roadmap.

## Important corrections to the source review

1. **A raw tier already exists.** `src/session.rs` calls `journal_ingress` before the exchange decode outcome is delivered to the core. `src/journal.rs` stores source, shard/replica, connection epoch, receive sequence, receive timestamp, frame kind and exact payload. Multiple replicas therefore remain separately observable in the raw journal. The canonical output is a projection chosen by the core; it does not erase the raw journal records.
2. **Normalized replay already has a meaningful input model.** `ReplayRecord` includes discovery, snapshot, delta, lifecycle, reset, status and tick. It is not raw replay and is not yet complete relative to live orchestration.
3. **The critical missing replay input is not just discovery/status/tick.** Live coordinator code consumes `WorkerEvent::RecoveryReady` and calls `core.handle_recovery_ready`. There is no corresponding normalized replay record. Replay also initializes event metadata with zero connection epoch and receive sequence, while live source frames carry actual fences.
4. **Raw header metadata needs expansion.** The current `RecordHeader` contains wall receive time, source, connection epoch and receive sequence but no explicit connection UUID, monotonic receive timestamp, process boot ID, full protocol-version tuple or clock-quality/uncertainty reference.
5. **Instrument data is partly parsed but incompletely retained.** Polymarket discovery recognizes a market version to choose CTF token IDs versus position IDs, but the shared `MarketInfo` does not preserve that version/identifier scheme as a typed listing field. `MarketInfo` also lacks explicit min size, fee schedule, collateral asset and neg-risk relation. Kalshi price-range information is represented, but native quote semantics and fee overrides are not first-class fields in the current common listing model.
6. **Continuity is extensive but not enforced by a standalone transition function.** `MarketState::set_continuity` directly assigns a state; continuity decisions are distributed through the large state module. The proposal is to extract a pure transition table while preserving current behavior through replay equality tests.

## Branch and CI evidence

- `polymarket-continuity-failure` was 16 commits ahead of `main` and 0 behind at review.
- PR #2 merged `journal-failure-hardening` into `polymarket-continuity-failure`, producing `e19a85c`.
- CI ran on source head `5924eb6e3861d551a0f62c58eb1dc2e689625390`; the merge commit has the same Git tree.
- Rust CI run `41447311946`: 221 library + 12 mock E2E + 2 replay tests = 235 passed, 3 ignored; format, Clippy, docs and release build steps passed.
- Architecture run `41447311921`: 42 Rust source files, 238 attributed test functions, 9 structurally checked Mermaid diagrams; passed.

No evidence was found in this review that a live Kalshi validation, 24-hour fault-injection soak or physical disk-exhaustion drill has been completed. The design keeps these as open gates.

## External protocol references checked

- Polymarket's help-center notice describes the April 28, 2026 exchange upgrade, new contracts/order book, pUSD collateral and match-time fee calculation: [official notice](https://help.polymarket.com/en/articles/14762452-polymarket-exchange-upgrade-april-28-2026).
- Polymarket's Rust CLOB client documents distinct token-backed V1/V2 and position-backed V3 protocol paths: [official client repository](https://github.com/Polymarket/rs-clob-client-v2).
- Kalshi's multivariate events response schema includes `mutually_exclusive`, `settlement_sources`, `price_ranges`, `fee_type_override` and `fee_multiplier_override`: [official API reference](https://docs.kalshi.com/api-reference/events/get-multivariate-events).

These protocol details change. Before implementation, re-check the official contract and pin the retrieved version/schema/fixtures in the relevant PR.
