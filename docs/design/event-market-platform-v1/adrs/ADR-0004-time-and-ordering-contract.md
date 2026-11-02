# ADR-0004: Separate receive order, core order and event time

**Status:** Proposed  
**Date:** 2026-11-02  
**Owners:** Data infrastructure / replay

## Context

The current journal stores receive wall time, connection epoch and receive sequence. The current replay format imposes non-decreasing timestamps for convenience. The live writer's processing order is the order in which the coordinator handles inputs; cross-source receive timestamps can regress. A research/market-making system needs to preserve arrival order and still support event-time analytics.

## Decision proposed

Carry at least three order/time dimensions:

1. Per-source/per-connection application receive sequence.
2. Global per-core-run input offset assigned at reducer application.
3. Exchange event time, where provided.

Also record receive wall UTC time, monotonic time, process boot ID and clock-quality observation reference. Never compare monotonic time across boot IDs. Do not sort or rewrite records to create a globally monotone timestamp series.

Watermarks are derived analytics; late events remain in the log and in replay.

## Alternatives considered

- **Sort by event time before replay:** rejected; changes observed latency and causality.
- **Wall clock only:** rejected; wall clock can step/regress.
- **One global sequence at the raw ingress writer:** rejected as the only order; writer queue order is not necessarily coordinator/core processing order.

## Consequences

- More metadata per record and clock-sync observation records.
- Replay uses core-input offsets, not timestamps.
- Latency can be measured using monotonic time within a boot/session.
- Cross-venue event-time analytics need explicit watermark/late-arrival policy.

## Acceptance criteria

- Tests prove receive sequence increments exactly once per application payload.
- WebSocket control frames do not consume application receive sequence.
- Core-input offset order is stable and complete.
- Timestamp regressions remain visible and do not break replay.
- Monotonic measurements cannot be joined across unrelated boot IDs.
