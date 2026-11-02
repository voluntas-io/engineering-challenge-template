# ADR-0001: Unified event log and deterministic replay

**Status:** Proposed  
**Date:** 2026-11-02  
**Owners:** Platform / data infrastructure

## Context

The current raw journal retains WebSocket application payloads and their source provenance, but does not replay them through the decoder and core. The current JSONL replay drives `CollectorCore` with normalized records, but it is a separate format and lacks some live control/acknowledgement inputs. The live coordinator also consumes discovery snapshots, source statuses, recovery acknowledgements, timer ticks and shutdown boundaries. Raw frames alone therefore cannot reproduce every core transition.

The current raw journal is valuable; discarding it or creating a second raw store would waste work and increase migration risk.

## Decision proposed

Adopt an append-only event-log abstraction with three linked streams:

1. Raw ingress (WebSocket application frames and raw REST discovery responses).
2. Total-ordered core inputs (all inputs to deterministic reducers).
3. Effects/results and reducer outputs (for causality and replay comparison).

Every canonical venue event references one or more raw records. The core-input sequencer assigns one monotonically increasing `core_input_offset` at the reducer boundary. The reducer consumes that offset order, never timestamp order.

The raw log remains the forensic source. The core-input log is the exact state-replay source. Both are immutable and share run/config/build/instrument provenance.

## Alternatives considered

- **Raw journal only:** rejected; it omits non-frame inputs and effect acknowledgements.
- **Normalized JSONL only:** rejected; it cannot re-decode venue bytes and is not the live writer's complete causal input stream.
- **One Kafka topic as immediate replacement:** deferred; no throughput or multi-host evidence justifies a broker migration yet.
- **Separate independent raw and canonical databases:** rejected for initial phase; creates unnecessary distributed consistency and retention problems.

## Consequences

Positive:

- Raw re-decode can investigate decoder bugs and protocol changes.
- Core replay can reproduce exact state transitions.
- Non-frame orchestration becomes reproducible.
- SBE becomes a rebuildable compatibility projection.

Costs:

- More storage, schemas and migration rules.
- Need an append-before-apply crash protocol and deterministic effect IDs.
- Need separate raw and core offsets because ingress order differs from core processing order.

## Required safeguards

- Preserve current `.pmj` reader as a legacy format reader.
- Append core input before applying it.
- A failed required append stops the run and invalidates completeness.
- No sorting by timestamp; store receive, event and processing order independently.
- Hash chains and signed manifest are a later integrity layer; SHA checksums alone are not authentication.

## Acceptance criteria

1. Every live reducer input is represented by a versioned `CoreInput` record.
2. Every frame-derived canonical event references the raw frame and decoder version.
3. `RecoveryAcknowledged`, discovery, source status, timer and shutdown events replay.
4. A mock live run replayed from core inputs reproduces output hashes/bytes.
5. Raw re-decode using the same decoder reproduces the same canonical events.
6. Crash injection after input append, after reducer step and before output append recovers to the same state/output.

## Revisit when

Storage throughput or multi-writer coordination exceeds the segmented-log envelope measured by Phase 4; revisit physical storage and broker technology without changing logical stream contracts.
