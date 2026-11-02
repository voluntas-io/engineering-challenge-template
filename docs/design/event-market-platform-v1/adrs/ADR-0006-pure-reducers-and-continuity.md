# ADR-0006: Use pure reducers for persistent state machines

**Status:** Proposed  
**Date:** 2026-11-02  
**Owners:** Core engine

## Context

`CollectorCore` is synchronous and shared by live and normalized replay, which is a strong foundation. However, the large state module contains multiple responsibilities, and continuity state can be assigned directly via a setter. Tests cover many paths, but the transition contract is not yet a standalone exhaustive model.

## Decision proposed

Define state-machine reducers with an exhaustive transition function and explicit outputs/effects. Begin with continuity, then instrument lifecycle, OMS, risk reservations, portfolio/ledger and settlement.

The reducer receives time and external acknowledgements as inputs. Effects are deterministic IDs; asynchronous effects execute outside the reducer and re-enter as logged inputs.

## Alternatives considered

- **Continue adding branches to the existing `handle_event`:** rejected beyond short maintenance fixes; behavior will become harder to reason about.
- **Rewrite the entire collector core:** rejected; risks losing validated behavior.
- **Build independent simulation state machines:** rejected; violates one-code-path invariant.

## Consequences

- Incremental extraction is required.
- Transition tables and property tests become first-class artifacts.
- A deterministic effect/result log is required for replay.
- More types are needed for invalid/unknown states, but silent assumptions are reduced.

## Acceptance criteria

- No unvalidated continuity assignment.
- Exhaustive state × input tests for continuity.
- Property: no delta mutates a book outside Live.
- Property: terminal listing state cannot be resurrected by a late event.
- Replay of existing fixtures produces unchanged output before/after refactor.
- Small finite state properties are model-checked when a clear proof obligation exists.
