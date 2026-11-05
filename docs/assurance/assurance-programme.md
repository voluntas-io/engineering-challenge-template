# Software Assurance Programme

## 1. Authority and scope

This document defines the project software-assurance process. It adapts selected practices from NASA-HDBK-2203, NPR 7150.2D, NASA-STD-8739.8B, NIST SP 800-218 and NIST SP 800-161. It is not a claim of compliance with those frameworks. NASA's software classes and assurance obligations must not be copied mechanically: the project must classify components by consequence, exposure, and authority over market data or customer assets.

The project has one product direction: B2B market-data and trading infrastructure for professional prediction-market firms. The first product provides live data, continuity status, historical records, and replay. Research, risk, order management, market making, and customer execution have separate release gates.

## 2. Assurance principles

1. **Requirements precede implementation.** Each behavior with operational, security, financial, or compatibility impact has a unique requirement identifier and an acceptance test.
2. **Design precedes authority.** A component receives no authority over orders, funds, credentials, or tenant data until its boundary and failure behavior have an approved design.
3. **Verification precedes claims.** A claim must cite a reproducible test, analysis, measurement, or externally authoritative source.
4. **Evidence is immutable.** Verification records identify commit SHA, toolchain, configuration, data, environment, command, result, and deviations.
5. **Unknown state blocks new exposure.** Unknown market, order, account, risk, or settlement state cannot authorize additional financial exposure.
6. **Failure is a designed state.** Storage, network, venue, process, clock, dependency, and recovery failures have explicit state transitions and operator actions.
7. **Independent controls remain independent.** Strategy code cannot edit risk limits, access signing keys, or bypass the order gateway.
8. **No silent data loss.** Queue saturation, rejected frames, write errors, sequence uncertainty, and incomplete recovery are observable and attributable.
9. **Reproducibility is a release property.** A release must identify its source tree, dependencies, toolchain, build inputs, test evidence and artifacts.
10. **Maintenance is part of design.** Every subsystem has an owner, contract, tests, runbook, compatibility policy, and retirement path.

## 3. Assurance tiers

Tiers describe required controls, not regulatory classifications.

| Tier | Component / activity | Authority and consequence | Minimum evidence |
|---|---|---|---|
| A0 | Offline research, documentation, developer tooling | No live venue or customer authority | Unit tests, reproducible inputs, review, lint |
| A1 | Public market-data ingestion and historical capture | Produces data relied on by downstream research | Protocol fixtures, decoder tests, continuity tests, journal fault tests, replay checks, soak evidence |
| A2 | Research and backtesting | Produces analytical results and strategy hypotheses | Dataset provenance, time-split controls, fee/fill assumptions, reproducibility, leakage tests |
| A3 | Paper OMS, simulated risk, simulated ledger | Models financial workflows without live order authority | State-machine tests, idempotency, invariant checks, reconciliation, crash recovery |
| A4 | Live execution, signing, customer account access, custody, settlement | Can create financial obligations or affect customer assets | Independent risk review, threat model, key custody review, dual control, operational readiness, legal/compliance approval, canary approval, reconciliation evidence |

The current collector is primarily A1. Current evidence does not establish A1 production readiness. A3 and A4 are not implemented and remain unauthorized.

## 4. Lifecycle and traceability

Every material requirement must use this chain:

`Need -> Requirement -> Hazard/Threat/Risk -> Architecture decision -> Interface/Invariant -> Implementation -> Test -> Verification record -> Operational metric/runbook -> Release disposition`.

Required identifiers:

- `REQ-*`: functional and non-functional requirement.
- `HAZ-*`: hazard or loss scenario.
- `THR-*`: threat scenario.
- `ADR-*`: architecture decision.
- `INV-*`: invariant.
- `TEST-*`: verification case.
- `OPS-*`: operational control or runbook.
- `REL-*`: release gate.

A requirement record must state:

- Identifier and concise title.
- Rationale and source.
- Owner.
- Assurance tier.
- Preconditions and triggering conditions.
- Required behavior and prohibited behavior.
- Observable acceptance criteria.
- Failure behavior.
- Linked ADR, source module, tests and operational evidence.
- Status: proposed, accepted, implemented, verified, waived, superseded.

An unverified requirement cannot be marked verified. A waiver identifies scope, rationale, approving authority, compensating control, expiry, and review date.

## 5. Architecture and design review

Require an ADR when a change affects:

- Persistent journal, replay, database, ledger or wire format.
- Financial amount, price, fee, collateral, position, risk, or settlement semantics.
- Authentication, authorization, tenant isolation, credential handling, signing, or custody.
- Process boundaries, concurrency ownership, failure containment, or data authority.
- Venue protocol compatibility or public API compatibility.
- Recovery, durability, retention, data deletion, or audit evidence.

A design review answers:

1. What problem and requirement does the change satisfy?
2. Which invariants must hold before and after the change?
3. Which component owns state and authority?
4. What inputs are trusted, untrusted, or partially trusted?
5. What can fail, and how is each failure observed?
6. What happens after process termination at each persistence boundary?
7. What is the retry and idempotency contract?
8. What is the compatibility and migration strategy?
9. Which tests prove the behavior?
10. What operational evidence and rollback are required?

## 6. Verification strategy

Use multiple independent verification methods. A passing unit test does not replace integration, fault, property, fuzz, security, performance, or operational evidence. The JPL Power of 10 principles may inform bounded control flow and analyzability, but the original paper targets C; Rust adaptations must be justified against Rust ownership, type, concurrency and async semantics.

| Verification method | Purpose | Project examples |
|---|---|---|
| Formatting and lint | Consistent syntax and static diagnostics | `cargo fmt`, Clippy with `-D warnings` |
| Unit tests | Local behavior | Parser, fixed arithmetic, state transition |
| Integration tests | Component contracts | Mock venue, journal, replay, SBE output |
| Property tests | Invariants over input domains | Book ordering, fixed arithmetic, ledger balance |
| Fuzz tests | Adversarial parser and decoder input | WebSocket payloads, JSON, SBE, manifests |
| Model/concurrency tests | Interleavings and state space | Loom for synchronization primitives where tractable |
| Dynamic analysis | Runtime undefined behavior / memory checks | Miri for supported unsafe paths |
| Fault injection | Failure transitions | Short writes, sync errors, queue saturation, crash recovery |
| Differential tests | Compare independent implementations or venue state | Native decoder vs fixture/reference parser; local book vs authoritative snapshot |
| Performance tests | Resource and latency distributions | Sustained and burst workloads with pinned environment |
| Security tests | Threat mitigations | Secret scan, dependency audit, authorization tests |
| Operational exercises | Recovery and operator procedure | Disk exhaustion, restart, backup restore, live soak |

Tests must be repeatable. Record seeds, fixtures, versions and environment where they affect results.

## 7. Rust controls

The detailed Rust standard is in `rust-engineering-standard.md`. Minimum mandatory controls:

- `unsafe` is prohibited by default; every approved block has a safety contract and tests.
- Financial arithmetic uses checked fixed-point or integer representations with explicit units and rounding.
- Identifiers with different domains use distinct types.
- Externally supplied input is validated at the boundary.
- Runtime errors are typed and propagated; no silent fallback that changes financial or data semantics.
- All queues, buffers, retries, tasks, and shutdown waits have bounds.
- Concurrency invariants are documented and tested.
- No detached task may own financial, persistence, or lifecycle authority.
- Serialization formats are versioned and compatibility-tested.
- Dependency changes include provenance, license, security, and maintenance review.

## 8. Change review and pull requests

One PR should have one primary behavioral objective. A PR description contains:

- Requirement identifiers.
- Scope and explicit non-goals.
- Design/ADR references.
- Invariants and failure analysis.
- Tests added or changed.
- Commands and exact results.
- Compatibility and migration impact.
- Security and financial impact.
- Operational and documentation changes.
- Rollback or disablement path.

The review must inspect the complete diff, generated artifacts, test names, ignored tests, changed dependencies, workflow permissions, and documentation claims. A green CI status is necessary but not sufficient.

## 9. Release evidence

Every release candidate must identify:

- Exact source commit and clean-tree state.
- Rust toolchain, target, features and build profile.
- Lockfile and dependency audit result.
- SBOM and build provenance.
- Test reports and ignored-test dispositions.
- Static-analysis results.
- Schema and migration compatibility.
- Known limitations and open risks.
- Rollback plan.
- Named approval by the component owner and release authority.

Release status is one of: `blocked`, `development`, `verification-pending`, `release-candidate`, `approved`, `withdrawn`. Do not infer approval from a tag or successful build.

## 10. Documentation authority

Maintain one authoritative definition for each concept. Charter documents define destination architecture and invariants. Roadmaps define implementation order. Runbooks define operator actions. Verification records preserve historical evidence. README files summarize and link to authoritative documents; they do not redefine contracts.

Historical documents are append-only evidence. Correct them with a dated correction or superseding record. Do not silently rewrite a past result to reflect the state observed after that result.

## 11. Assurance review cadence

- Every PR: requirement links, code review, tests, documentation, dependency and security impact.
- Every release: complete release evidence and unresolved-risk review.
- After each incident: causal analysis, detection gap, containment gap, corrective action and regression test.
- At least quarterly once operating continuously: dependency, threat, protocol, backup, access and recovery review.
- On venue protocol or regulatory change: impact assessment before deployment.

## 12. Non-claims

This programme cannot establish perpetual security. It establishes mechanisms to detect drift, prove selected properties, contain failures, recover from known faults, and revise controls as venues, threats, dependencies, business models, and laws change.
