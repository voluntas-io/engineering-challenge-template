# Engineering roadmap and acceptance gates

## Operating rule

One behavioral objective per PR. Each PR includes a regression test, source-pinned verification note, updated architecture/test inventory and explicit non-goals. Do not combine new protocol semantics, storage migration and new trading behavior in one change.

The [assurance baseline](../../assurance/README.md) applies to every phase: requirements and evidence precede implementation, Rust correctness rules are mandatory, financial controls are explicit, and verification records pin the reviewed commit. A phase gate is not satisfied by a successful build alone.

## Phase 0 — Land and freeze the current collector

**Objective:** move the integration baseline to `main` without conflating independent behavior.

Current source refs to review:

- `0235b103` — Polymarket feed-contract fixtures and decoder tests.
- `ce61a34e`, `36a0cfff`, `74140c48`, `ce6f2433`, `13bc7e0d` — continuity/failover and clockless recovery fencing.
- `4362283d` plus journal hardening commits through `5924eb6` — raw journal and failure/shutdown contracts.

Suggested PR stack:

1. Feed contract and golden fixtures. Verify official source, parser atomicity, unknown event handling and schema docs.
2. Continuity/recovery state machine. Review all source-state transitions and clockless receive fencing.
3. Raw journal. Review capture boundary, append semantics, manifest, verifier and failure handling.
4. Journal follow-up hardening is already merged into the integration branch via PR #2; preserve its evidence and source history.

The exact cherry-pick/stack arrangement must respect commit dependencies. Do not cherry-pick only a leaf commit without its required parent changes. If GitHub review cannot accept the stack cleanly, create integration PRs in dependency order. The integration branch currently sits 16 commits ahead of `main`; PR #2 merged only into that branch.

**Exit gate:** current branch code is on `main`; CI passes on the exact merge commit; docs identify the correct baseline; no unsupported capacity/live claims remain.

## Phase 1 — Data contract foundation

**PR 1A: typed instrument and listing model**

- Introduce Event, Outcome, Claim and VenueListing IDs.
- Add immutable listing revisions, identifier scheme and multidimensional protocol-version tuple.
- Carry Polymarket market identifier scheme, CLOB API, exchange contract and collateral as distinct fields.
- Carry Kalshi full price ranges, fee overrides, settlement-source references and native yes/no quote values.
- Add minimum size, tick grid, lifecycle, fee schedule and collateral metadata with explicit Unknown values.
- Do not wire into order execution; compile and test catalog/inspection/replay only.

**Acceptance:** fixtures for old/current protocol forms, revision update tests, unknown metadata fail-closed tests, historical listing revision retained, no schema silently changes old meaning.

**PR 1B: native and canonical quote model**

- Keep raw venue side/price/quantity and canonical claim-side derivation together.
- Version fixed-point decimal semantics and units.
- Test Kalshi NO-side transformation and Polymarket CTF/position unit distinctions.
- Test tick-band boundary and quantity increment constraints.

**Acceptance:** no float arithmetic in book/order/fee/ledger types; quote conversion reversible or explicitly non-reversible with reason; no native value loss.

**PR 1C: outcome graph**

- Add mutually-exclusive, exhaustive, complement and cross-venue relation records.
- Require evidence and validity interval.
- Enforce sum-to-one constraints only for sets proven exhaustive and exclusive under compatible payoff/collateral rules.

**Acceptance:** multi-outcome market fixtures; property tests that do not infer complementarity from labels alone.

## Phase 2 — Unified input log and replay

**PR 2A: core-input log schema**

- Define envelope, schema version, offsets, causality references and `CoreInput` variants.
- Capture initial and periodic discovery, source status, recovery acknowledgement, timer tick, operator action and shutdown input.
- Record every raw frame reference and decoder result.
- Preserve legacy `.pmj` read support.

**PR 2B: deterministic core runner**

- Define `Core::step` with no I/O or clocks.
- Make coordinator an adapter from live effects/events to logged `CoreInput`.
- Add `RecoveryAcknowledged` to normalized replay format.
- Preserve connection epoch and receive sequence in replay.

**PR 2C: raw-decode replay**

- Add `journal replay --mode raw-decode` or a separate `redecode` command.
- Apply pinned decoder versions to archived frames.
- Link canonical events to raw IDs and event ordinals.

**PR 2D: equality CI test**

- Run mock exchange; record raw and core-input logs; replay core inputs; compare output hashes/bytes.
- Re-decode raw log with pinned decoder; compare canonical stream.
- Fault-inject process crash between log append, reducer step and output append.

**Acceptance:** both replay modes are deterministic; no missing core input type; byte comparison is well-defined; crash recovery regenerates outputs; exact manifests and config hashes are pinned.

## Phase 3 — Continuity reducer and code decomposition

**PR 3A:** pure exhaustive continuity transition function and table tests.  
**PR 3B:** property-based invariants and model-checking proof for the selected finite state properties.  
**PR 3C:** split `collector/state.rs` by bounded domain responsibility: source selection, continuity, recovery, discovery/sharding, book projection and scheduling.

Do not move everything at once. First extract functions without semantic change, then migrate one state machine per PR.

**Acceptance:** no direct unvalidated continuity assignment; each transition has a unit/property test; core replay output unchanged for existing fixtures.

## Phase 4 — Operational evidence and durable archive

**PR 4A:** journal storage-full, partial write, permission, fsync and restart-recovery semantics.  
**PR 4B:** segment retention, compression, indexes, disk watermarks and recovery tooling.  
**PR 4C:** protocol drift alarms using unknown/rejected event rates with per-adapter baselines.  
**PR 4D:** completeness certificate v1 for a real capture window.

Live validation, soak and storage exhaustion are separate operational tasks, not substitutes for code tests:

- Kalshi live feed validation only with authorized account/credentials and within the venue's access rules.
- 24-hour soak with a predeclared fault schedule: network loss, reconnect storms, slow disk, full disk, process termination, clock step, partial segment, permission failure and venue protocol unknown event.
- Physical disk exhaustion on an isolated test volume, with no production path to that volume.
- Post-run manifest verification, restart recovery and independent completeness report.

**Acceptance:** run reports pin commit/config/machine/fault schedule; no unaccounted accepted event; known outages identified; unquantifiable Polymarket gaps remain labeled unquantifiable.

## Phase 5 — Historical research and simulation

- Historical partitions from raw and canonical logs.
- Index by tenant, venue, event, outcome, listing, time and protocol version.
- Feature pipelines with provenance links.
- Fee-aware, collateral-aware simulator.
- Conservative queue-position/fill probability model; latency, rate limits, rejects, partial fills, cancels and adverse selection.
- Chronological train/test split, frozen holdout, baseline models and reproducible run manifest.

**Acceptance:** stranger reproduces dataset and report from data manifest + code/config hashes; reported PnL is net of fees/slippage/capital lock-up assumptions; sensitivity ranges displayed; no point estimate masquerades as exact queue position.

## Phase 6 — Paper execution foundations

Separate PRs:

1. Intent and risk-decision schema.
2. Risk reservation engine and independent kill switch.
3. OMS idempotency and ambiguity state machine.
4. Venue order/query/cancel adapter contracts.
5. Paper execution adapter using the same OMS reducer.
6. Double-entry ledger and position projection.
7. Account/order/fill/balance reconciliation.
8. Resolution and settlement state machine.
9. Signer interface and custody controls (no live route until independently approved).

**Acceptance:** fault-injected timeout after submit never duplicates exposure; reservations are atomic; every order has a risk decision; ledger balances per asset; reconciliation mismatch blocks risk; paper mode has no live endpoint/key.

## Phase 7 — Business shape and platform controls

Before external users or Phase 8:

- Choose primary business shape: own-account, market making, or platform/data product. Shared foundation stays the same; risk and operating budgets differ.
- Define tenancy, retention, API compatibility, quotas, encryption and audit.
- Decide supported account and venue combinations under qualified legal/compliance review.
- Define platform SLOs and completeness certificate service contract.

## Phase 8 — Restricted canary (separately authorized)

No canary is authorized by this document. A future approval requires all of:

- Named account and eligible venue access.
- Written legal/compliance authorization.
- Strategy and market allowlist.
- Independent risk gate and atomic reservations.
- Signer/key custody review and recovery drills.
- Hard capital, per-event and per-market caps.
- Kill-switch test and order/cancel timeout fault injection.
- Continuous account/order/position/settlement reconciliation.
- Human approval and session close review.

Any unresolved data, order, account, ledger or eligibility state halts new risk.

## Phase 9 — Venue three

The third venue is the architecture exam. It should require:

- New adapter, raw fixtures and venue configuration.
- No changes to `CollectorCore`, economic claim model, risk reducer, OMS state machine or ledger contract.
- Adapter contract tests and a new compatibility report.

If it requires a shared-core change, document which abstraction failed and fix the abstraction before adding venue-specific conditionals.

## PR definition of done

- One behavioral objective.
- Exact source commit and config digest.
- Regression tests and protocol golden fixtures where relevant.
- Formatting, Clippy, test, doc test, release build and architecture CI pass on exact PR head.
- Verification note records commands and actual results.
- All unknowns and operational limitations retained in docs.
- No live keys/routes unless the phase explicitly authorizes them.
