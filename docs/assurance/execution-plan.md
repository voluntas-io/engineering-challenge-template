# Assurance and Product Execution Plan

## Business decision

Position the company as a B2B software provider for professional prediction-market firms. The first product is live market data, continuity status, historical records and replay. Research is the second product. Risk and execution require separate approval gates. Multi-tenant services follow only after the product and legal perimeter are defined.

This is a business hypothesis until customer interviews, design-partner tests, data-rights analysis, willingness-to-pay evidence and retention validate it. Revenue estimates in the uploaded founder strategy are illustrative scenarios, not forecasts.

## Baseline

- Repository: `kj111422/engineering-challenge-template`
- Observed `main` baseline: `3da31e84cb883c78c2104c839d55a718c3891088` (2026-11-02, PR #4 merged)
- Frozen architecture charter: `df87cb1c207a06e87e29a2cae8cd3b77e110a67e`
- Current source product: Rust market-data collector with venue adapters, order-book reconstruction, continuity, SBE output, raw-frame journal and normalized replay.
- Not implemented: raw-frame-to-core replay, complete unified CoreInput log, typed economic instrument model, research/backtesting, risk engine, OMS, signer, double-entry ledger, settlement, tenant identity or commercial control plane.
- Open operational gate: JH-4 short-write injection, physical storage exhaustion and process-restart-after-crash testing.
- No live order transmission is authorized.

## Dependency graph

```text
G0 Repository and evidence baseline
  -> G1 Engineering standard, glossary, requirements traceability
  -> G2 English audit and documentation reconciliation
  -> G3 JH-4 journal storage correctness
  -> G4 Unified CoreInput log and deterministic replay
  -> G5 Market-data completeness and operational soak
  -> G6 Data product pilot and customer validation
  -> G7 Historical research and backtesting
  -> G8 Paper risk / OMS / ledger / reconciliation
  -> G9 Restricted live execution decision
  -> G10 Tenant platform and customer execution
```

Customer discovery runs in parallel with engineering because interviews and data-rights review do not require execution authority. No engineering gate depends on customer discovery being complete, and no customer contract should promise features before their release gates pass.

## Work packages

### G0 — Baseline and repository hygiene

Verified snapshot (2026-11-02):
- `main` = `3da31e84cb883c78c2104c839d55a718c3891088`.
- Branch API reports `main` protected; `polymarket-continuity-failure` and `journal-failure-hardening` unprotected.
- `polymarket-continuity-failure` = `275f78f`; `main` is 2 commits ahead and 0 behind.
- `journal-failure-hardening` = `5924eb6`; `main` is 5 commits ahead and 0 behind.
- No repository rulesets are configured. The branch-protection detail endpoint is inaccessible to the connected GitHub integration; verify exact rules in repository settings.
- No tag refs and no GitHub Releases were returned.
- No open PRs. PR #4 merged into `main`; PR #1 is closed without merge; PR #2 was merged earlier.
- The tracked tree contains 133 blobs: 38 Markdown files and 45 Rust files.
- CI run `41468148370` and Architecture run `41468148207` passed on the exact `main` SHA. FOSSA License Compliance, Dependency Quality and Security Analysis statuses are success.
- `ci.yml` and `architecture.yml` already target pushes and PRs to `main`, support manual dispatch, use `ubuntu-24.04`, have concurrency groups, least-privilege `contents: read`, pinned action SHAs and bounded job timeouts.
- `live.yml` is manual-only and enables public-feed integration tests using `RUN_LIVE_TESTS=1`; retain this separate opt-in workflow.
- Historical merge commits remain in the Git graph. Do not rewrite published history; enforce linear history for future changes.

Status update (2026-11-05): the branch cleanup, protection review, FOSSA confirmation and tag inventory are complete. Only `main` remains. The exact protection rule requires the Rust CI, Architecture CI and FOSSA contexts, strict up-to-date checks, linear history, conversation resolution, no force pushes and no deletion; repository merges are squash-only with automatic head-branch deletion. No tags or releases exist. The production roadmap now separates its historical 2026-10-26 baseline from the current verified baseline.

Tasks:
- [x] In the local checkout, confirm `HEAD`, `origin/main`, clean/dirty tree and untracked files.
- [x] Recheck the three remote refs immediately before deleting stale branches.
- [x] Review branch-protection settings in GitHub UI: required PR, required CI and Architecture checks, linear history, no force-push, no deletion, conversation resolution, squash-only merge.
- [x] Confirm any external FOSSA checks are reliable on PR events before requiring them.
- [x] Confirm no open PR references either stale branch.
- [x] Delete the two obsolete branches after local ancestry confirmation; retain no archive branch unless an explicit owner needs it.
- [x] Keep the tag namespace empty until a reviewed release; then add annotated version tags and protect `v*`.
- [x] Preserve historical workflow runs and verification records.
- [x] Correct the roadmap's old baseline label by distinguishing its 2026-10-26 baseline snapshot from the current 2026-11-02 `main` SHA. Do not rewrite its historical measurements.
- [x] Do not rewrite historical commits or verification records.

Exit: one permanent development branch (`main`), short-lived topic branches, required checks and branch rules verified, tag/release policy documented, stale historical baselines labelled, and exact refs/CI/evidence captured.

### G1 — Assurance, requirements and terminology

Tasks:
- [x] Add this assurance programme through a documentation-only PR.
- [ ] Approve assurance tiers A0-A4 or replace them with a reviewed project model.
- [ ] Add requirement, hazard, threat, invariant, test, operation and release IDs.
- [ ] Build a requirement-to-test matrix for the existing collector.
- [ ] Identify every public/serialized contract.
- [ ] Approve canonical glossary terms.
- [ ] Assign document authorities and owners.
- [ ] Define documentation corrections without changing historical evidence.

Exit: requirements have owners and verification routes; terminology collisions that affect code or external contracts have dispositions.

### G2 — English and tracked-tree audit

Tasks:
- [x] Copy `scripts/assurance/english_audit.py` into the repository.
- [x] Add the unit tests and test edge cases.
- [x] Run against all tracked text files, not a hard-coded extension list.
- [x] Run against all Git commit subjects/bodies reachable from local refs.
- [x] Save baseline JSON report.
- [x] Classify every finding as confirmed defect, valid technical use, quoted source, historical evidence, false positive or open review.
- [ ] Correct confirmed tautologies and unsupported claims.
- [x] Rewrite normative first-person prose and new commit messages; the baseline found no authored first-person findings.
- [x] Do not mutate license text, protocol fixtures, quotations or historical evidence.
- [x] Verify Markdown links and cross-document definitions.
- [ ] Add a non-blocking CI report first.
- [ ] Review all findings and suppressions.
- [ ] Correct confirmed defects and pin the accepted baseline report.
- [ ] Enable strict CI enforcement only for high-confidence tautologies after baseline review and exception policy approval.

Exit: every candidate is dispositioned or assigned; no unreviewed blanket replacement; future regressions are detectable. The initial audit job reports findings without blocking unrelated work. A subsequent change promotes only reviewed high-confidence rules to required status checks.

### G3 — JH-4 storage correctness

Tasks:
- [ ] Add a deterministic partial-write test double.
- [ ] Test writes returning short counts until completion.
- [ ] Test write errors before any bytes and after partial bytes.
- [ ] Test failed flush and sync.
- [ ] Test directory and manifest metadata failure.
- [ ] Test rotation failure and rename failure.
- [ ] Test queue saturation followed by ordinary shutdown.
- [ ] Test concurrent producer failure latching.
- [ ] Test kill/crash at record write, segment rotation and manifest update boundaries.
- [ ] Restart a child process and verify incomplete-run disposition.
- [ ] Test recovery idempotency.
- [ ] Exercise quota/filesystem capacity exhaustion on an isolated volume.
- [ ] State precisely which acknowledged records survive each failure boundary.
- [ ] Verify report, manifest, segment counts and verifier disposition agree.

Exit: short writes, write/sync/rotation/manifest failures, physical disk exhaustion, crash/restart and recovery have deterministic tests and evidence. A failed run cannot be finalized or reported complete.

### G4 — Unified input log and replay

Tasks:
- [ ] Define `CoreInput` as the complete input alphabet for the deterministic core.
- [ ] Include discovery, source payload result, status, timer, recovery acknowledgement, operator command, shutdown and effect-result inputs where they affect core state.
- [ ] Preserve raw ingress independently of normalized CoreInput.
- [ ] Preserve event time, receive wall time, receive monotonic time, connection epoch, receive sequence and core processing offset separately.
- [ ] Pin decoder version, configuration digest, schema versions and binary build identity.
- [ ] Implement a raw-frame re-decode runner.
- [ ] Implement deterministic CoreInput replay.
- [ ] Define non-deterministic effects and their recorded outcomes.
- [ ] Compare snapshots, state hashes, events and SBE output under exact matching conditions.
- [ ] Add corruption, missing-input, duplicate-input and version-mismatch tests.

Exit: deterministic replay reproduces declared outputs from complete recorded inputs; non-reproducible dependencies are explicit.

### G5 — Data quality and operations

Tasks:
- [ ] Define data freshness and continuity SLIs.
- [ ] Define the scope of feed completeness claims.
- [ ] Add independent snapshot comparison for selected markets.
- [ ] Separate exchange event time from receive time and processing order.
- [ ] Run reconnect and source failover drills.
- [ ] Run a 24-hour or longer predeclared soak with pinned commit/configuration.
- [ ] Record CPU, RSS, disk growth, write latency, journal lag, receive rate, book age, recovery time and discontinuities.
- [ ] Run storage exhaustion on an isolated filesystem.
- [ ] Run process restart and backup restoration drills.
- [ ] Review alerting and runbooks with an operator who did not implement the feature.

Exit: measured capacity, recovery and failure evidence supports a defined pilot operating envelope. No unsupported full-universe or event-completeness claim.

### G6 — Commercial data product

Customer discovery tasks (parallel with G1-G5):
- [ ] Define target customer profile: professional market makers, quantitative teams, proprietary trading firms and prediction-market operators.
- [ ] Conduct structured interviews; label target counts as targets until completed.
- [ ] Ask customers to demonstrate feed maintenance, incident response, historical reconstruction and internal costs.
- [ ] Validate willingness to test a read-only product.
- [ ] Obtain data licensing and venue redistribution review.
- [ ] Define one pilot use case and product boundary.
- [ ] Validate price hypotheses through paid pilots rather than treating illustrative ranges as market facts.
- [ ] Define API and service objectives from actual customer needs.
- [ ] Create customer support and incident communication procedures.

Engineering tasks:
- [ ] Define public data API contract.
- [ ] Define schema evolution and customer compatibility policy.
- [ ] Implement tenant identity only when customer boundary requires it.
- [ ] Implement authorization and rate limits before external access.
- [ ] Add usage measurement and billing integration after product/contract design.

Exit: pilot customers have used the product and a customer has paid or supplied written commercial evidence; data rights and support commitments are documented.

### G7 — Research and backtesting

- [ ] Build time-indexed historical data from validated recordings.
- [ ] Preserve metadata versions and discontinuity intervals.
- [ ] Implement market-quality features before predictive modeling.
- [ ] Model fees, settlement, slippage, queue position, partial fills, cancellations and adverse selection.
- [ ] Enforce chronological train/validation/holdout partitions.
- [ ] Add look-ahead and leakage tests.
- [ ] Generate reports with assumptions and uncertainty.
- [ ] Validate execution simulation against observed venue behavior.

Exit: strategy research is reproducible and net-of-cost assumptions are explicit.

### G8 — Paper risk, OMS and ledger

- [ ] Define order intent and idempotency contract.
- [ ] Define order lifecycle, including ambiguous submission.
- [ ] Implement risk reservation transaction.
- [ ] Implement OMS persistence and venue simulator.
- [ ] Implement account-event ingestion and reconciliation.
- [ ] Implement double-entry ledger with immutable postings.
- [ ] Implement positions, fees, collateral and settlement states.
- [ ] Test duplicate, reordered and missing events.
- [ ] Test crash recovery at every state transition.
- [ ] Keep signer and live venue credentials absent.
- [ ] Run paper mode with real order transmission impossible by construction.

Exit: paper orders, fills, positions and ledger reconcile through restart and fault tests.

### G9 — Restricted live execution decision

This is a separate approval, not an automatic engineering milestone.

- [ ] Obtain legal and compliance determination for each jurisdiction, venue and user type.
- [ ] Review venue terms and eligibility.
- [ ] Complete threat model and independent security review.
- [ ] Implement secure signing boundary and key lifecycle.
- [ ] Require independent pre-trade risk and out-of-process kill switch.
- [ ] Define capital, position, loss, concentration, price, rate and liquidity limits.
- [ ] Define order, fill, balance and settlement reconciliation.
- [ ] Run paper and shadow operation for a declared period.
- [ ] Run incident and recovery exercises.
- [ ] Name release and risk approvers.
- [ ] Approve narrow market/account/size scope.

Exit: written release approval with hard operating limits and emergency stop. Without approval, live order submission remains disabled.

### G10 — Tenant platform

- [ ] Define tenant/user/service identity.
- [ ] Define data and account isolation.
- [ ] Test cross-tenant access and inference.
- [ ] Define API entitlements and rate limits.
- [ ] Define customer data retention, deletion and export.
- [ ] Define billing, support, incident communication and SLA reporting.
- [ ] Add tenant-specific risk controls before any customer order authority.
- [ ] Complete penetration and authorization testing.

Exit: customer isolation and service obligations pass independent review.

## Pull request sequencing

1. Documentation-only: assurance policy and glossary.
2. Tooling-only: English audit scanner and tests.
3. JH-4: one PR per storage failure contract or cohesive test group.
4. CoreInput/replay: schema/ADR first, then implementation PRs.
5. Operational soak: separate evidence record; no feature mixing.
6. Data pilot: data rights, API, auth, customer isolation and support scope.
7. Research, paper execution, live authority and tenant features each receive independent gates.

## Agent operating contract

An automated coding agent MUST:

- Read the charter, relevant ADRs, requirement, source, tests, runbooks and recent verification records before editing.
- Inspect `git status`, branch, exact SHA, remotes, open PRs and current CI.
- Preserve untracked and user-owned changes.
- Do not force-push, reset, or rebase published history; do not push directly to `main`.
- Preserve historical results; record corrections as dated amendments.
- State the invariant before proposing an implementation.
- Add a failing regression test before changing behavior when practical.
- Change one primary behavior per PR.
- Update source, tests, requirements, docs and evidence as a coherent set.
- Run exact gates and report command, exit code, counts, ignored tests, commit SHA and environment.
- Mark each claim as observed, tested, inferred, proposed or unverified.
- Stop on unclear financial semantics, missing venue contract, incomplete authorization, or ambiguous persistence behavior.
- Do not invent API behavior, regulatory applicability, market completeness, live-trading status or production readiness.

## Completion rule

A gate closes only when every acceptance criterion has evidence on the exact reviewed commit. A plan, checklist, green earlier commit or documentation statement does not close a gate.
