# Software assurance and financial-market engineering baseline

**Status:** Integrated engineering baseline. This directory defines how the repository is designed, reviewed, verified and released.
**Baseline commit:** `3da31e84cb883c78c2104c839d55a718c3891088` (`main`, 2026-11-02; local audit re-run 2026-11-05).
**Architecture charter:** [`docs/design/event-market-platform-v1/`](../design/event-market-platform-v1/README.md).
**Product direction:** B2B market data, continuity, historical records and replay for professional prediction-market firms.
**Current product boundary:** market-data collection, continuity, recording and replay. Research follows data quality. Risk, order management, market making and customer execution require separate gates.
**Open gates:** JH-4 storage correctness, operational soak, and every live-execution gate remain open.

## 1. Scope and limits

This baseline adapts selected practices from NASA software engineering, NIST secure development, CPMI-IOSCO PFMI, SEC, SEBI, CFTC and FIX sources to a Rust market-data and trading platform. The source documents have different legal and organisational scopes.

The baseline does not claim NASA certification, safety-critical classification, regulatory compliance, exchange membership, production durability, feed completeness or live-trading readiness. Legal applicability for a venue, product, customer class or jurisdiction requires qualified review.

The framework is additive. Historical verification records, commit messages, fixtures, licenses and protocol payloads retain their original wording. Corrections to factual errors use dated amendments rather than silent rewrites.

## 2. Contents

| Document | Purpose |
|---|---|
| [`assurance-programme.md`](assurance-programme.md) | Lifecycle, assurance tiers, traceability, evidence, review and release policy |
| [`rust-engineering-standard.md`](rust-engineering-standard.md) | Rust correctness, safety, concurrency, persistence, API and dependency rules |
| [`financial-market-controls.md`](financial-market-controls.md) | Market data, research, risk, OMS, market making, ledger, custody, tenant and compliance controls |
| [`technical-writing-and-terminology.md`](technical-writing-and-terminology.md) | Prose policy, requirement sentence structure, first-person policy and canonical glossary |
| [`execution-plan.md`](execution-plan.md) | Sequenced work packages, dependencies, exit criteria and agent instructions |
| [`standards-reading-register.md`](standards-reading-register.md) | Primary-source register, applicability boundaries and review status |
| [`audit-report-format.md`](audit-report-format.md) | Report schema, classification vocabulary, suppression policy and baseline procedure |
| [`repository-baseline-and-hygiene-2026-11-02.md`](repository-baseline-and-hygiene-2026-11-02.md) | Historical branch, CI, protection, tag and release audit |
| [`project-language-audit-2026-11-02.md`](project-language-audit-2026-11-02.md) | Historical remote language audit at the baseline commit |
| [`verification-2026-11-02-pack.md`](verification-2026-11-02-pack.md) | Historical verification of the assurance pack before integration |
| [`verification-2026-11-05-integration.md`](verification-2026-11-05-integration.md) | Local integration verification and baseline disposition |
| [`../verification-2026-11-05-jh4-short-writes.md`](../verification-2026-11-05-jh4-short-writes.md) | JH-4 short and partial-write evidence |
| [`../verification-2026-11-05-jh4-flush-sync.md`](../verification-2026-11-05-jh4-flush-sync.md) | JH-4 flush and sync failure evidence |
| [`../verification-2026-11-05-jh4-recovery.md`](../verification-2026-11-05-jh4-recovery.md) | JH-4 recovery, crash and metadata failure evidence |
| [`../verification-2026-11-05-jh4-enospc-report.md`](../verification-2026-11-05-jh4-enospc-report.md) | JH-4 directory failure, ENOSPC and report agreement evidence |
| [`reports/english-audit-2026-11-05-main-3da31e8.json`](reports/english-audit-2026-11-05-main-3da31e8.json) | Machine-readable baseline audit |
| [`reports/project-language-audit-2026-11-02.json`](reports/project-language-audit-2026-11-02.json) | Historical machine-readable remote audit metadata |
| [`ci/quality-gate.proposed.yml`](ci/quality-gate.proposed.yml) | Proposed non-blocking CI job; not installed |

The audit tooling is in [`scripts/assurance/`](../../scripts/assurance/). The glossary is section 8 of the terminology standard.

## 3. Required engineering sequence

1. Define the requirement, owner, acceptance evidence and failure model.
2. Record architecture decisions that affect persistent formats, financial semantics, security boundaries or public interfaces.
3. Implement the smallest reviewed change with typed errors, explicit state transitions and bounded resources.
4. Add deterministic tests, fault injection and verification evidence before claiming the behavior exists.
5. Record the exact commit, configuration and gate results.
6. Release only after the applicable gates in the execution plan pass.

A component does not receive production approval because it compiles, because unit tests pass, or because a mock test completes. No live order route, signing key or customer capital permission is authorized by this baseline.

## 4. Run the language audit

The non-blocking command scans tracked prose and comments, writes a JSON report, and exits zero when review candidates are present:

```bash
scripts/assurance/run_audit.sh
scripts/assurance/run_audit.sh --history
```

Use an explicit output path when the default `build/assurance/english-audit.json` is not suitable:

```bash
scripts/assurance/run_audit.sh build/assurance/baseline.json
```

Run the scanner directly for strict tautology enforcement or a commit range:

```bash
python3 scripts/assurance/english_audit.py --repo . --strict \
  --exclude 'docs/assurance/reports/*.json'
python3 scripts/assurance/english_audit.py --repo . \
  --history-range origin/main..HEAD
```

Run the scanner tests:

```bash
python3 -m unittest discover -s scripts/assurance -p 'test_*.py' -v
```

The scanner reads the complete `git ls-files` set rather than a fixed extension list. It scans UTF-8 text and reports binary or unreadable files separately. Findings are candidates for human review; the tool has no automatic rewrite operation. See [`audit-report-format.md`](audit-report-format.md) for the report schema and classification rules.

## 5. Current baseline result

The local audit at `3da31e8` scanned 133 tracked text files and all 60 reachable commit messages. It produced one confirmed defect and 292 review candidates:

- One tautology in `src/orderbook.rs:307`. The behavior-neutral correction lands in a separate PR.
- Five promotional-vocabulary candidates retained as technical statements about bounded retries, blocked writers, a rejected format alternative and clock-synchronization limits.
- No first-person pronouns in authored prose, source comments or visible diagram labels.
- No intent adverbs.
- Review candidates in the absolute-guarantee and vague-quantifier categories remain visible. They are not authorization for bulk replacement.

The exact counts and dispositions are in [`verification-2026-11-05-integration.md`](verification-2026-11-05-integration.md) and the machine-readable report.

## 6. Change discipline

- One behavioral objective per pull request.
- Short-lived topic branches; one protected `main`; squash merge; linear history for new changes.
- New requirements identify an actor, action, condition, observable result and verification path.
- New correctness claims cite a proof, invariant, test or verification record.
- New performance claims identify hardware, workload, measurement method, sample size and result.
- New financial claims identify the unit, asset, time basis, fee treatment and accounting convention.
- New commits use imperative subjects and third-person engineering language.
- Historical evidence and failed CI runs are preserved.

## 7. Source frameworks

The following sources are references, not claims of adoption or compliance:

- NASA-HDBK-2203 Version D: <https://standards.nasa.gov/node/182>
- NPR 7150.2D: <https://swehb.nasa.gov/spaces/SITE/pages/123601159/NPR%2B7150.2D>
- NASA-STD-8739.8B: <https://standards.nasa.gov/standard/NASA/NASA-STD-87398>
- NASA SWE-061: <https://swehb.nasa.gov/spaces/7150/pages/16450283/SWE-061%2B-%2BCoding%2BStandards>
- NASA SWE-135: <https://swehb.nasa.gov/spaces/7150/pages/16450005/SWE-135%2B-%2BStatic%2BAnalysis>
- NASA SWE-062: <https://swehb.nasa.gov/spaces/SWEHBVD/pages/102695446/SWE-062%2B-%2BUnit%2BTest>
- NIST SP 800-218 SSDF: <https://csrc.nist.gov/pubs/sp/800/218/final>
- NIST SP 800-161 Rev. 1 Update 1: <https://csrc.nist.gov/pubs/sp/800/161/r1/upd1/final>
- CPMI-IOSCO PFMI: <https://www.bis.org/committees/cpmi/pfmi/overview>
- SR-26-2 model-risk guidance: <https://www.federalreserve.gov/frrs/guidance/supervisory-guidance-on-model-risk-management.htm>
- SEC Rule 15c3-5: <https://www.ecfr.gov/current/title-17/section-240.15c3-5>
- SEBI retail algorithmic-trading circular: <https://www.sebi.gov.in/legal/circulars/feb-2025/safer-participation-of-retail-investors-in-algorithmic-trading_91614.html>
- FIX Protocol: <https://fixtrading.org/standards/fix-protocol/>
- JPL, The Power of 10: <https://spinroot.com/gerard/pdf/Power_of_Ten.pdf>

NASA documents govern NASA work. NIST publications are frameworks. PFMI applies to entities within its scope. SEC, SEBI and CFTC rules apply to covered activities and entities. Applicability requires legal and compliance review for each deployment.
