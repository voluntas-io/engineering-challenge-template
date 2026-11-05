# Verification record — assurance baseline integration

**Verification date:** 2026-11-05
**Baseline commit:** `3da31e84cb883c78c2104c839d55a718c3891088` (`main`)
**Baseline tree:** `52aecd149b131e98baa9ee3cbbfaa08004227ec4`
**Scope:** Integration of the assurance documents and audit tooling, a local audit of the exact baseline tree, and the documentation and architecture checks available before merge.
**Merge reference:** The merge commit is recorded in the repository history. This record does not contain its own commit SHA.

## 1. Source pack integrity

The source pack's `SHA256SUMS` manifest was verified for all 16 listed files:

```text
shasum -a 256 -c SHA256SUMS
16/16 OK
```

The pack's documentation was integrated with stable repository paths. The unapplied comment patch was not retained; the correction lands in a separate behavior-neutral PR. The historical pack verification record remains unmodified except for a dated integration note.

## 2. Scanner integration

The scanner was integrated at `scripts/assurance/english_audit.py` with tests at `scripts/assurance/test_english_audit.py`.

The integration refines candidate selection so all categories inspect authored prose and comments rather than raw code or markup. The baseline run before this refinement produced 1,237 findings, dominated by Rust `Some` identifiers and SVG attributes. The refined scanner:

- scans Rust line comments, Python, YAML, TOML, shell and service comments;
- scans Markdown prose outside fenced code and removes link destinations, URLs, Mermaid node identifiers and inline code;
- scans only visible `text`, `title` and `desc` content in SVG files;
- scans commit subjects and bodies as prose;
- excludes license text and extensionless legal files;
- leaves code, fixtures and protocol payloads available in the tree without treating their identifiers as prose.

Tests:

```text
python3 -m unittest discover -s scripts/assurance -p 'test_*.py' -v
Ran 16 tests
OK
```

The 11 original tests pass without modification except for the import path. Five tests cover the integration refinements: Rust identifiers, Rust comments, SVG attributes, license text and Markdown inline code.

## 3. Baseline audit

Command:

```bash
python3 scripts/assurance/english_audit.py --repo . --include-history --strict \
  --json-out docs/assurance/reports/english-audit-2026-11-05-main-3da31e8.json
```

The command returned exit status `1` because strict mode found the confirmed tautology. It scanned the exact tracked tree at `3da31e8`; the new documentation and tooling files were untracked at the time of the run.

| Measure | Result |
|---|---:|
| Tracked paths | 133 |
| Scanned text files | 133 |
| Binary files | 0 |
| Unreadable files | 0 |
| Commit messages scanned | 60 |
| Total findings | 293 |
| Tautology severity | 1 |
| Review severity | 292 |
| Findings in existing commit bodies | 10 |
| Identified false positives | 1 |
| Valid technical or explanatory uses | 24 |
| Open review candidates | 257 |

Category counts:

| Category | Findings | Baseline disposition |
|---|---:|---|
| `tautology` | 1 | Confirmed defect at `src/orderbook.rs:307`; correction in a separate PR |
| `promotional_or_banned_candidate` | 5 | Valid technical use: bounded retry, blocked-writer, rejected-alternative and clock-synchronization descriptions |
| `filler` | 11 | Explanatory or historical prose; no technical meaning depends on the flagged word |
| `informal_or_ambiguous_verb` | 8 | Valid technical use: last-known-good checkpoint, malformed-entry handling, latency-methodology wording |
| `unsupported_absolute_or_guarantee` | 150 | Review candidates, mostly scoped invariants and test assertions; individual evidence review remains open |
| `vague_quantifier_or_scope` | 118 | Review candidates, mostly explanatory prose and parameter descriptions; individual threshold review remains open |

The one identified false positive is `src/exchange/polymarket/codec.rs:299`, where a comment quotes the Rust `Option` variant. Ten findings are in existing commit bodies; those messages are historical evidence and remain unchanged. Of the 292 review candidates, 24 are retained technical or explanatory uses and 257 remain `open_review`. They are not defects and do not authorize bulk replacement.

The machine-readable report includes the scanner output and a `classification` object with category dispositions and review notes.

A strict scan of the staged integration tree excluded the baseline report and found only the same single tautology. The added documentation produced three review-only candidates and no new strict failure.

## 4. Documentation and architecture checks

| Check | Result |
|---|---|
| `python3 -m unittest discover -s scripts -p 'test_*.py' -v` | Pass: 7 tests |
| `python3 scripts/check_architecture.py --check` | Pass: 42 Rust sources, 238 attributed tests, 42 reachable files, 9 diagrams |
| `python3 -m py_compile scripts/assurance/english_audit.py scripts/assurance/test_english_audit.py` | Pass |
| Relative-link check for changed documentation | Pass: 46 links across 16 files |
| `cargo fmt --all -- --check` | Pass |
| `cargo clippy --all-targets --locked -- -D warnings` | Pass |
| `cargo test --all-targets --locked` | Pass: 235 passed, 3 ignored |
| `cargo test --doc --locked` | Pass: 0 doc tests |
| `cargo build --release --locked` | Pass |
| `git diff --check` | Pass |

The Architecture workflow now also runs the assurance scanner tests. The audit itself remains non-blocking and is not installed as a required CI check. The proposed CI job is retained at `docs/assurance/ci/quality-gate.proposed.yml`.

## 5. Not run locally

Every project gate relevant to this documentation-only change was run locally at the integration commit: formatting, lint, all-target tests, doc tests and the release build.

The required Rust CI workflow reruns these gates on the exact pull-request head. Remote CI remains the authoritative evidence for the merged commit because the local tree differs from the eventual squash commit SHA.

## 6. Open items

- The one-line correction at `src/orderbook.rs:307` lands in a separate PR.
- Individual review of the 257 open candidates remains open.
- The non-blocking audit is available as a local command but is not yet a CI report.
- Strict CI enforcement remains disabled pending baseline review and exception approval.
- JH-4 storage correctness, operational soak and all live-execution gates remain open.

## 7. Disposition

The assurance documents, scanner, tests, baseline report and navigation changes are ready for review. This change does not alter runtime behavior, order authority, custody, settlement or tenant boundaries. It establishes the engineering baseline required before JH-4 and any trading work.
