# Project Language Audit — 2026-11-02

**Repository:** `kj111422/engineering-challenge-template`
**Commit:** `3da31e84cb883c78c2104c839d55a718c3891088`
**Git tree:** `52aecd149b131e98baa9ee3cbbfaa08004227ec4`
**Scope:** 133 tracked blobs, including 9 SVG architecture diagrams; 60 commits reachable from `main`.

> **Integration note (2026-11-05):** This document records the historical remote audit at the stated commit. The integrated scanner now restricts candidate checks to authored prose and comments, so local counts differ from the remote sweep. The one-line source correction remains a separate change. See [`verification-2026-11-05-integration.md`](verification-2026-11-05-integration.md).

## Method

The Git tree was enumerated at the pinned commit. Every tracked blob was fetched from the raw GitHub content endpoint. All 133 fetches succeeded. The audit applied candidate patterns for high-confidence tautologies, promotional or absolute wording, first-person pronouns, and intent adverbs. Generated SVGs were checked as XML; first-person analysis examined visible `text`, `title`, and `desc` elements rather than CSS or attributes. Markdown link destinations, fenced code examples, Mermaid node identifiers, and Rust identifiers were excluded from first-person prose classification.

Commit subjects and bodies were read from the 60 commits returned by the `main` commit history API. All 60 messages were scanned.

This was a remote, read-only content audit. No project files were edited during the scan.

## Results

| Category | Confirmed findings | Disposition |
|---|---:|---|
| High-confidence tautology | 1 | Correct the source comment in a separate PR |
| Promotional/absolute vocabulary candidates | 5 | All 5 are context-appropriate technical wording; retain |
| First-person pronouns in authored prose/comments/diagram labels | 0 | No edits required |
| Intent adverbs | 0 | No edits required |
| Commit subject/body candidates | 0 | No historical message changes required |

### Confirmed tautology

`src/orderbook.rs:307`:

> Duplicate asks are malformed too, with the same exact error. <!-- english-audit-ignore: verbatim source text documented for correction -->

Proposed correction:

> Duplicate asks are malformed too, with the same error.

The patch is supplied at `patches/0001-remove-orderbook-comment-tautology.patch`. It changes one comment and does not alter executable behavior. The patch was checked with `git apply --check` against a synthetic file containing the exact target context; it has not been applied to the project checkout.

### Retained technical vocabulary

The five promotional/absolute candidates are not marketing claims:

| File and line | Phrase | Context | Disposition |
|---|---|---|---|
| `docs/design/event-market-platform-v1/adrs/ADR-0010-storage-format-and-compatibility.md:22` | `forever` | A rejected alternative: retaining PMJ as the canonical format forever | Retain; states an architectural alternative and its rejection |
| `docs/design/event-market-platform-v1/input-log-and-replay.md:75` | `perfectly` | Explicitly disclaims perfect machine-clock synchronization | Retain; states a limitation |
| `src/journal.rs:2652` | `forever` | Bounded shutdown despite a full queue and blocked writer | Retain; describes a liveness failure the deadline must prevent |
| `src/writer.rs:807` | `forever` | A timed send prevents a wedged writer from blocking the collector without bound | Retain; states the failure bound |
| `src/ws.rs:231` | `forever` | Authentication failure is fatal rather than retried without bound | Retain; states retry policy |

The word `unmatched` appeared in measurement metadata and latency documentation to identify events without a corresponding replica observation. That is a domain status, not promotional language; the scanner's promotional matcher was narrowed so this technical use is not classified as an advertising claim.

### First-person false-positive classes

The initial token-level grep identified occurrences that were not pronouns:

- `I` as Mermaid node identifiers in `docs/architecture.md`. <!-- english-audit-ignore: literal identifier token -->
- `we` inside the external link destination `what-we-do`. <!-- english-audit-ignore: literal URL token -->
- `us` as a Rust variable name denoting microseconds in `src/verify.rs`. <!-- english-audit-ignore: literal identifier token -->
- `my` in generated SVG IDs and CSS selectors such as `my-svg`. <!-- english-audit-ignore: literal generated identifier -->

The refined scanner uses Markdown, Rust-comment and SVG-visible-text rules. The 9 SVGs contain no first-person pronouns in visible labels. The remaining tracked prose and comments contain no confirmed first-person pronoun findings.

## Corrective action

Apply the one-line comment patch in a topic branch, run `git diff --check`, and allow the existing Rust CI and Architecture workflows to validate the PR. No historical commit message rewrite is justified: the 60 reachable commit subjects and bodies contain no candidate matches.

## Limits

This lexical audit does not prove that every sentence is clear, every term is consistent, or every technical statement is correct. It does not replace domain review, requirement traceability, API-contract validation, threat analysis, financial control review, or standards applicability analysis. The findings are tied to the exact commit and must be rerun after documentation or code changes.
