# Audit report format and disposition policy

## 1. Purpose

This document defines the machine-readable report produced by `scripts/assurance/english_audit.py` and the review vocabulary used to disposition findings. The report is evidence of a scan, not proof that the scanned prose is correct, complete or compliant.

## 2. Report schema

The scanner writes one JSON object. `schema_version` identifies this format.

| Field | Type | Meaning |
|---|---|---|
| `schema_version` | integer | Report schema revision. The current value is `1`. |
| `repository` | string | Repository identifier or local path supplied to `--repo`. |
| `head` | string | Exact commit SHA scanned. |
| `tree` | string | Git tree SHA for `HEAD`. |
| `scope` | string | Whether the scan covered the tracked tree, all reachable commits or one revision range. |
| `stats` | object | Tracked path, scanned file, binary file, unreadable file and excluded file counts. |
| `summary` | object | Total findings and counts by category and severity. |
| `policy` | object | Strict-mode behavior, the review-only status of candidates and the absence of automatic rewrites. |
| `findings` | array | One object per candidate finding. |

A baseline report can add a `classification` object and `review_notes` array. Those additions do not change scanner behavior; they record human disposition of the baseline.

## 3. Finding fields

| Field | Meaning |
|---|---|
| `source` | Tracked path, `git-commit:<sha>:subject` or `git-commit:<sha>:body` |
| `line` | One-based line number in the source; `1` for SVG visible text and one-based within the scanned field for commit subjects and bodies |
| `category` | Candidate category from the table below |
| `severity` | `error` for high-confidence tautology; `review` for every other category |
| `phrase` | Exact matched text |
| `rationale` | Why the category is flagged |
| `context` | Scanned line or visible SVG text, truncated to 500 characters |

## 4. Categories

| Category | Severity | Meaning |
|---|---|---|
| `tautology` | `error` | High-confidence redundant construction such as `merge together` or `repeat again` |
| `promotional_or_banned_candidate` | `review` | Absolute or promotional vocabulary; technical uses remain valid |
| `filler` | `review` | Emphasis that may add no technical meaning |
| `first_person` | `review` | First-person voice in normative or technical text |
| `intent_adverb` | `review` | Author intent where an observable condition is required |
| `vague_quantifier_or_scope` | `review` | Quantity or scope that may need a threshold or explicit qualification |
| `unsupported_absolute_or_guarantee` | `review` | Absolute or guarantee wording that may require a scope and evidence reference |
| `informal_or_ambiguous_verb` | `review` | Phrase that may hide a named operation, state or acceptance criterion |

The scanner examines authored prose and comments. It does not scan code identifiers, fenced code blocks, Markdown link destinations, Markdown inline code, SVG attributes, CSS, license text or extensionless legal files. Commit subjects and bodies are scanned as prose.

## 5. Classification vocabulary

Every reviewed finding receives exactly one disposition:

| Disposition | Meaning |
|---|---|
| `confirmed_defect` | The wording is wrong, redundant or unsupported in its context and should change |
| `valid_technical_use` | The wording carries a defined technical meaning; retain it |
| `quoted_source` | The wording reproduces an external standard, license, contract or protocol |
| `historical_evidence` | The wording records a past state, run or decision; retain it and amend with a dated note if needed |
| `false_positive` | The scanner matched an identifier, literal, protocol field or other non-prose token |
| `open_review` | The finding needs a named reviewer and an evidence decision |

A category-level review is acceptable for a baseline when it states the basis. It is not a claim that every line was individually adjudicated. Unresolved findings stay `open_review`; they do not become silent exceptions.

## 6. Suppression policy

A suppression applies to one line and requires a reason:

```text
<!-- english-audit-ignore: quoted source wording retained as evidence -->
```

The scanner accepts the marker in Markdown, source comments and commit text. It does not accept a repository-wide ignore file, a directory-wide suppression or a bare marker without a reason. Do not suppress a confirmed defect; correct it.

## 7. Commands and exit status

Non-blocking command:

```bash
scripts/assurance/run_audit.sh
scripts/assurance/run_audit.sh --history
```

The wrapper exits zero when findings exist, so review does not block unrelated work. It exits non-zero only for an operational error.

Strict command:

```bash
python3 scripts/assurance/english_audit.py --repo . --strict \
  --exclude 'docs/assurance/reports/*.json'
```

Strict mode exits `1` when a tautology is present and `2` for an operational error. The baseline report is excluded because it preserves finding text as evidence. Add exclusions only for files that intentionally preserve a flagged phrase.

## 8. Baseline procedure

1. Record the exact commit SHA and confirm a clean tracked tree.
2. Run the scanner over the tracked tree and all reachable commits.
3. Save the unmodified JSON report before correcting anything.
4. Classify findings by category and record specific false positives.
5. Correct only confirmed defects in a focused change.
6. Preserve historical documents and commit messages.
7. Re-run the scanner after every documentation or source change.
8. Record the command, counts, commit and reviewer in a verification record.

## 9. Limits

The audit is lexical. It cannot prove that a sentence is clear, a term is consistent, a technical statement is correct, or a financial claim is supported. It does not replace architecture review, requirement traceability, threat modelling, financial-control review or standards applicability analysis.
