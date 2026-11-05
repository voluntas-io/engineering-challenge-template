# Technical Writing and Terminology Standard

## 1. Objective

Technical prose is an engineering interface. A reader must be able to identify the subject, action, conditions, required behavior, failure behavior, evidence and owner without relying on the author's unstated intent.

This standard applies to requirements, ADRs, architecture documents, runbooks, verification records, public API documentation, Rust comments, error messages, metric descriptions, workflow names and commit messages.

It does not authorize rewriting licenses, quotations, external specifications, protocol payloads, immutable historical records or customer-provided text. Such material can be identified and annotated without changing its source meaning.

## 2. Requirement sentence structure

A normative statement should identify:

- **Actor:** component, operator, customer, venue adapter, risk service, or release authority.
- **Action:** a testable verb.
- **Object:** state, data, order, record, credential, or operation.
- **Condition:** when the rule applies.
- **Constraint:** the required or prohibited behavior.
- **Evidence:** test, trace, metric, record, or approval that proves compliance.

Example:

> When a journal append fails, the journal writer MUST latch the run as incomplete, reject finalization, and expose the failure category in the run report. The fault-injection test `TEST-JRN-FAIL-003` verifies the transition.

Avoid:

> The journal should safely handle failures in a robust manner. <!-- english-audit-ignore: intentionally flawed writing example -->

The second statement has no failure model, state transition, observable result or verification reference.

## 3. English rules

- Prefer a named actor and an active verb.
- Use one term for one concept.
- Define abbreviations at first use.
- Replace vague demonstratives such as "this", "that", and "it" when their referent is unclear.
- Use exact units, clock domains, currencies, assets, versions and counts.
- Distinguish fact, assumption, target, hypothesis, requirement and recommendation.
- Distinguish implemented behavior from intended architecture.
- Distinguish observed evidence from an inferred explanation.
- Distinguish current status from historical status.
- Use dates in ISO 8601 form in machine-facing records.
- Use RFC 2119 requirement keywords only for normative text.
- Avoid promotional, emotional, anthropomorphic or self-congratulatory language.
- Do not use an adverb to substitute for an invariant or evidence.
- Do not describe a system as secure, safe, reliable, durable, complete, deterministic or production-ready without stating the scope and supporting evidence.

## 4. Tautology handling

Tautology patterns are review candidates, not an automatic replacement list. Examples include:

- `exact same`, `same exact` -> `same` or a precise equality statement. <!-- english-audit-ignore: literal examples in the audit policy -->
- `end result`, `final outcome` -> `result` or a defined terminal state. <!-- english-audit-ignore: literal examples in the audit policy -->
- `repeat again` -> `repeat`. <!-- english-audit-ignore: literal examples in the audit policy -->
- `revert back` -> `revert` or `restore`, depending on the operation. <!-- english-audit-ignore: literal examples in the audit policy -->
- `merge together`, `combine together` -> `merge` or `combine`. <!-- english-audit-ignore: literal examples in the audit policy -->
- `each and every` -> `each`. <!-- english-audit-ignore: literal examples in the audit policy -->
- `basic fundamentals` -> `fundamentals`. <!-- english-audit-ignore: literal examples in the audit policy -->
- `necessary prerequisite` -> `prerequisite`. <!-- english-audit-ignore: literal examples in the audit policy -->

A phrase is not tautological merely because it contains repetition in a code example, protocol field, quotation, legal title, test fixture or historical record. Human review must determine whether the phrase adds meaning in context.

## 5. Candidate vocabulary requiring review

The audit tool flags these categories:

| Category | Examples | Treatment |
|---|---|---|
| Tautology | `exact same`, `repeat again`, `revert back` | Correct when semantically redundant | <!-- english-audit-ignore: explicit vocabulary list for the audit policy -->
| Promotional claim | `bulletproof`, `world-class`, `best-in-class`, `revolutionary` | Replace with a measurable claim or remove | <!-- english-audit-ignore: explicit vocabulary list for the audit policy -->
| Unsupported certainty | `always`, `never`, `guarantees`, `ensures`, `exactly once`, `lossless` | Retain only when the precise scope and proof exist | <!-- english-audit-ignore: explicit vocabulary list for the audit policy -->
| Filler | `obviously`, `clearly`, `basically`, `actually`, `really`, `very` | Remove when it adds no technical meaning | <!-- english-audit-ignore: explicit vocabulary list for the audit policy -->
| Intent adverb | `intentionally`, `deliberately`, `knowingly`, `accidentally` | State observable design or event; retain when intent itself is evidence | <!-- english-audit-ignore: explicit vocabulary list for the audit policy -->
| First-person voice | `I`, `we`, `our`, `us` | Avoid in normative requirements and commit messages; allowed in founder narratives, quotations and historical records | <!-- english-audit-ignore: explicit vocabulary list for the audit policy -->
| Vague scope | `some`, `many`, `often`, `usually`, `large`, `small`, `reasonable` | Replace with a threshold or explicit qualification where behavior depends on it | <!-- english-audit-ignore: explicit vocabulary list for the audit policy -->

Do not ban domain vocabulary such as `intent`, `unknown`, `valid`, `current`, `future`, `recovery`, `robustness`, `final`, `market`, `contract`, `token` or `claim` globally. Define its use in the glossary. The scanner flags phrases, not all instances of individual technical words.

## 6. First-person policy

Use third-person component names or direct imperatives in engineering documents and commit messages.

Preferred:

> Add a replay test for a crash after segment synchronization.

Avoid:

> We should add a replay test because we want to make sure it works. <!-- english-audit-ignore: intentionally flawed writing example -->

First-person language remains valid in founder letters, customer interviews, personal statements, quoted standards and historical records. The audit flags it for review; it does not rewrite it.

## 7. Intent adverbs

Intent adverbs can hide the mechanism. Replace them with the mechanism where possible.

- Instead of "the writer intentionally rejects finalization", write "the writer rejects finalization after the failure latch is set". <!-- english-audit-ignore: intentionally flawed phrase under review -->
- Instead of "the retry accidentally duplicates an order", write "the retry submits the same order intent without reusing its idempotency key". <!-- english-audit-ignore: intentionally flawed phrase under review -->
- Instead of "the process deliberately stops", state the condition and resulting transition. <!-- english-audit-ignore: intentionally flawed phrase under review -->

Retain intent words when author intent is itself relevant to a design decision, incident finding, legal statement or forensic record.

## 8. Canonical glossary

| Canonical term | Definition | Avoid using as a synonym |
|---|---|---|
| Economic event | Real-world proposition or event | Market, contract |
| Outcome | Possible result of an economic event | Side, token |
| Contingent claim | Economic claim defined by an outcome and resolution rule | Listing, instrument |
| Venue listing | Venue-specific representation of an event/claim | Economic event |
| Instrument | Tradable venue object with typed trading rules | Claim, token unless venue defines token |
| Asset | Unit of value or collateral | Currency unless currency is specifically meant |
| Native price | Venue-native price representation | Canonical price |
| Canonical price | Normalized price with quote/base unit | Native quote |
| Raw ingress payload | Exact application payload at the capture boundary | Decoded event |
| Decoded event | Typed result produced by a versioned decoder | Raw payload |
| Core input | Input accepted by the deterministic core | WebSocket message |
| Core output | State/effect result produced by the core | Venue message |
| Receive sequence | Sequence assigned at the payload ingress boundary for a defined source epoch | Exchange sequence |
| Processing offset | Position in deterministic core processing order | Timestamp |
| Exchange event time | Timestamp assigned by the venue | Receive time |
| Wall-clock time | UTC/calendar timestamp from system clock | Monotonic time |
| Monotonic time | Non-decreasing process/host clock used for intervals | UTC time |
| Order intent | Typed request to submit, amend or cancel an order | Order |
| Order | Venue-facing object and its lifecycle | Intent, execution |
| Execution | Venue-reported trade/fill event | Order acknowledgement |
| Position | Quantity of claims held by an account | Balance |
| Balance | Ledger-derived amount for an account and asset | Position |
| Ledger transaction | Balanced set of accounting entries | Execution |
| Settlement | Discharge of the defined obligation under venue rules | Fill, payout declaration |
| Reconciliation | Comparison of independent records and disposition of differences | Verification alone |
| Tenant | Isolated customer boundary | User, account |
| Account | Venue or platform financial/accounting scope | Tenant |

Venue-defined names and external protocol fields retain their exact names in adapter code. Their mapping to canonical concepts must be documented at the adapter boundary.

## 9. Document authority and precedence

1. The design charter defines target architecture and invariants.
2. Accepted ADRs define decisions and consequences.
3. Requirements define testable obligations.
4. Source code implements the accepted design.
5. Tests and verification records state what was checked.
6. Runbooks define operator response.
7. The roadmap defines sequence and priority.
8. README files summarize and link; they do not override the sources above.

When two documents conflict, do not pick one silently. Open a documentation issue, identify both statements, assign an owner, determine authority and publish a correction or superseding decision.

## 10. Commit message standard

Use imperative subjects with one primary purpose. Example:

`Add deterministic short-write injection to journal tests`

Commit body should state requirement IDs, behavior changed, compatibility impact and tests run. It must not claim a gate passed when it was not run. Do not rewrite existing commit messages; apply this rule to new commits.
