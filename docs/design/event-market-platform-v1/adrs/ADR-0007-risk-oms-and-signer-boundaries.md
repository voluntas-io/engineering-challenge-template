# ADR-0007: Separate strategy, risk, OMS and signing authority

**Status:** Proposed — mandatory before live order submission  
**Date:** 2026-11-02  
**Owners:** Execution / security

## Context

The current repository is a public market-data collector. It has no OMS, independent risk gate, ledger, authenticated order route or signer service. Adding an order endpoint to the collector would combine data ingestion, decision authority and custody in one failure domain.

## Decision proposed

Use typed boundaries:

`Strategy -> OrderIntent -> RiskDecision + Reservation -> OMS -> Signer -> VenueExecutionAdapter`.

- Strategy emits intent only.
- Risk evaluates exact data/instrument/account state and atomically reserves worst-case exposure.
- OMS owns idempotency and order lifecycle, including `Unknown` after ambiguous timeout.
- Signer validates exact request digest, policy generation, venue/chain/contract allowlist and then signs using KMS/HSM-controlled keys.
- Kill switch and risk deny path do not depend on strategy process.

Paper execution uses a simulated venue adapter and the same OMS reducer. It cannot reach live network endpoints or keys by construction.

## Alternatives considered

- **Strategy signs and submits directly:** rejected; keys and limits are exposed to strategy failure/compromise.
- **Collector process owns orders:** rejected; market-data failures can then become order-authority failures.
- **Blind retry on submit timeout:** rejected; duplicates can create excess exposure.

## Consequences

- More infrastructure and fault handling before live use.
- Every order requires a risk authorization and reservation reference.
- Ambiguous states require query/reconcile or manual halt.
- Signer and kill-switch audits become deployment prerequisites.

## Acceptance criteria

- Fault injection proves ambiguous submit cannot duplicate exposure.
- Concurrent intents cannot over-reserve the same limit.
- Strategy has no signer key, secret read permission or live endpoint route.
- Signer only signs validated digests and rejects unknown domains/contract addresses.
- Kill switch works when strategy, OMS or network dependencies fail.
- Paper build has no live route and no key material.
