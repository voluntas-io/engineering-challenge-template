# ADR-0009: Tenant isolation and venue eligibility are first-class policy

**Status:** Proposed  
**Date:** 2026-11-02  
**Owners:** Platform / compliance

## Context

The platform may support own-account trading, market making and other users. Customer raw data, account state and strategy code cannot share unrestricted access. Venue access and trading eligibility vary by venue, account, jurisdiction and time. Public market data access is not the same as permission to trade.

## Decision proposed

Include `TenantId` in dataset, account, intent, risk, ledger and authorization contracts. Use tenant-scoped access, retention, encryption context, quota, audit and export policy.

Model eligibility as a versioned decision for `(tenant, account, venue, listing, jurisdiction, time)`. Missing, expired, contradictory or unsupported eligibility returns Unknown and denies order routing. Store minimal decision outputs and evidence references; do not place unnecessary PII in event logs.

## Alternatives considered

- **Add tenant isolation after onboarding users:** rejected; data partition leakage becomes expensive to remediate.
- **Infer eligibility from IP or network routing:** rejected; it is not a reliable legal authorization and must never be used to route around restrictions.
- **Boolean `is_allowed` on a venue:** rejected; decision scope and validity must be recorded.

## Consequences

- Tenant scope appears in log manifests, auth and storage policy.
- Compliance ownership and qualified legal review are necessary before live activity.
- Multi-tenant platform features are not required for the first internal collector milestone, but identifiers and boundaries must not foreclose them.

## Acceptance criteria

- Cross-tenant access tests deny raw, canonical, account and report reads by default.
- Eligibility decisions are auditable, versioned and scoped.
- Unknown eligibility denies order routing.
- No feature is built to circumvent venue or jurisdiction restrictions.
- Secrets and identity documents are kept outside raw market-data logs.
