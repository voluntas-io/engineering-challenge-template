# ADR-0008: Ledger is the account truth; positions are projections

**Status:** Proposed  
**Date:** 2026-11-02  
**Owners:** Accounting / execution

## Context

A trading platform requires reconciliation of cash, collateral, fills, fees, positions, transfers, settlements and disputes. PnL alone is not account truth. Prediction-market collateral and share positions can use different assets and settlement conventions.

## Decision proposed

Build an append-only, multi-asset double-entry ledger. Each source event produces an immutable posting transaction. Per-asset debits and credits balance. Positions, balances and PnL are projections from postings and valuations.

Keep order status and settlement status separate. Settlement events carry resolution evidence, provisional/challenge/appeal/finality states and compensating corrections.

## Alternatives considered

- **Mutable position table as source of truth:** rejected; cannot robustly audit corrections, partial fills or duplicated events.
- **Single USD-only ledger:** rejected; erases pUSD/USDC/USDC.e/collateral distinctions and conversion risk.
- **Treat filled as settled:** rejected; oracle/dispute and payout timing are separate.

## Consequences

- Accounting and valuation rules must be explicit.
- Reconciliation must run continuously and on session boundaries.
- An unexplained discrepancy becomes a risk hold, not a dashboard warning only.

## Acceptance criteria

- Duplicate fill event is idempotently rejected or recognized with no duplicate postings.
- Every transaction balances per asset.
- Partial fills, fees, cancel, settlement, void, challenge and correction scenarios reconcile.
- External balances/orders/trades are compared against ledger projections.
- Unknown settlement or account state blocks new risk for affected scope.
