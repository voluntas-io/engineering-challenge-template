# ADR-0003: Preserve native quotes; use unit-aware exact arithmetic

**Status:** Proposed  
**Date:** 2026-11-02  
**Owners:** Domain model / pricing

## Context

The current core uses fixed-point values suited to binary-dollar style contracts and maintains a normalized two-sided book. It preserves raw payload bytes in the journal, but normalized events do not expose both native venue quote semantics and the canonical economic interpretation. Fee and collateral conventions are not first-class in the current shared model.

Polymarket's April 2026 upgrade introduced new exchange contracts, pUSD collateral and match-time fee calculation. Kalshi's documented event API includes full price ranges, fee override fields, mutually-exclusive metadata and settlement-source references. Protocol and fee changes are expected, not exceptional.

## Decision proposed

- Use exact integer/fixed-decimal values with explicit unit and scale. No `f64` in price, quantity, fee, balance or ledger arithmetic.
- Model a `Price` as quote asset per base unit.
- Store `NativeQuote` and `CanonicalQuote` together, connected by a derivation record.
- Model full piecewise price grids and quantity min/increment/max.
- Model `AssetId` separately for pUSD, USDC, USDC.e, USD and future collateral.
- Version fee schedules with maker/taker formulas, fee asset, validity time, raw metadata reference and formula implementation version.
- Record actual fee charged on each fill. A fee quote is an estimate with a named formula, not a guarantee.

## Alternatives considered

- **Keep all prices as millionths of USD:** rejected as a universal domain assumption. It may remain in the SBE compatibility projection.
- **Use IEEE floating point:** rejected for financial arithmetic and tick validation.
- **Discard native NO side after converting to YES:** rejected; it removes venue semantics and rounding evidence.
- **Hard-code one fee formula per venue:** rejected; fee schedules can change by market, role, time and protocol revision.

## Consequences

- Arithmetic and serialization require explicit scale/rounding rules.
- Adapters retain raw values and translate to canonical economic values.
- Research can use venue-native views and cross-venue canonical comparisons.
- Unknown fee or collateral metadata blocks fee-aware backtests and risk approval.

## Acceptance criteria

- Exact fixed-point round-trip fixtures for every supported venue.
- Tick-band boundary tests and min-size/step validation.
- Native/canonical transformations preserve derivation and original quote.
- Fee calculations are versioned and verified against official fixtures/examples.
- Fill records store actual fee amount and fee asset.
- No asset conversion assumes 1:1 without an explicit, time-valid conversion relation and its costs/risks.
