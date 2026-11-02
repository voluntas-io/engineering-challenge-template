# ADR-0002: Separate economic claims from venue listings

**Status:** Proposed  
**Date:** 2026-11-02  
**Owners:** Domain model / adapters

## Context

The current `MarketInfo` is a compact exchange-level record keyed by string identifiers. Polymarket discovery can choose CTF token IDs versus Protocol V2 position IDs, and Kalshi exposes full price bands, but the shared model does not retain the complete set of economic, listing, collateral, fee, protocol and resolution facts needed by research and trading.

A title, ticker, token ID or slug is not a stable economic identity. Two venues can list similar questions with materially different resolution terms.

## Decision proposed

Use separate immutable concepts:

- `EconomicEvent`: the event/question and resolution policy.
- `Outcome`: a semantic outcome within an event.
- `ContingentClaim`: a payoff function over event outcomes.
- `VenueListing`: one venue-specific instrument that exposes a claim for trading.
- `OutcomeConstraint`: documented relations such as mutually exclusive, exhaustive or complement.
- `CrossVenueEquivalence`: graded relation plus explicit differences and evidence.

Use opaque newtypes for all identifiers. A listing references a claim; a claim references an economic event/outcome. Every material listing change creates a revision.

## Alternatives considered

- **Keep market ticker as universal instrument ID:** rejected; venue-specific and mutable semantics leak into every subsystem.
- **Normalize by title/slug/question similarity:** rejected; no semantic or resolution guarantees.
- **One universal contract schema with arbitrary user code:** rejected; unsafe and impossible to validate consistently. Use a versioned declarative payoff AST.

## Consequences

- Discovery/catalog schemas become more detailed.
- Multiple venue listings can point to one claim without asserting exact equivalence.
- Positions and risk aggregate at listing, claim, event and relation levels.
- Historical listings remain tied to the revision under which orders/fills occurred.

## Required fields before a listing is tradable

Native ID and identifier scheme; venue; protocol-version tuple; listing revision; status; price grid; min quantity/increment; collateral asset; fee schedule; settlement rule reference; eligibility requirements; evidence provenance. Unknown required fields are explicit and fail closed for order risk.

## Acceptance criteria

- Polymarket CTF and position-backed identifiers produce distinct identifier schemes.
- Kalshi ticker and full price range are represented without lossy coercion.
- Event/outcome/claim/listing identity is deterministic and stable across title/slug edits.
- A listing update creates a revision and does not mutate history.
- Cross-venue equivalence tests verify resolution differences are retained.
