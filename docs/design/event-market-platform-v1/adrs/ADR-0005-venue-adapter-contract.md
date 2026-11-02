# ADR-0005: Venue adapters are versioned anti-corruption layers

**Status:** Proposed  
**Date:** 2026-11-02  
**Owners:** Venue integration

## Context

The system currently has Polymarket and Kalshi adapter implementations. Venue protocols, identity schemes, order semantics, fees, collateral and resolution rules evolve at different rates. One generic `version` field is insufficient. The architecture target requires a third venue to avoid shared-core changes.

## Decision proposed

Each venue adapter implements a versioned capability contract and keeps venue-native semantics inside the adapter boundary.

```rust
trait VenueAdapter {
    fn venue_id(&self) -> VenueId;
    fn protocol_set(&self) -> VenueProtocolSet;
    fn capabilities(&self) -> VenueCapabilities;
    async fn discover(&self, request: DiscoveryRequest) -> Result<RawDiscoveryBatch>;
    fn decode_market_frame(&self, frame: RawFrameRef) -> DecodeResult;
    fn normalize(&self, decoded: DecodeResult, catalog: &CatalogSnapshot) -> Vec<CanonicalEvent>;
}
```

Execution capabilities are separate traits and are not required by a public-data adapter. A data-only venue adapter cannot access OMS or signer methods. Capabilities explicitly declare sequence/clock guarantees, subscription semantics, snapshot behavior, price grid, fee source, collateral, idempotency, client-order lookup and cancel guarantees.

Pin fixtures to official contract versions and adapter code revisions. Unknown additive events are retained and counted; unknown critical protocol changes fail closed.

## Alternatives considered

- **Venue `match` branches inside `CollectorCore`:** rejected; creates permanent core leakage.
- **One adapter interface that requires every venue capability:** rejected; many venues do not support the same sequence, snapshot or order functionality.
- **Dynamic scripts for decoder plugins:** rejected for first implementation; executable extension loading complicates trust and reproducibility.

## Consequences

- Adapter capability declarations become explicit test artifacts.
- More golden fixtures and protocol metadata need maintenance.
- Shared core remains smaller and a new venue is a meaningful architecture test.

## Acceptance criteria

- Adapter contract suite runs against every venue fixture corpus.
- Capability mismatch blocks unsupported workflows.
- New venue adds adapter, fixtures and configuration without editing core reducer/model semantics.
- Protocol-version fields remain independent (market ID scheme, REST, WebSocket, CLOB, exchange contract, signing domain).
