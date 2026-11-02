# ADR-0010: Version the event-log format and preserve old readers

**Status:** Proposed  
**Date:** 2026-11-02  
**Owners:** Data infrastructure

## Context

The current shared journal uses `PMR1`, `PMJSEG01`, `PMJEND01` and `.pmj` filenames while it can contain both Polymarket and Kalshi payloads. The logical format has schema versioning and checksums, but its naming implies one venue. Existing captures are valuable and must remain readable.

## Decision proposed

Introduce a venue-neutral v2 logical format under the `EventLog` interface. Keep a read-only legacy `.pmj` reader. A migration tool may copy/convert a verified old run into the new format but must preserve raw bytes and source metadata exactly, and record the migration tool/build provenance.

Format v2 adds explicit envelope version, stream type, connection UUID, protocol-version tuple, monotonic receive time, process boot ID, clock-quality reference, raw/core causal references and hash-chain fields.

Physical encoding remains behind the interface. Start with segmented append-only files; choose compression, columnar archive and object storage only from measured throughput, query and retention requirements.

## Alternatives considered

- **Rename magic/file extension in place:** rejected; old captures and readers would become ambiguous.
- **Keep PMJ forever as canonical:** rejected; a venue-neutral platform should not expose venue branding in generic infrastructure.
- **Adopt Parquet as the live write-ahead log:** rejected; columnar query layout is useful for research but not the first append/durability contract.

## Consequences

- Two readers must exist during migration.
- Format conformance fixtures and version compatibility matrix become mandatory.
- Old captures remain verifiable; conversion is separate from verification.

## Acceptance criteria

- Legacy fixtures verify and read unchanged.
- V2 fixtures test version, hash-chain, offsets and exact payload round-trip.
- Truncated/partial/corrupt segments fail closed.
- Migration preserves payload bytes and records the old/new manifest IDs.
- Retention cannot remove unfinalized or referenced data.
