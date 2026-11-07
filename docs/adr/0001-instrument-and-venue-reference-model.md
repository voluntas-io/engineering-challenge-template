# ADR 0001: Instrument and venue reference model

- **Status:** Proposed
- **Date:** 2026-11-07
- **Scope:** Documentation and design decision only. No Rust types, catalog,
  database, schema, or adapter changes.
- **Related:** [Platform charter](../platform-charter.md),
  [architecture](../architecture.md), [design decisions](../design-decisions.md).

## 1. Status and context

This ADR is **Proposed**. It becomes **Accepted** only after the identity
invariant and worked scenarios are reviewed.

The platform charter defines five boundaries and states that an instrument is
not a venue symbol. The current implementation does not yet separate those
concepts. `MarketInfo` carries exchange, native symbol, output symbol, outcome,
close time, and price metadata in one record. The collector keys books by
`native_symbol`, and the catalog persists a list of `MarketInfo` records under
one exchange name.

That representation is sufficient for two prediction-market adapters, but it
cannot answer basic reference-data questions without venue-specific
interpretation:

- Is this record an instrument, a listing, a venue market, a data product, or a
  serialization symbol?
- Can one instrument trade on two venues?
- Is a Kalshi event the same thing as a tradable market?
- Is a Polymarket condition the same thing as an outcome token?
- When a symbol is reused or changed, which historical identity applies?
- Which organization supplied the data, and which venue executed the trade?

This ADR decides the reference-model meanings before any code is changed. The
invariant is:

> An instrument has a persistent identity independent of its ticker, listing,
> venue, feed, connection, or serialization symbol.

The model must also support a source that has no public order book, including a
dark pool that publishes only trade reports.

## 2. Current implementation findings

These findings are observations about `main` at the time of writing. They are
recorded so the migration is explicit; none of them is fixed by this ADR.

| Current implementation | Identity problem |
|---|---|
| `MarketInfo` | Combines instrument, venue, listing, mapping, output identity, and outcome metadata in one struct |
| `native_symbol` | Used as the key in `CollectorCore.markets`; it is a venue wire identifier, not an instrument key |
| `symbol` | Namespaced output identity written to SBE, not a global instrument identity |
| `exchange_market_id` | Its meaning differs by adapter: Kalshi stores a parent event ticker; Polymarket stores a parent condition ID |
| `ExchangeKind` | Selects a Kalshi or Polymarket connector; it is not a global trading-venue enumeration |
| `SourceId` | Contains `{ shard, replica }`; it identifies a collector worker, not an external data provider |
| `Catalog` | Stores one exchange name and a collection of `MarketInfo` records |
| `outcome` | A display label for a prediction-market side, not an instrument identifier |

### Kalshi event and market identifiers

The Kalshi adapter currently builds:

```rust
MarketInfo {
    exchange: "kalshi".to_owned(),
    native_symbol: self.ticker.clone(),
    symbol: self.ticker,
    exchange_market_id: self.event_ticker,
    // ...
}
```

`event_ticker` is the parent event identifier. Kalshi documents an event as
containing one or more markets, and each market has its own ticker. The current
code therefore stores an event grouping in a field that other code interprets as
a market identity.

`count_selected_markets` deduplicates by `exchange_market_id`, falling back to
`native_symbol`. For Kalshi that can count several distinct markets under one
event as one selected market.

Consequences today:

- `CapacityPlan.selected_markets` and `Catalog.selected_markets` can undercount
  Kalshi markets that share an event.
- Shard sizing is not affected because it uses `outcome_symbols`, not
  `selected_markets`.
- Kalshi discovery statistics currently set `selected_markets` to
  `markets.len()`, which counts market records directly. Capacity and catalog
  therefore use a different counting rule from discovery.

This is an implementation issue, not a reason to keep `exchange_market_id` as a
market key. The reference model must first decide what an event, market,
instrument, and listing mean. The counting fix belongs to a later PR.

### Polymarket identifiers

The Polymarket adapter currently derives the output symbol from the condition ID
and the outcome array index:

```text
polymarket:<condition_id>:<index>
```

`native_symbol` is the outcome token identifier, and `exchange_market_id` is the
condition ID repeated for every outcome token. The condition is a parent
grouping; each token is independently tradable. They are not interchangeable
instrument identifiers.

### Runtime source identity

`SourceId { shard, replica }` names one websocket source worker. It is a runtime
replica identity. It must not be used as the identity of an exchange, a data
provider, a feed, or a trading venue.

### Connector identity

`ExchangeKind { Kalshi, Polymarket }` is a configuration-level connector
selector. It is not a registry of financial venues, and it must not grow into
one. A future venue can use a shared connector (for example FIX) without adding
a variant for every trading facility.

## 3. Domain definitions

### Instrument

An `Instrument` is a financial instrument or an independently tradable contract.
It has a persistent internal identity that is assigned by the platform and does
not depend on a ticker, a venue, an outcome label, or a connection.

| Example | Identity distinction |
|---|---|
| Ordinary share | One instrument; may have several listings |
| Futures contract | Each expiry is a distinct instrument |
| Option contract | Each underlying, expiry, strike, right, and contract specification is distinct |
| FX spot pair | Base and quote currencies define the pair |
| Event contract | Each independently tradable contract is distinct |
| Polymarket outcome token | Each token is distinct from its parent condition |
| OTC derivative | Contract terms and counterparties matter; no public listing is required |

Do not use one generic `outcome` field as the identity model. An outcome label is
metadata on an instrument or contract, not its identity.

Instrument relationships are separate from identity: underlying, outcome-of,
deliverable, related instrument, share class, and contract family. A relationship
does not make two legally or economically distinct contracts the same
instrument.

### InstrumentIdentifier

One instrument can have zero or more external identifiers. Each identifier is a
namespaced assertion:

```text
scheme
assigning_authority
identifier_value
scope
valid_from
valid_to
observed_at
source_provenance
```

An identifier is meaningful only within its namespace and scope. The standards
have different roles:

| Standard | Role |
|---|---|
| ISO 6166 | ISIN identification, where assigned |
| ISO 10962 | CFI classification, not an instrument key |
| ISO 10383 | Market and facility identification through MICs |
| ISO 4217 | Currency identification |

MICs identify facilities and trade-reporting locations, not instruments. The
ISO 10383 model distinguishes operating MICs and segment MICs; preserve that
hierarchy. An operating MIC may identify the venue operator's facility, while a
segment MIC identifies a market segment. Do not flatten both into one venue
field.

**Decision:** never use a ticker, CFI code, currency code, or MIC as the internal
instrument primary key.

### Venue and VenueOperator

A `Venue` is a trading facility with jurisdiction, regulatory classification, and
external identifiers. It is not necessarily an exchange, and it is not
necessarily a public order-book venue. Venues include exchanges, MTFs, ATSs,
dark pools, and other regulated facilities.

A `VenueOperator` is the legal entity that operates one or more venues. The
operator and the venue are separate identities. One operator can run several
venues; one venue can have several market segments.

### MarketSegment

A `MarketSegment` is a segment of a venue, where the venue has such a structure.
It carries the segment MIC or venue-specific segment identifier and the rules
that apply to listings in that segment. A venue has one or more segments when the
venue publishes that structure; the model must not invent a segment that the
venue does not define.

### Listing

A `Listing` is the admission of an instrument to trade on a market segment,
with effective dates and listing terms. A listing belongs to exactly one
instrument and one market segment during its validity period. An instrument can
have many listings across venues and time.

An OTC instrument may have no public listing. That is a valid state, not a
missing venue or a fabricated exchange.

### VenueInstrumentMapping

A `VenueInstrumentMapping` binds an external trading identifier to a listing.
It is scoped by venue, market segment, feed, or protocol where necessary, and it
has an effective interval because symbols can change or be reused.

```text
Instrument: canonical instrument A
Listing:
    venue: venue V
    market_segment: segment S
VenueInstrumentMapping:
    feed: feed F
    native_symbol: ABC
    valid_from: ...
    valid_to: ...
```

`ABC` on one venue and `ABC` on another are two mappings. They never collide
merely because the text matches.

## 4. Entity relationships and cardinalities

```text
                         INSTRUMENT
                             |
                +------------+------------+
                |            |            |
          Identifiers   Specification  Relationships
                |
             LISTING
                |
       VenueInstrumentMapping
                |
         MARKET SEGMENT
                |
              VENUE
                |
         VENUE OPERATOR


          DATA PROVIDER
                |
               FEED
                |
           FEED SESSION
                |
      Runtime worker / connection
```

Cardinalities:

- One `VenueOperator` operates one or more `Venue` records.
- One `Venue` has one or more `MarketSegment` records when segments are known.
- One `Instrument` has zero or more `Listing` records.
- One `Listing` belongs to one `Instrument` and one `MarketSegment` during its
  validity period.
- One `Listing` has one or more `VenueInstrumentMapping` records across feeds and
  protocols.
- One `DataProvider` supplies one or more `Feed` records.
- One `Feed` has one or more `FeedSession` records and zero or more
  `Entitlement` records; public data still carries an explicit access policy.
- One `Feed` may carry events originating from many venues.
- A venue may have no public market-data feed; a feed may have no venue.

These are conceptual cardinalities. The implementation encoding, storage, and
identifier type are deferred.

## 5. Internal versus external identity

| Identity | Purpose | May change? | Used as a key? |
|---|---|---|---|
| Internal instrument identity | Stable platform reference | No | Yes, within the platform |
| Instrument identifier | External assertion such as ISIN or venue identifier | Yes, with validity | Scoped lookup only |
| Listing identity | Admission of an instrument to a segment over time | Listing terms may change | Yes, with effective dates |
| Venue identity | Facility identity such as a MIC | Facility metadata may change | Yes, with validity |
| Venue operator identity | Legal entity | Corporate structure may change | Separate reference |
| Feed / provider identity | Data delivery and entitlement | Feed versions change | Scoped reference |
| Venue instrument mapping | External symbol on a feed | Yes; symbols are reusable | Scoped and effective-dated |
| Output symbol | Serialization identity for the current SBE contract | May be regenerated | Not an instrument key |
| `SourceId` | Runtime worker replica | Per run | Runtime only |

The internal instrument identity should be opaque and platform-assigned. It must
not be a hash of mutable fields, a ticker, an ISIN, a CFI code, a MIC, a
Polymarket token ID, or a Kalshi ticker. The concrete representation is deferred;
this ADR adds no UUID or ULID dependency.

Output symbols remain a compatibility concern. The current SBE output must keep
working, so the output symbol is a serialization mapping to a listing or
instrument, not the instrument identity itself.

## 6. Identifier scope and temporal validity

Identity is temporal. An instrument, listing, feed, or mapping that is valid
today may be superseded or reused later. Historical events must resolve against
the identity that was valid when the event occurred.

Two times must be distinguished:

- **Valid time:** when the identifier, listing, or mapping applies in the
  market.
- **Observed time:** when the platform learned or recorded the assertion.

Every assertion retains its source and provenance. Corrections append a new
version; they do not overwrite the old mapping. The model must support
as-of-event resolution rather than resolving everything against the latest
catalog.

Ambiguity is an explicit state, not a silent merge. A resolution result should
be able to represent at least:

| Result | Meaning |
|---|---|
| Resolved | One candidate matches the namespace, scope, and validity interval |
| Ambiguous | More than one candidate matches; the event retains the candidates |
| Unresolved | No candidate is known yet; the event remains pending |
| Conflict | The source assertions contradict each other or the namespace is invalid |

Identity resolution must explicitly reject:

- automatic merging by ticker;
- automatic merging by company or instrument name;
- assuming two instruments are identical because their ISIN-like text matches,
  without checking namespace and scope;
- merging economically related but legally distinct contracts.

A source may assert an identifier that cannot be resolved immediately. The
platform must preserve the assertion and its provenance until resolution is
possible. It must not invent an instrument to make the event look complete.

## 7. Data-provider, venue and feed distinctions

Data delivery identity is separate from trading-venue identity:

| Entity | Meaning |
|---|---|
| DataProvider | The organization or system supplying data |
| Feed | A data product with a channel, protocol, schema, data products, and access scope |
| FeedSession | A runtime connection or subscription to a feed |
| Entitlement | The right to receive, retain, and use a feed's data |

One provider may supply data from many venues. One feed may contain trades from
multiple venues, or may consolidate venues. A trade report can originate from a
reporting facility while execution occurred at a different venue.

A feed declares its data products, visibility, delay, sequence scope, schema
version, retention terms, and redistribution rights. A feed session is one
runtime instance and does not create a new data product.

Runtime worker identity is also separate. The current `SourceId` is a
worker/replica identity:

```text
SourceId { shard, replica }
```

It identifies one websocket source worker. It is not an external source. A later,
separate change should rename it to `SourceWorkerId` or `ReplicaId`; this ADR
does not rename it.

`ExchangeKind` remains a connector selector. It is not a global venue
enumeration, and adding a venue must not require adding a variant for every
facility.

## 8. Worked identity scenarios

### 8.1 One share listed on two venues

```text
Instrument I (share)
  Listing L1: venue A, segment A1, mapping symbol ABC on feed FA
  Listing L2: venue B, segment B1, mapping symbol ABC on feed FB
```

Result: one instrument, two listings, two mappings. A consolidated view may
derive one best bid/offer across listings, but the listings and their books stay
distinct.

### 8.2 Same ticker on two unrelated venues

```text
Instrument I1: venue A, segment A1, mapping ABC
Instrument I2: venue B, segment B1, mapping ABC
```

Result: two mappings and no identity collision. A resolver must use venue,
segment, feed, and validity, not ticker text.

### 8.3 Futures contracts with different expiries

```text
Instrument F1: product P, expiry 2027-03
Instrument F2: product P, expiry 2027-06
```

Result: two instruments with a shared product-family relationship. They are not
one instrument with a mutable expiry. Aggregating a continuous series is a
derived view, not an identity merge.

### 8.4 One Kalshi event with several markets

```text
Event E (parent event)
  Market M1 -> Instrument I1 (ticker M1)
  Market M2 -> Instrument I2 (ticker M2)
```

Result: the event is a grouping relationship, not an instrument. Each Kalshi
market is a distinct tradable instrument with its own ticker. The current code's
`exchange_market_id = event_ticker` and `count_selected_markets` deduplication is
an implementation defect to be fixed after the model is accepted. Discovery's
record count and capacity's selected-market count must eventually agree, but
this ADR does not change either.

### 8.5 OTC dealer quote without a public listing

```text
Instrument I (OTC contract)
  DataProvider D (dealer or interdealer venue)
  Feed Q (dealer quote feed)
  No public listing; no public order book
```

Result: an instrument and a data provider, with no fabricated venue or listing.
The quote is a feed product. If the dealer is also the execution venue, that is a
separate venue assertion, not an assumption implied by the quote.

### 8.6 Dark-pool execution reported by a trade-reporting facility

```text
Execution venue V (ATS/dark pool)
Reporting source R (trade-reporting facility)
Instrument I
Trade event T: executed at V, reported by R
```

Result: execution venue and reporting source remain distinct fields. The event is
a trade. The platform must not infer hidden resting orders from the report and
must not attach a synthetic book to V.

### 8.7 Polymarket condition and outcome tokens

```text
Condition C (parent market/condition)
  Outcome token T_yes -> Instrument I_yes
  Outcome token T_no  -> Instrument I_no
```

Result: the condition is a grouping, and each outcome token is an independently
tradable instrument. The condition identifier and the token identifiers are not
interchangeable. The current output symbol `polymarket:<condition_id>:<index>`
is a serialization mapping, not an instrument key. The `outcome` label (`Yes`,
`No`) is metadata, not identity.

## 9. Alternatives considered

1. **Keep `MarketInfo` as the universal identity.** Rejected: it conflates
   instrument, listing, venue, feed, and output symbol, and makes cross-venue
   consolidation ambiguous.
2. **Use the venue symbol as the primary key.** Rejected: tickers are reused,
   change, and collide across venues.
3. **Use the ISIN as a universal key.** Rejected: not every instrument has one,
   one instrument can have multiple listings, and an ISIN is a namespaced
   assertion rather than a platform identity.
4. **Model every venue as an exchange and every listing as a market.** Rejected:
   it cannot represent ATSs, dark pools, OTC dealers, or trade-reporting
   facilities without inventing a public book.
5. **Extend `ExchangeKind` into a global venue list.** Rejected: it would turn
   connector selection into a maintenance-heavy venue registry. Connectors must
   be reusable across venues.
6. **Treat a Kalshi event or Polymarket condition as the instrument.** Rejected:
   both are parent groupings with multiple independently tradable contracts.
7. **Generate identity from a hash of metadata.** Rejected: mutable titles,
   symbols, and labels would change identity.
8. **Adopt UUID or ULID now.** Deferred: the representation is an implementation
   detail. This ADR adds no dependency and no code.

## 10. Consequences and migration sequence

### Consequences

- The reference model becomes more explicit, with more entities and more
  validation than the current `MarketInfo` record.
- Cross-venue consolidation becomes possible without erasing venue, listing, or
  feed identity.
- OTC and dark-pool sources can be represented without a fabricated public book.
- Temporal querying and audit require versioned assertions and as-of resolution.
- The migration cannot be a single mechanical rename. The current output symbol
  and SBE compatibility must be preserved while the internal model changes.
- The Kalshi selected-market undercount remains until a later implementation PR
  fixes the counting key.

### Migration sequence (later PRs, not this one)

1. Accept this ADR and review the identity invariant.
2. Introduce internal instrument, listing, mapping, provider, and feed types
   alongside `MarketInfo`, without changing output behavior.
3. Have each adapter emit explicit identity assertions in addition to its
   current normalized record.
4. Add as-of resolution and ambiguity states behind a read-only interface.
5. Fix selected-market counting to use the reviewed market identity, and align
   discovery, capacity, and catalog accounting.
6. Version the catalog schema and migrate records with history preserved.
7. Rename the runtime `SourceId` to a worker/replica identity in a separate,
   mechanical change.
8. Only then consider changing the numeric and event models.

None of these steps is permitted to change existing SBE wire semantics without a
separate schema-evolution decision.

## 11. Explicit non-goals

This ADR does not:

- add Rust structs or enums;
- change `MarketInfo`;
- change `ExchangeKind`;
- change `SourceId`;
- change `Catalog`;
- add a database;
- add UUID or ULID dependencies;
- change the SBE schema;
- implement an identifier-resolution engine;
- add an exchange adapter;
- change the numeric, price, quantity, or event model;
- fix the Kalshi selected-market count;
- rename runtime types.

## 12. Acceptance criteria

| Check | Required by this ADR |
|---|---|
| Instrument and listing | Different identities |
| Venue and operator | Different identities |
| Venue and reporting source | Different identities |
| Instrument identifiers | Namespaced and scoped |
| Listing and symbol mapping | Effective-dated |
| Feed and feed session | Different identities |
| Runtime `SourceId` | Documented as worker identity, not a data source |
| Event/condition grouping | Distinct from tradable instruments |
| OTC | Listing optional |
| Dark pools | No inferred hidden book |
| Current implementation | Documented without capability inflation |
| Code changes | None |
| Schema changes | None |

The ADR is ready to move from **Proposed** to **Accepted** when the invariant in
section 1 is accepted and the worked scenarios in section 8 are accepted as the
required semantics. Implementation begins only after that review.
