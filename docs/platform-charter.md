# Cross-venue market data platform charter

This document defines the target domain and adapter boundaries for the
platform. It is a design decision, not a description of current support.
[`architecture.md`](architecture.md) remains the authoritative description of
the implemented system, and [`requirement-test-matrix.md`](requirement-test-matrix.md)
remains the evidence index for what is actually tested.

The current implementation is a prediction-market collector built on reusable
market-data infrastructure. Kalshi and Polymarket are its first connectors and
its current test environments. They are not the limits of the domain model.

## Product identity

This is cross-venue market-data infrastructure: ingestion, normalization,
validation, journaling, replay, and analysis of financial-market data across
asset classes and trading venues.

The platform is designed to support equities, fixed income, currencies, listed
and OTC derivatives, commodities, digital assets, event contracts, and
alternative trading venues. The current code does not yet support that full
scope. Each capability must be added behind the boundaries below and proven by
tests before it is claimed.

The platform is a market-data system. Order management, execution, portfolio
management, and risk engines remain separate bounded contexts. The platform
should be able to feed those systems without becoming one of them.

## Market and source scope

These are target categories. They describe what the domain model must eventually
represent, not what the adapters currently ingest.

| Asset class | Market types |
|---|---|
| Equities | Listed shares, ETFs, depositary receipts |
| Fixed income | Government bonds, corporate bonds, bills |
| Foreign exchange | Spot, forwards, swaps, NDFs |
| Derivatives | Futures, options, swaps, structured products |
| Commodities | Energy, metals, agriculture |
| Digital assets | Spot, perpetuals, futures, options |
| Event contracts | Prediction and event markets |
| OTC markets | Dealer quotes, RFQs, bilateral transactions |

Source and venue identity must cover exchanges, multilateral trading facilities,
alternative trading systems, dark pools, brokers, internalizers, dealers, and
trade-reporting facilities. A source is not required to expose an order book.
Each source declares which data products it provides and under what access
conditions.

## Five domain boundaries

```
Instrument and Reference Data
            |
Venue, Listing and Source Identity
            |
Transport and Protocol Connectors
            |
Normalized Market Data Events
            |
Journal, Replay, Storage and Analytics
```

Dependencies point downward. Venue adapters translate into the normalized
layer. The shared core does not branch on a venue name, a venue ticker, a token
identifier, a binary outcome, or a venue-specific sequencing rule.

### 1. Instrument and reference data

An instrument is not a ticker, a listing, or a venue symbol.

One instrument can be listed on several venues. A futures contract has an
expiry, a multiplier, and settlement terms. An option has an underlying, expiry,
strike, and option type. A bond has an issuer, coupon, maturity, and seniority.
An FX pair has a base and quote currency. An event contract has a resolution
rule and an outcome set.

The target model separates:

- internal instrument identity;
- external identifiers and aliases;
- instrument specification and classification;
- listing and venue mapping;
- contract terms, expiry, multiplier, and settlement;
- price denomination and quantity unit.

External standards are references, not universal keys. Use ISO 6166 (ISIN)
where an instrument has one, ISO 10962 (CFI) for classification, ISO 10383
(MIC) for venues and trade-reporting facilities, and ISO 4217 for currencies.
Do not assume every instrument has an ISIN, and do not collapse currency,
instrument, listing, and venue identifiers into one namespace. Maintain an
explicit identifier namespace plus an alias mapping.

### 2. Venue, listing and source identity

The target model distinguishes:

- `Venue`: the legal or regulatory trading facility;
- `Market`: a venue-operated market or data product;
- `Listing`: an instrument admitted to trade on a venue;
- `VenueInstrumentMapping`: the venue's symbol or identifier for that listing;
- `Feed`: a named data product with a version and access conditions;
- `FeedSession`: one connection or subscription instance;
- `FeedEntitlement`: what the consumer is authorized to receive and retain.

Jurisdiction and regulatory-facility classification are part of source
identity. An ATS in one jurisdiction is not equivalent to a dark pool, an MTF,
or a dealer quotation system in another. The model must retain that context
rather than reducing every venue to an exchange.

### 3. Transport and protocol connectors

Keep four concerns separate:

1. Transport: TCP, TLS, WebSocket, HTTP, multicast UDP, and other delivery
   mechanisms.
2. Protocol and encoding: FIX, FIX/SBE, FAST, ITCH/PITCH, and venue-specific
   binary or textual protocols.
3. Venue adapter: authentication, subscription management, sequencing,
   recovery rules, and venue-specific eligibility.
4. Normalization: conversion into the platform's event model.

FIX is a candidate foundation because its application semantics are independent
of transport and encoding and its scope covers multiple asset classes.
SBE and FAST are encoding technologies within the protocol landscape; neither
substitutes for the instrument or event model.

Start with compile-time Rust adapters and explicit capability contracts. Do not
build a plugin ABI or dynamic connector framework until a deployment requirement
justifies it.

### 4. Normalized market data events

An order book is one market-data product, not the whole domain. The core must
eventually distinguish:

| Event | Meaning |
|---|---|
| Order-book snapshot | Full book at a point in time |
| Order-book delta | Change to a book |
| Order event | Add, modify, or delete an order where L3 is available |
| Quote | Bid/ask indication or firm quote |
| Trade | Executed transaction |
| Auction | Indicative price, imbalance, and auction state |
| RFQ | Request for quote and quote response |
| Lifecycle | Listing, halt, expiry, settlement, or cancellation |
| Market status | Open, closed, auction, halted, or suspended |

Do not normalize these into one generic price update. A trade print is not a
book update. An indicative quote is not a firm bid or offer. An auction
imbalance is not an order-book level.

Book semantics and visibility must be explicit:

- L1: top of book;
- L2: aggregated price levels;
- L3: order-level data;
- consolidated: derived across venues;
- indicative: not necessarily executable;
- participant-entitled: available only under a feed's access conditions.

Every normalized event retains its source, venue, instrument, receipt and
exchange timestamps, sequence domain, and raw-data provenance. Sequence scope
is explicit: a per-session counter is not assumed comparable across replicas or
venues.

### 5. Journal, replay, storage and analytics

The shared core owns continuity and evidence: sequencing, gap detection,
recovery, raw-frame journaling, deterministic replay, and provenance. Storage
and analytics consume normalized events and books without weakening the
invariants of the ingestion path.

Replay must remain possible without a live venue. A recorded capture, including
its sequence and timing context, is the primary regression fixture for
adapters and the core.

## Numeric and price model

The current implementation uses six-decimal fixed-point values and restricts
prices to `[0, 1]`, matching the supplied challenge schema and binary event
contracts. That is a current-implementation constraint, not a platform rule.

The target model requires:

- exact decimal arithmetic for prices and quantities, never `f64` accounting;
- an explicit price denomination: currency, points, rate, yield, or another
  unit;
- an explicit quantity unit: shares, contracts, lots, base currency, or
  physical units;
- contract multipliers and instrument-specific conversion rules;
- instrument-specific tick schedules and price bands, without assuming a
  `[0, 1]` interval.

The supplied SBE schema must remain compatible with existing captures. A
general-market output contract is versioned separately rather than changing
existing wire semantics in place.

## Access, entitlement and provenance

Every feed records:

- access status: public, licensed, delayed, or participant-entitled;
- source and venue identity;
- feed version and schema;
- receipt and exchange timestamps;
- sequence scope and detected gaps;
- raw-frame provenance;
- retention and redistribution terms.

A successful connection does not imply authorization to redistribute the data.
Entitlement is a domain input, not an operational afterthought. The platform
must not claim a data product it is not authorized to receive or retain.

## Dark pools and reported execution venues

A dark pool is not an exchange with a hidden public order book. In the United
States, the SEC describes alternative trading systems as systems that allow
orders to be entered without publicly displaying price and size. That makes
their available data products fundamentally different from a public L2 feed.

| Data source | Platform representation |
|---|---|
| Public post-trade reports | Trade events and reported execution details |
| Aggregated ATS statistics | Venue or participant volume statistics |
| Licensed real-time trade feeds | Entitled execution reports |
| Participant-only market data | Restricted quotes or trading interest |
| Authorized private order data | Restricted order-level feed, only with explicit entitlement |

Public trade reports do not prove access to a dark pool's book, and the platform
must never infer hidden resting orders from prints. A dark-pool connector is a
trade-reporting source unless an authorized participant feed explicitly provides
more. The same treatment applies to broker internalizers, OTC dealers, and
trade-reporting facilities.

The US ATS framework is one jurisdiction's classification. Jurisdiction and
regulatory status remain attributes of the source, not assumptions baked into
the event model.

## Compatibility rules

1. The supplied challenge SBE schema stays valid for existing captures and
   readers. Changes require template-reader compatibility tests.
2. General market data uses a separately versioned output contract; it does not
   silently reinterpret existing fields.
3. Every normalized event retains raw-frame provenance so a capture can be
   replayed and audited.
4. Capability differences are declared, not inferred from a venue name.
5. A new adapter must not require venue-specific branches in the shared book,
   journal, recovery, or replay paths.

## Development order

| Order | Work | Completion criterion |
|---|---|---|
| 1 | Final retry-delay evidence (PR #28) | Final-attempt behaviour has deterministic regression coverage |
| 2 | Platform charter (PR #29) | Implemented system and target scope are separate |
| 3 | Instrument model ADR (PR #30) | Instrument, listing, venue, and source have separate meanings |
| 4 | Numeric model ADR (PR #31) | Price and quantity no longer assume prediction-market ranges |
| 5 | Event model ADR (PR #32) | Trades, quotes, books, auctions, and RFQs have distinct semantics |
| 6 | Schema evolution plan (PR #33) | Challenge SBE compatibility remains intact |
| 7 | First traditional-market connector (PR #34) | Shared core works outside prediction markets |

Choose the first traditional-market connector only after checking feed access,
entitlement terms, protocol documentation, and test-data availability. Do not
add an adapter merely to increase the connector count.

Do not begin instrument-model refactoring until these boundaries and
compatibility rules are written and reviewed.

## Architectural review rule

Before adding a feature, answer these questions:

1. Is this a market-domain concept or a venue-specific behaviour?
2. Can it represent more than one asset class?
3. Does it preserve price, quantity, currency, and instrument semantics?
4. Does it distinguish public, licensed, delayed, and participant-only data?
5. Does it preserve source identity, sequence scope, and timestamps?
6. Can it be replayed and tested without a live venue?
7. Does it preserve compatibility with existing SBE captures?

Reject a design that makes the shared core understand Kalshi tickers,
Polymarket token identifiers, binary outcome prices, or one venue's sequencing
rules.

## Current implementation gap

The following current constraints are deliberate: they fit the supplied
challenge, and removing them is scheduled work rather than an implicit claim of
general support.

- `ExchangeKind` enumerates only Kalshi and Polymarket.
- `MarketInfo` carries prediction-market fields such as `outcome`.
- `PriceRange::validate` and `OrderBook` reject prices outside `[0, 1]`.
- `fixed.rs` documents six-decimal prices and share-or-contract quantities.
- `ExchangeCapabilities` models only whether a comparable sequence clock exists.
- `Exchange` assumes discovery and a session factory.
- The challenge SBE schema encodes USDC prices and share-or-contract quantities.

Closing these gaps changes the domain model, not just documentation. Until then,
the platform is a prediction-market collector with reusable infrastructure, and
this charter is the reviewed target it is being moved toward.
