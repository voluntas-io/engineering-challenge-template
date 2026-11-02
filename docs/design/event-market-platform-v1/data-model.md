# Canonical data model

**Status:** proposed domain contract, not code currently present.  
**Rule:** domain identity is independent of venue wire identity; exact decimals are unit-aware; native values are retained.

## 1. Identifier taxonomy

Use opaque newtypes in Rust. Do not pass arbitrary `String` identifiers between subsystems.

```rust
struct TenantId(Uuid);
struct AccountId(Uuid);
struct EventId(Uuid);                 // economic event/question
struct OutcomeId(Uuid);               // outcome in an economic event
struct ClaimId(Uuid);                 // contingent claim/payoff
struct ListingId(Uuid);               // one venue's tradable listing
struct VenueId(String);               // registry-controlled identifier
struct NativeMarketId(String);        // exchange market/event ID
struct NativeInstrumentId(String);   // ticker, token ID, position ID, etc.
struct RawFrameId(Digest256);
struct CoreRunId(Uuid);
struct CoreInputOffset(u64);
struct OrderIntentId(Uuid);
struct ClientOrderId(String);
struct VenueOrderId(String);
struct FillId(String);
struct LedgerTransactionId(Uuid);
struct InstrumentRevision(u64);
struct ListingRevision(u64);
```

Identity rules:

- `EventId` identifies the economic event, not a URL, slug or ticker.
- `OutcomeId` identifies one semantic outcome under an event.
- `ClaimId` identifies the payoff function over outcomes.
- `ListingId` identifies a venue-specific way to trade a claim.
- `NativeInstrumentId` is interpreted only by its `VenueAdapter` and `IdentifierScheme`.
- A listing may change tick grid, fee schedule, collateral or protocol representation. These are versioned listing revisions, not in-place mutation of old records.
- A new identifier scheme or protocol version creates a new immutable revision; the old mapping remains queryable.

## 2. Economic event and contingent claim

```rust
struct EconomicEvent {
    id: EventId,
    revision: u64,
    title: LocalizedText,
    description: Option<String>,
    category_path: Vec<String>,
    event_window: Option<TimeInterval>,
    resolution_policy: ResolutionPolicy,
    lifecycle: EconomicLifecycle,
    created_at: EventTime,
    supersedes: Option<EventRevisionRef>,
}

struct Outcome {
    id: OutcomeId,
    event_id: EventId,
    canonical_label: String,
    semantic_definition: String,
}

struct ContingentClaim {
    id: ClaimId,
    event_id: EventId,
    outcome_id: Option<OutcomeId>,
    payoff: PayoffSpec,
    payoff_asset: AssetId,
    settlement_policy: SettlementPolicy,
    revision: u64,
}
```

`PayoffSpec` is a declarative, versioned AST. Do not use arbitrary user-supplied executable code as a payoff definition.

```rust
enum PayoffSpec {
    Binary { win: Amount, lose: Amount, void: Amount },
    Scalar { expression: BoundedExpression, min: Amount, max: Amount },
    Piecewise { branches: Vec<PayoffBranch>, default: Amount },
    Basket { legs: Vec<PayoffLeg>, aggregation: AggregationRule },
}
```

An event's `ResolutionPolicy` records:

- Primary source and fallback source(s), including URL or stable source identifier.
- The exact rule text and SHA-256 of the rule snapshot.
- Publication/observation window and timezone semantics.
- Challenge/dispute process, challenge deadline, appeal stages and finality condition.
- Void/cancellation conditions and treatment of ambiguous outcomes.
- Source revision, fetched time and evidence references.

A title match is not economic equivalence. Resolution rules and payoff semantics define the claim.

## 3. Venue listing

```rust
struct VenueListing {
    id: ListingId,
    claim_id: ClaimId,
    venue: VenueId,
    native_market_id: NativeMarketId,
    native_instrument_id: NativeInstrumentId,
    identifier_scheme: IdentifierScheme,
    protocol: VenueProtocolSet,
    revision: ListingRevision,
    lifecycle: ListingLifecycle,
    price_grid: PriceGrid,
    quantity_rules: QuantityRules,
    collateral: CollateralSpec,
    fee_schedule: FeeScheduleRef,
    eligibility: EligibilityRequirements,
    native_metadata: ImmutableRawReference,
    valid_time: ValidTimeRange,
}

enum IdentifierScheme {
    KalshiTicker,
    PolymarketCtfTokenId,
    PolymarketPositionId,
    Future { namespace: String, version: String },
}

struct VenueProtocolSet {
    market_identifier_version: Option<String>,
    rest_api_version: Option<String>,
    websocket_contract_version: Option<String>,
    clob_api_version: Option<String>,
    exchange_contract_generation: Option<String>,
    signing_domain_version: Option<String>,
    schema_hash: Digest256,
}
```

**Important Polymarket rule:** do not conflate market `version` (which discovery may use to choose `clobTokenIds` versus `positionIds`) with CLOB API version, exchange contract generation or EIP-712 domain version. These are separate fields and may evolve independently. The April 2026 upgrade and the Rust client V1/V2/V3 protocol matrix make that separation necessary.

### Listing lifecycle

Use a monotonic versioned event stream, not a bool called `active`:

```rust
enum ListingLifecycle {
    Discovered,
    Open,
    Suspended { reason: String },
    Closed,
    Resolved { resolution_ref: EvidenceRef },
    Settled { settlement_ref: EvidenceRef },
    Delisted,
    Unknown { raw_status: String },
}
```

Unknown or contradictory status can be displayed for research, but it cannot pass the trade-eligibility predicate.

## 4. Exact amounts, prices, grids and collateral

Never use `f64` for a price, quantity, fee, balance, fill or ledger amount.

```rust
struct DecimalValue {
    coefficient: i128,
    scale: u32,             // value = coefficient * 10^-scale
}

struct Amount {
    value: DecimalValue,
    unit: UnitId,
}

struct Price {
    quote_asset: AssetId,
    base_unit: UnitId,
    value: DecimalValue,    // units of quote asset per one base unit
}

struct PriceBand {
    lower_inclusive: DecimalValue,
    upper_exclusive: DecimalValue,
    step: DecimalValue,
}

struct PriceGrid {
    bands: Vec<PriceBand>,
    source: MetadataProvenance,
    revision: u64,
}

struct QuantityRules {
    minimum: Amount,
    increment: Amount,
    maximum: Option<Amount>,
}

struct Asset {
    id: AssetId,
    kind: AssetKind,
    network: Option<NetworkId>,
    contract_address: Option<String>,
    decimals: u8,
    canonical_symbol: String,
}
```

Units must be explicit:

- `YES_SHARE`, `NO_SHARE`, token/position unit and Kalshi contract are not silently interchangeable.
- `pUSD`, USDC, USDC.e and USD are separate `AssetId` values. A displayed 1:1 backing relation is metadata, not permission to erase conversion risk or routing cost.
- `Price` is a quote per base unit with a quote asset, not a dimensionless float assumed to be dollars.
- A price grid may be piecewise. Validate a price against the correct band; do not reduce Kalshi's full band structure to one global tick.
- Quantity minimum and increment are per listing and can change by revision.

For arithmetic, use checked operations and explicit rounding policy (`TowardZero`, `Floor`, `Ceiling`, `Bankers` only where contractually justified). Every rounding decision is named and testable.

## 5. Fee model

```rust
struct FeeSchedule {
    id: FeeScheduleId,
    venue: VenueId,
    listing_id: ListingId,
    revision: u64,
    maker: Option<FeeFormula>,
    taker: Option<FeeFormula>,
    fee_asset: AssetId,
    valid_from: EventTime,
    valid_until: Option<EventTime>,
    source: MetadataProvenance,
    formula_schema_version: u32,
}

enum FeeFormula {
    FlatRate { rate: DecimalValue },
    VenueFormula { formula_id: String, parameters: CanonicalJson },
    Unknown { reason: String },
}

struct FeeQuote {
    schedule_ref: FeeScheduleRef,
    side: Side,
    liquidity_role: LiquidityRole,
    price: Price,
    quantity: Amount,
    fee: Amount,
    evaluated_at: EventTime,
    formula_implementation: String,
}
```

The adapter supplies the current fee parameters and the fee engine evaluates a versioned formula. The raw metadata and formula source are archived. An unknown formula is allowed as an explicit data state but blocks fee-aware performance claims and live risk approvals. Do not treat maker and taker fees as one field. Fees may be evaluated at match time, not order creation; store the fee actually charged on each fill.

## 6. Native and canonical quote representations

Do not throw away venue-native sides during normalization.

```rust
struct NativeQuote {
    listing: ListingId,
    native_side: NativeSide,
    native_price: DecimalValue,
    native_quantity: DecimalValue,
    quote_asset: AssetId,
    source_frame: RawFrameId,
}

struct CanonicalQuote {
    claim: ClaimId,
    economic_side: EconomicSide,
    price: Price,
    quantity: Amount,
    derivation: DerivationRef,
}

struct BookUpdate {
    listing: ListingId,
    native_updates: Vec<NativeQuote>,
    canonical_updates: Vec<CanonicalQuote>,
    event_time: Option<EventTime>,
    receive_time: ReceiveTime,
    source_order: SourceOrder,
    provenance: ProvenanceRef,
}
```

A Kalshi NO-bid-to-YES-ask transformation is represented as a derivation, not destructive conversion. Canonical projections can expose a common YES-claim view while retaining native NO quotes, rounding and quantity semantics.

## 7. Outcome relationships and cross-venue equivalence

```rust
enum OutcomeConstraintKind {
    MutuallyExclusive,
    Exhaustive,
    ExactlyOne,
    AtMostOne,
    Complement,
    Conditional,
}

struct OutcomeConstraint {
    id: Uuid,
    event_id: EventId,
    members: Vec<OutcomeId>,
    kind: OutcomeConstraintKind,
    evidence: Vec<EvidenceRef>,
    validity: ValidTimeRange,
    revision: u64,
}

struct CrossVenueEquivalence {
    left_claim: ClaimId,
    right_claim: ClaimId,
    relation: EquivalenceClass,
    differences: Vec<ResolutionDifference>,
    evidence: Vec<EvidenceRef>,
    assessment_model: Option<String>,
    assessed_at: EventTime,
}

enum EquivalenceClass { Exact, Near, Related, Incomparable, Unknown }
```

Do **not** assume that outcome prices sum to one merely because labels are mutually exclusive. A valid probability-style sum constraint requires both an exhaustive and mutually exclusive outcome set under compatible payoff, collateral and settlement conventions. Market bid/ask prices still differ because of spread, fees, liquidity and venue basis risk.

Cross-venue relation is not a boolean `same_market`. Store differences in resolution sources, rule versions, settlement timing, void treatment, collateral and jurisdiction. Any numeric match score must be calibrated, versioned and accompanied by the evidence that generated it.

## 8. Time and provenance types

```rust
struct ReceiveTime {
    wall_utc_ns: i128,
    monotonic_ns: u128,
    process_boot_id: Uuid,
    clock_sync: ClockSyncRef,
}

struct SourceOrder {
    source_id: SourceId,
    connection_id: Uuid,
    connection_epoch: u64,
    application_receive_seq: u64,
}

struct Provenance {
    raw_frames: Vec<RawFrameId>,
    decoder_id: String,
    decoder_version: String,
    canonical_schema_version: u32,
    adapter_protocol: VenueProtocolSet,
    core_build: Digest256,
    config_digest: Digest256,
    instrument_revision: InstrumentRevision,
    listing_revision: ListingRevision,
}
```

`wall_utc_ns` supports cross-process display and time correlation. `monotonic_ns` supports local elapsed-time and latency measurement. `process_boot_id` prevents accidental comparison of monotonic values across process restarts. `ClockSyncRef` records clock source, offset estimate and uncertainty at the time of capture. Exchange time is a separate optional field, never a replacement for receive time.

## 9. Strategy intent and risk contract

Strategy output is an intent, not an order and not a signature.

```rust
struct OrderIntent {
    id: OrderIntentId,
    tenant: TenantId,
    account: AccountId,
    strategy_id: String,
    strategy_revision: String,
    listing: ListingId,
    listing_revision: ListingRevision,
    side: Side,
    limit_price: Price,
    quantity: Amount,
    time_in_force: TimeInForce,
    expires_at: Option<EventTime>,
    client_order_id: ClientOrderId,
    market_state_ref: CoreInputOffset,
    created_at: EventTime,
}

struct RiskDecision {
    id: Uuid,
    intent_hash: Digest256,
    result: RiskResult,
    policy_revision: String,
    limits_snapshot: Digest256,
    portfolio_snapshot: Digest256,
    data_quality_snapshot: Digest256,
    eligibility_decision: EligibilityDecisionRef,
    exposure_before: ExposureVector,
    worst_case_exposure_after: ExposureVector,
    reasons: Vec<RiskReason>,
    decided_at: EventTime,
}

enum RiskResult { Approved { reservation: ReservationId }, Rejected }
```

Risk approval must atomically reserve worst-case exposure before OMS submission. Without a reservation, an intent is rejected. Concurrent strategies cannot each pass an independent stale exposure check and overrun the same limit.

Risk checks include, at minimum:

- Account and tenant not disabled; kill switch clear.
- Eligibility attestation valid for this account, venue, listing, jurisdiction and time.
- Listing lifecycle open; protocol and listing revision supported.
- Book continuity, freshness, source quality and completeness satisfy policy.
- Fee schedule known and effective; collateral asset and conversion rules known.
- Price grid and minimum order size satisfied.
- Open orders, positions, unsettled fills and worst-case outcomes within per-listing, event, category, venue, collateral and account limits.
- Reconciliation state known and within tolerance.

## 10. OMS and execution model

```rust
enum OrderState {
    Created,
    RiskApproved,
    SubmitPending,
    Accepted { venue_order: VenueOrderId },
    PartiallyFilled,
    CancelPending,
    Filled,
    Cancelled,
    Rejected { reason: String },
    Expired,
    Unknown { operation_id: Uuid, since: EventTime },
}
```

`Unknown` is a first-class state, not an exception to be retried. A submit timeout transitions to `Unknown`. The OMS queries by client ID/order ID and reconciles. It must not blindly resubmit. If the venue cannot provide a reliable query or idempotency primitive, the affected account/listing is halted for manual reconciliation.

```rust
struct Fill {
    id: FillId,
    order: OrderIntentId,
    venue_trade_id: String,
    listing: ListingId,
    side: Side,
    price: Price,
    quantity: Amount,
    fee: Amount,
    liquidity_role: LiquidityRole,
    exchange_time: Option<EventTime>,
    receive_time: ReceiveTime,
    raw_reference: Option<RawFrameId>,
}
```

Order status and settlement status are separate state machines. A filled order is not a settled contract.

## 11. Portfolio, ledger and settlement

Portfolio positions are projections from fills, transfers, corporate/market lifecycle events and settlement postings. They are not free-form mutable balances.

```rust
struct LedgerTransaction {
    id: LedgerTransactionId,
    effective_at: EventTime,
    recorded_at: ReceiveTime,
    source_event: LogEventRef,
    postings: Vec<Posting>,
    policy_version: String,
}

struct Posting {
    account: LedgerAccountId,
    asset: AssetId,
    quantity: DecimalValue,
    direction: DebitOrCredit,
}
```

Each transaction balances per asset/commodity. A USD valuation report may aggregate assets, but the accounting ledger must not pretend pUSD, USDC, USDC.e and USD are the same instrument. The ledger models at least:

- Venue cash/collateral by asset and chain.
- Contingent-claim positions by claim and listing.
- Open-order reservations and locked collateral.
- Fees, rebates, transfer costs and gas/network costs.
- Settlement receivables/payables and disputed claims.
- External transfers and reconciliation adjustments.

Only source-evidenced events post to the ledger. Corrections are compensating transactions, not edits to historical postings.

```rust
enum SettlementState {
    Unresolved,
    Provisional { evidence: EvidenceRef },
    Challenged { dispute: EvidenceRef },
    Appealed { evidence: EvidenceRef },
    Final { result: SettlementResult, evidence: EvidenceRef },
    Voided { evidence: EvidenceRef },
    Unknown,
}
```

Settlement processing waits for the policy-defined finality condition. Oracle challenge windows and dispute outcomes are explicit events. A venue resolution message is evidence, not automatically the final internal settlement state when the contract's rules require another stage.

## 12. Completeness certificate

```rust
struct CompletenessCertificate {
    dataset: DatasetId,
    interval: TimeInterval,
    scope: Vec<ListingId>,
    raw_manifest: Digest256,
    core_input_manifest: Digest256,
    coverage: Vec<CoverageInterval>,
    discontinuities: Vec<DiscontinuitySummary>,
    source_redundancy: Vec<SourceCoverage>,
    rejected_frames: Vec<RejectSummary>,
    unknown_intervals: Vec<UnknownInterval>,
    decoder_versions: Vec<String>,
    core_build: Digest256,
    config_digest: Digest256,
    generated_at: EventTime,
}
```

The certificate distinguishes:

- **Known complete:** every expected input in the declared capture contract was accounted for.
- **Known incomplete:** a gap, disk failure, rejected payload or source outage is identified.
- **Unquantifiable:** the venue provides no sequence/clock proof or external reference to bound the missing interval.

Never turn “no detected gap” into “the venue published no missing event.” The certificate states what is known and the guarantees used.

## 13. Tenant and platform model

Add `TenantId` to dataset, account, order intent, policy and authorization scopes from the beginning. Do not make every current internal module multi-tenant prematurely. Tenant boundaries are a control-plane and storage boundary:

- Separate tenant access to raw bytes, canonical events, reports and account state.
- Tenant-specific encryption keys or envelope-encryption context for customer data.
- Per-tenant quotas, retention, rate limits and export controls.
- Audit access and mutations; customer data is never visible across tenant IDs by default.
- Keep secrets and private identity evidence outside the event log; store policy decision IDs and evidence digests instead of unnecessary PII.
