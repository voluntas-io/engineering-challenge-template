# State machines and deterministic core

## 1. Reducer contract

Every persistent domain subsystem is modeled as:

`(state, input, immutable_config) -> (new_state, outputs, effects)`.

The reducer must be total for all representable inputs: unsupported or malformed input returns a typed error or an explicit `Unknown`/`Rejected` outcome; it does not panic, silently coerce or invent a state.

No reducer may read a clock, perform I/O, use randomness or call another service. Timers, external acknowledgements, policy updates and reconciliation observations are explicit inputs.

## 2. Book continuity

Proposed state set:

```rust
enum ContinuityState {
    Unseeded,
    Live,
    Stale,
    Resyncing,
    Terminal,
}
```

Proposed input classes:

```rust
enum ContinuityInput {
    ValidAnchoringSnapshot { order: SourceOrder },
    Delta { order: SourceOrder },
    SourceLost { reason: DiscontinuityReason },
    SequenceGap { expected: u64, observed: u64 },
    ClockRegression { previous: i64, observed: i64 },
    MalformedFrame { raw_ref: RawFrameId },
    RecoveryRequested { generation: u64 },
    RecoveryAcknowledged { generation: u64, fence: SourceOrder },
    RecoverySnapshot { generation: u64, order: SourceOrder },
    LifecycleTerminal { evidence: EvidenceRef },
}
```

Transition table. This is the normative starting point; refine it against the present implementation and venue semantics before code migration.

| Current | Input | Next | Required effect |
|---|---|---|---|
| Unseeded | Valid anchoring snapshot | Live | Install source book; create coverage start |
| Unseeded | Delta | Unseeded | Ignore delta; request snapshot/recovery |
| Unseeded | SourceLost / malformed | Stale | Record incident |
| Live | Valid in-order delta | Live | Apply atomically |
| Live | Gap / clock regression / disconnect | Stale | Invalidate affected source book; record incident; stop promoting deltas |
| Live | Malformed frame | Stale or remain Live with source-local rejection according to atomic batch policy | Record raw reject and explicit policy outcome |
| Stale | Delta | Stale | Do not mutate book; retain raw record |
| Stale | RecoveryRequested | Resyncing | Emit recovery effect |
| Resyncing | Delta | Resyncing | Do not mutate book |
| Resyncing | RecoveryAcknowledged | Resyncing | Store recovery fence |
| Resyncing | Snapshot newer than acknowledged fence | Live | Replace book, resolve incident, record new anchor |
| Resyncing | Snapshot not newer than fence | Resyncing | Reject as pre-recovery evidence |
| Any non-terminal | LifecycleTerminal | Terminal | Retire listing and emit lifecycle output |
| Terminal | Any late book event | Terminal | Ignore and count late event; never resurrect |

Important distinctions:

- Source-local continuity and canonical listing continuity are related but not identical. A source may be stale while another source has a valid book.
- A shadow snapshot is not necessarily proof that it is newer than the active book. Clockless recovery requires an acknowledged recovery generation and a strictly newer receive fence.
- For Polymarket, absence of an exchange sequence means a gap may be unquantifiable. A reconnect and snapshot can re-anchor current state but cannot recover unknown deltas during the outage.
- A malformed atomic batch must not partially update a book. Parse/validate the entire batch before mutation.
- Terminal status must be monotonic unless a new listing revision is created with evidence that the venue has reopened/relisted it.

Implementation tasks:

1. Create a pure `transition_continuity(state, input) -> TransitionResult` function.
2. Move transition permission out of scattered direct assignments such as `MarketState::set_continuity`.
3. Define the transition table as data or exhaustive Rust matches.
4. Make invalid transitions explicit and countable.
5. Property-test that a delta never mutates a book outside `Live`.
6. Property-test terminal states cannot return to Live without a new listing revision.
7. Model-check the small finite state machine after property tests stabilize. Kani or TLA+ are candidates; select one by proof obligation, not fashion.

## 3. Source and recovery state

```rust
struct SourceSessionState {
    source_id: SourceId,
    connection_id: Uuid,
    connection_epoch: u64,
    app_receive_seq: u64,
    connectivity: ConnectivityState,
    last_frame_wall_time: Option<EventTime>,
    last_frame_monotonic: Option<MonotonicTime>,
    current_subscription_revision: u64,
}

enum ConnectivityState { Connecting, Connected, Backoff, Disconnected, Failed }
```

Recovery is a protocol with a causally ordered acknowledgement:

1. Core emits `Recover { generation, symbols, reconnect }` as an effect.
2. Worker records receipt and action result.
3. Worker emits `RecoveryAcknowledged { generation, connection_epoch, receive_seq }` as a core input.
4. The next snapshot is accepted only if it is newer than the acknowledged fence and satisfies listing/epoch requirements.
5. The reducer resolves the incident only when the anchor rules are met.

No callback may mutate core state outside a logged `CoreInput`.

## 4. Instrument lifecycle reducer

A listing lifecycle reducer consumes versioned discovery and lifecycle events. It must retain historical revisions and distinguish:

- Discovery refresh of unchanged listing metadata.
- Material economic change (resolution rule, payoff, collateral, native ID scheme).
- Trading constraint change (tick, min size, fee, order eligibility).
- Suspension, closure, resolution, settlement and void.

A material change creates a new revision and can invalidate open risk. Do not overwrite the old row; open orders and historical fills refer to the revision on which they were created.

## 5. Order reducer

The OMS uses explicit commands and observed events.

| Current | Input | Next | Rule |
|---|---|---|---|
| Created | RiskApproved + reservation | RiskApproved | Record limits snapshot and reservation |
| Created | RiskRejected | Rejected | No submission |
| RiskApproved | SubmitStarted | SubmitPending | Stable client order ID; write intent first |
| SubmitPending | VenueAccepted | Accepted | Store venue order ID |
| SubmitPending | VenueRejected | Rejected | Record venue reason |
| SubmitPending | Timeout / connection loss | Unknown | Never retry blindly |
| Unknown | Query confirms accepted | Accepted / PartiallyFilled / Filled | Apply observed order state and fills |
| Unknown | Query confirms absent and venue semantics prove safe | Risk-approved retry path | Reuse documented idempotency semantics; otherwise halt/manual reconcile |
| Accepted | Fill | PartiallyFilled or Filled | Unique venue trade ID; append fill once |
| Accepted / PartiallyFilled | CancelStarted | CancelPending | Record cancel request |
| CancelPending | VenueCancelled | Cancelled | Release unfilled reservation |
| Any open state | Expiry | Expired | Release only after venue state is reconciled |

A cancel timeout is also ambiguous. The order may continue filling while cancellation is in flight. Exposure remains reserved until venue truth resolves the state.

## 6. Risk reducer

Risk is not a pure predicate over stale input. It is a deterministic approval plus atomic reservation transition:

`(risk_state, intent, account_snapshot, market_snapshot, policy) -> (risk_state', decision, reservation_effect)`.

The risk service serializes conflicting reservations or uses a transaction with compare-and-swap/serializable isolation. A valid approval binds to:

- Intent hash and exact listing revision.
- Account and tenant.
- Portfolio and open-order snapshot hashes.
- Book/continuity/completeness snapshot.
- Fee and collateral revisions.
- Eligibility decision and policy version.
- Per-market, event, category, venue, asset and account limits.
- Expiry/lease and kill-switch generation.

Reservation expiry must not release collateral while an order may still exist at the venue. Unknown order state pins risk until reconciled.

## 7. Ledger and settlement reducers

Ledger postings are append-only. Fills, fees, transfers, funding/conversion, settlement, reversals and reconciliation adjustments are separate source events. A correction is a compensating posting.

Settlement is independent from order state:

- Contract open.
- Resolution proposed/provisional.
- Challenge/dispute.
- Appeal/secondary adjudication.
- Final settlement.
- Void/cancelled.

Every transition points to source evidence and policy revision. Reconciliation can hold a contract in `Unknown` and block new exposure even after the order itself is filled.

## 8. Determinism properties

Required invariant suite:

1. Same state + same input + same config => same next state and output bytes.
2. Replaying the same core-input offsets reproduces the same output hashes.
3. Reordering two inputs with distinct offsets changes only according to the actual reducer ordering; timestamps never silently reorder them.
4. Unknown/invalid data never promotes a listing/book/order/account to a safe state.
5. Every accepted effect has one stable effect ID; repeated delivery is idempotent or rejected as duplicate.
6. A `FillId`/venue trade ID can create at most one ledger posting group.
7. A settlement event cannot be applied twice to the same claim revision.
8. A missing effect result leaves the dependent state unresolved, not guessed.
