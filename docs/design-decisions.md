# Design decisions

This record explains the choices a reviewer is most likely to question. Each
entry states the context, the decision, the alternatives considered, the
consequences, and the implementation and verification references. The
requirement-to-test matrix remains the authoritative evidence index.

## 1. Exchange abstraction and explicit capabilities

**Context.** Kalshi and Polymarket expose different wire protocols and sequence
semantics. The collector core previously identified a venue by name to decide
whether a sequence clock was available.

**Decision.** Venue behavior sits behind the `Exchange` trait. The adapter
reports `ExchangeCapabilities`, currently `has_sequence_clock`; the core
consumes that value and never matches on a venue name. The coordinator obtains
capabilities from the constructed adapter, and the replay composition root uses
the same `capabilities_for` mapping. Both adapters currently report `false`:
Kalshi's per-subscription `seq` cannot compare replicas, and its wire `clock`
is optional and not treated as cross-replica comparable until verified. They
therefore use the clockless recovery fence; synthetic clocked tests exercise
the capable path directly.

**Alternatives.** Keep `ExchangeKind` checks inside the core; build a separate
state machine per venue; assume every venue has a sequence clock.

**Consequences.** Adding a venue requires an adapter and a capability
declaration, not a change to the state machine. Capability semantics are
explicit and testable, but the capability set must grow deliberately when a new
core decision depends on a venue feature.

**Implementation.** `src/exchange/mod.rs`, `src/exchange/kalshi/mod.rs`,
`src/exchange/polymarket/mod.rs`, `src/collector/state.rs` (`CoreConfig`).

**Verification.** `src/exchange/mod.rs::configured_venue_capabilities_are_explicit`,
`src/collector/state/tests.rs::capabilities_are_not_derived_from_the_venue_name`,
`src/collector/state/tests.rs::kalshi_without_clock_requires_clockless_recovery`.

## 2. Canonical market model

**Context.** Discovery, codecs, replay, and output must agree on market
identity, source identity, event shape, and output shape. Venue payloads differ
in field names, numeric encodings, and identifier policy.

**Decision.** Normalize at the adapter boundary into `MarketInfo`,
`ExchangeEvent`, `EventMeta`, and `OutputMessage` in `src/model.rs`. Discovery
and codec modules own venue-specific parsing and eligibility. The core and
writer consume only the normalized model.

**Alternatives.** Pass raw JSON through the core; keep per-venue core types;
normalize only at the writer.

**Consequences.** The correctness-critical core is deterministic and reusable by
live, replay, and mock paths. Normalization can discard venue detail, so the
adapters must preserve fields that affect eligibility, identity, or timing, and
their tests must check the mapping explicitly.

**Implementation.** `src/model.rs`, `src/exchange/*/discovery.rs`,
`src/exchange/*/codec.rs`, `src/replay.rs`.

**Verification.** `src/exchange/kalshi/codec.rs::maps_yes_and_no_snapshot_levels`,
`src/exchange/polymarket/codec.rs::decodes_book_and_timestamp`,
`src/exchange/polymarket/discovery.rs::expands_outcome_tokens`,
`tests/replay_fixture.rs::sample_fixture_replays_through_the_same_core_and_decodes`.

## 3. Fixed-point representation

**Context.** The supplied SBE schema represents dollars and contract counts as
signed 64-bit integers scaled by 1,000,000. Exchange APIs publish decimal
strings with venue-specific precision, and the data path must not introduce
binary floating-point rounding.

**Decision.** Parse decimal strings exactly into micros in `src/fixed.rs`, reject
more than six fractional digits instead of silently rounding, and keep all
price and quantity arithmetic integral. The SBE bridge converts the normalized
integer values directly.

**Alternatives.** Use `f64`; add a general-purpose decimal crate; parse inside
each adapter.

**Consequences.** Comparisons, aggregation, and serialization are exact and
reproducible. Malformed or over-precise input is rejected at the boundary.
Every conversion must go through the fixed-point helpers rather than ad hoc
parsing.

**Implementation.** `src/fixed.rs`, `src/orderbook.rs`,
`src/util/dtos.rs`, `src/util/orderbook_schema.rs`.

**Verification.** `src/fixed.rs::parses_exact_decimals`,
`src/fixed.rs::rejects_inexact_or_malformed_values`,
`src/orderbook.rs::applies_absolute_and_delta_updates`,
`src/orderbook.rs::rejects_negative_results`.

## 4. Collector state ownership

**Context.** Book maintenance, source selection, failover, snapshot scheduling,
and discovery reconciliation are correctness-critical. Websocket I/O, retries,
and heartbeats are asynchronous and venue-specific.

**Decision.** One synchronous `CollectorCore` owns the books and decision state.
Session workers decode frames and deliver normalized events; the coordinator
owns worker and output lifecycles. The core performs no I/O and no sleeping.

**Alternatives.** A task per symbol mutating shared books; an actor per symbol;
a lock-protected global book; putting failure handling in the session layer.

**Consequences.** Decisions are serialized, invariants are local to one state
machine, and the same core runs under live, replay, and mock timestamps. The
core must remain efficient at the measured 100,000-symbol envelope, and any
blocking work must stay outside it.

**Implementation.** `src/collector/state.rs`, `src/session.rs`,
`src/coordinator.rs`, `src/replay.rs`.

**Verification.** `src/collector/state/tests.rs::snapshot_anchors_delta_application_in_per_symbol_order`,
`src/collector/state/tests.rs::cross_symbol_output_order_is_processing_order_and_not_globally_sorted`,
`tests/replay_fixture.rs::sample_fixture_replays_through_the_same_core_and_decodes`.

## 5. Redundant source architecture

**Context.** Public websocket sources can drop connections, stop delivering, or
fall behind. A single connection per shard would make continuity depend on one
venue path.

**Decision.** Assign symbols to fixed shards at startup and open `replicas`
connections per shard. Every replica maintains a shadow book; one source is
active per symbol. A backup is promoted only when its book is usable and, for
sequenced venues, not older than the last emitted clock. Promotion is followed
by an anchoring full snapshot. The per-socket symbol bound is enforced before
workers start.

**Alternatives.** One connection per shard; reconnect-only continuity;
active-active merging of sources; promote any backup without a freshness check.

**Consequences.** Redundancy costs connections and shadow memory. Failover can
emit an extra snapshot, which is intentional. Clockless venues need a stricter
recovery fence because they cannot prove shadow freshness from a sequence
number. The tested envelope is documented in `operating-envelope.md`; it is not
a claim of full-universe simultaneous subscription.

**Implementation.** `src/capacity.rs`, `src/collector/state.rs`,
`src/session.rs`.

**Verification.** `src/collector/state/tests.rs::clocked_shadow_replica_takes_over_after_source_loss`,
`src/collector/state/tests.rs::clocked_failover_anchors_resumed_deltas_and_preserves_receive_times`,
`src/collector/state/tests.rs::clockless_full_recovery_requires_acknowledged_fence_and_fresh_snapshot`,
`tests/e2e_mock_exchange/failover.rs::single_socket_loss_fails_over_and_emits_anchoring_snapshot`.

## 6. SBE compatibility

**Context.** The challenge supplies a FIX SBE schema and template-compatible
aligned reader/writer helpers. A submission that changes the schema or framing
would no longer be verifiable with the supplied tooling.

**Decision.** Keep `sbe_schemas/` unchanged, generate DTOs through the
configured build script, and write through the aligned writer with explicit
chunk sentinels and padding. The SBE capture remains the primary output; the
raw-frame journal is a separate artifact and does not alter it.

**Alternatives.** Use a custom binary format; emit JSON; extend the supplied
schema for journal or metadata fields.

**Consequences.** Captures remain readable by the pristine template reader and
by the offline `inspect`/`verify` commands. Chunk size and padding rules are
part of the output contract. New observability data must live in a sidecar or a
separate directory rather than changing the schema.

**Implementation.** `sbe_schemas/`, `src/util/build.rs`,
`src/util/aligned_sbe_writer.rs`, `src/util/aligned_sbe_reader.rs`,
`src/writer.rs`.

**Verification.** `src/util/aligned_sbe_writer.rs::aligned_writer_pads_only_on_finish`,
`src/util/aligned_sbe_reader.rs::rejects_non_zero_bytes_after_a_chunk_sentinel`,
`src/writer.rs::default_chunk_output_is_readable_by_default_reader`,
`tests/replay_fixture.rs::sample_fixture_replays_through_the_same_core_and_decodes`.

## 7. Raw-frame journaling

**Context.** Normalized SBE output cannot reconstruct the exact public frames,
including frames the decoder rejected. Debugging a venue contract change or a
decode decision requires the original bytes.

**Decision.** Add an optional append-only journal that captures the exact
payload at the receive boundary before normalization. Records carry per-source
epoch and receive sequence, checksums, and receive time. Segments receive a
header, footer, and manifest entry; verification recomputes every checksum and
compares footer and manifest counts. The queue is bounded, and a write, flush,
sync, rename, or saturation failure latches the run as failed.

**Alternatives.** Text logging; store raw bytes inside the SBE stream; rely on
venue history; use an unbounded queue.

**Consequences.** The journal adds disk I/O and can fail a run, which is the
intended durability contract. A checksum-valid journal proves the integrity of
recorded payloads, not that the venue delivered every event. Retention and
disk-watermark policies are not implemented.

**Implementation.** `src/journal.rs`, `src/session.rs`, `src/coordinator.rs`,
`src/cli.rs` (`journal verify`, `journal recover`).

**Verification.** `src/journal/tests.rs::payload_bytes_survive_write_and_read`,
`src/journal/tests.rs::checksum_mismatch_and_corrupt_manifest_fail_verification`,
`src/journal/tests.rs::queue_saturation_then_shutdown_stays_failed`,
`tests/e2e_mock_exchange.rs::journal_accounts_for_every_delivered_payload`,
`tests/e2e_mock_exchange.rs::journal_finalizes_and_verifies_on_graceful_shutdown`.

## 8. Recovery and continuity semantics

**Context.** A recovery snapshot that is applied to a stale book can hide a gap
and produce a plausible but wrong history. Sequenced and clockless venues need
different evidence before recovery is safe.

**Decision.** A sequence gap triggers a reset or resubscribe. For sequenced
venues, a shadow is promoted only when it is at least as new as the last emitted
clock. For clockless venues, recovery requires an acknowledged fence and a
snapshot received strictly after that acknowledgement; a recovery snapshot
before the acknowledgement is rejected without mutating the active book.
Promotion and reset are followed by an anchoring snapshot. An empty snapshot is
a retirement boundary. Cross-symbol output is processing order, not global
timestamp order.

**Alternatives.** Best-effort application of any snapshot; timestamp-only
ordering; sort the output globally; drop a gap without reporting it.

**Consequences.** Gaps are reported rather than hidden, and `verify` can prove
that a later snapshot disagrees with reconstructed deltas. `verify` is not a
completeness oracle by itself. Consumers that need global time order must
buffer downstream and preserve file order.

**Implementation.** `src/collector/state.rs`, `src/verify.rs`, `src/cli.rs`.

**Verification.** `src/collector/state/tests.rs::recovery_snapshot_before_ack_is_rejected_without_mutation`,
`src/collector/state/tests.rs::clockless_full_recovery_requires_acknowledged_fence_and_fresh_snapshot`,
`tests/e2e_mock_exchange/failover.rs::full_outage_recovery_exposes_the_uncovered_interval`,
`tests/e2e_mock_exchange.rs::negative_control_missing_update_is_detected_by_verify`,
`src/verify.rs::reports_a_snapshot_that_disagrees_with_reconstructed_deltas`.

## 9. Instrument and venue reference model

**Status.** Accepted 2026-11-07. The full decision, entity definitions,
cardinalities, temporal rules, and worked scenarios are in
[ADR 0001](adr/0001-instrument-and-venue-reference-model.md).

**Context.** `MarketInfo` combines instrument, venue, listing, mapping, output
symbol, and prediction-market outcome metadata. Kalshi stores a parent
`event_ticker` in `exchange_market_id`, and `count_selected_markets`
deduplicates by that field, so several markets under one event can be counted as
one. Polymarket repeats a parent condition ID for every outcome token. The
current runtime `SourceId { shard, replica }` identifies a worker, not an
external data source.

**Decision.** An instrument has a persistent platform identity independent of
its ticker, listing, venue, feed, connection, or output symbol. Instrument,
instrument identifier, venue, venue operator, optional market segment, listing,
venue-assigned mapping, provider/feed-assigned mapping, data provider, feed,
feed session, and entitlement are separate concepts. Identifiers are namespaced,
scoped, and effective-dated. Runtime worker identity is not external source
identity. Events and conditions are groupings, not instruments.

**Alternatives.** Keep `MarketInfo` as universal identity; key by ticker; use
ISIN as a universal key; model every source as a public exchange; extend
`ExchangeKind` into a venue registry.

**Consequences.** Cross-venue and OTC/dark-pool identity becomes representable
without rewriting venue behavior into the core. Migration is incremental and
must preserve existing SBE output. The Kalshi selected-market undercount remains
until a later implementation PR. This decision adds no code and no dependency.

**Implementation.** None in this decision. The migration sequence and
non-goals are in
[ADR 0001](adr/0001-instrument-and-venue-reference-model.md).

**Verification.** Documentation review against the ADR acceptance criteria;
the requirement matrix and architecture inventory are unchanged.

## 10. Kalshi subscription acknowledgement folding

**Context.** Kalshi keeps one `orderbook_delta` subscription per websocket
connection. A repeated `subscribe` on the same connection adds its markets to
the existing `sid`; the venue acknowledges it with `ok`, which carries the
connection's `sid`, the subscription `seq`, and, for current responses, the
full `msg.market_tickers` list, rather than issuing a fresh `subscribed`.
Chunked initial loads and later top-ups therefore complete against one sid,
not one sid per command. The earlier adapter expected a `subscribed` per
command: every chunk after the first stayed in the pending map until the
acknowledgement deadline, the tracked market set covered only the first
chunk, and the session cycled until a seq failure forced a reconnect.

**Decision.** `KalshiMessage::Ok` resolves the pending `subscribe` it
acknowledges. The markets fold into the subscription the venue names,
creating it when the venue has not acknowledged it yet; the venue's
`msg.market_tickers`, when present, replaces the tracked set; an `ok` without
a sid folds into the single tracked subscription; an `ok` that can be
unambiguously assigned to nothing fails the connection so the next reconnect
resubscribes cleanly instead of running on divergent local state. A
`subscribed` that names an existing sid is folded the same way rather than
rejected. `ok` sequence numbers still feed the sequence tracker so a
following delta is not misread as a gap.

**Alternatives.** Issue one `subscribe` per connection and drop the chunking;
send `unsubscribe` before resubscribing on every terminal error; treat a
missing acknowledgement as fatal without resolving it.

**Consequences.** The tracked subscription matches the venue's actual sid
state, the pending map drains, and newly added markets reconcile against the
authoritative market list. A loss of a market from the venue's list now
re-subscribes exactly that market on the next reconcile. Terminal subscription
errors still rely on seq-gap detection and reconnect for a full resync; a
venue that answers a repeated `subscribe` with a fresh sid is still handled,
as is a venue that puts `sid` only on `subscribed`.

**Implementation.** `src/exchange/kalshi/codec.rs` (`Ok::full_tickers`),
`src/exchange/kalshi/session.rs` (`merge_subscription`, the `Ok` and
`Subscribed` arms).

**Verification.** `src/exchange/kalshi/session.rs::repeated_subscribe_acked_with_ok_merges_into_the_connection_sid`,
`src/exchange/kalshi/session.rs::merged_ok_without_sid_uses_the_single_orderbook_subscription`,
`src/exchange/kalshi/session.rs::merged_ok_market_list_replaces_the_tracked_set_authoritatively`,
`src/exchange/kalshi/session.rs::subscribed_with_an_existing_sid_merges_instead_of_failing`.
