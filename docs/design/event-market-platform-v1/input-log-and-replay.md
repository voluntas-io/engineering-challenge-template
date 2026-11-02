# Unified input log and deterministic replay

## 1. Objective

Extend the existing raw-frame journal. Do not create a parallel raw store. The goal is a durable, versioned record from which both the venue protocol path and the deterministic core path can be reproduced.

The existing `.pmj` journal records application frames before decode. It is valuable and should remain readable. It is not yet the complete input log because the live coordinator also changes core state through discovery, source status, timer ticks, recovery acknowledgements and shutdown/finalization inputs.

## 2. Stream topology

Use separate logical streams with causal references:

| Stream | Ordering key | Contents | Purpose |
|---|---|---|---|
| Raw ingress | `tenant/venue/capture/source/connection` | Verbatim WebSocket application payloads; optional transport-control observations; raw REST discovery responses | Decoder re-runs and forensics |
| Core input | `core_run_id` | Every input delivered to the deterministic reducer | Exact state replay |
| Effect/result | `core_run_id` | Reducer effects, dispatch records and external acknowledgements/results | Recreate causal state changes without live I/O |
| Core output | `core_run_id` | Reducer outputs and output hashes/offsets | Replay comparison and provenance |

The raw stream is source evidence. The core-input stream is the authoritative order of reducer application. Neither stream should be reconstructed by sorting timestamps.

## 3. Record envelope

A proposed JSON representation for the logical envelope follows. The on-disk encoding may be binary for performance, but the semantic fields and evolution rules are normative.

```json
{
  "envelope_version": 1,
  "stream": "raw_ingress",
  "tenant_id": "tenant-uuid",
  "capture_id": "capture-uuid",
  "venue_id": "polymarket",
  "partition_key": "2026-11-02",
  "stream_offset": 10428,
  "record_id": "sha256:...",
  "record_type": "websocket_application_frame",
  "source": {
    "source_id": "s0r1",
    "connection_id": "connection-uuid",
    "connection_epoch": 4,
    "application_receive_seq": 882
  },
  "time": {
    "receive_wall_utc_ns": 1793649600123456789,
    "receive_monotonic_ns": 38112200455,
    "process_boot_id": "boot-uuid",
    "exchange_time_ns": null,
    "clock_sync_ref": "clock-sync-uuid"
  },
  "protocol": {
    "market_identifier_scheme": "ctf_token_id",
    "rest_api_version": null,
    "websocket_contract_version": "market-channel-2026-04",
    "clob_api_version": "v2",
    "exchange_contract_generation": "v2",
    "signing_domain_version": "2"
  },
  "frame": {
    "kind": "text",
    "payload_encoding": "base64",
    "payload": "eyJldmVudF90eXBlIjoiYm9vayJ9"
  },
  "payload_sha256": "...",
  "previous_record_hash": "...",
  "record_hash": "..."
}
```

Notes:

- `connection_id` is a unique UUID for one successful connection; `connection_epoch` remains the local monotonic generation number. Do not rely on shard/replica alone as a transport identity.
- `application_receive_seq` is contiguous within the application-data sequence domain for that source/connection. It does not increment for ping/pong. If transport-control observations are stored, they have a different `transport_seq` and type.
- Raw payload bytes are never normalized in place. A text frame's exact UTF-8 byte sequence is retained.
- REST discovery bodies are raw records too. Persist the raw response body, sanitized request parameters, relevant response headers, endpoint/contract version, pagination cursor, ETag/request ID when available, receive times and content hash. Never store credentials, authorization headers or secret query material.
- `clock_sync_ref` identifies a separately logged clock-quality observation; it is not a claim that the machine clock was synchronized perfectly.

## 4. Core input record

The core input stream has its own monotonically increasing `core_input_offset`. One raw frame can decode to multiple canonical events, and a non-frame input can be a core input without any raw WebSocket frame.

```json
{
  "envelope_version": 1,
  "stream": "core_input",
  "core_run_id": "core-run-uuid",
  "core_input_offset": 92511,
  "logical_time": {
    "wall_utc_ns": 1793649600123456789,
    "monotonic_ns": 38112200455,
    "clock_sync_ref": "clock-sync-uuid"
  },
  "input": {
    "type": "venue_event",
    "event_ordinal": 0,
    "event": {
      "type": "book_snapshot",
      "listing_id": "listing-uuid",
      "native_symbol": "asset-or-ticker",
      "native_values_ref": "raw-frame-id",
      "canonical_book": {
        "bids": [],
        "asks": []
      }
    }
  },
  "origin": {
    "raw_frame_ids": ["raw-frame-id"],
    "decoder_id": "polymarket-market-decoder",
    "decoder_version": "git:...",
    "instrument_revision": 12,
    "listing_revision": 7
  },
  "config_digest": "sha256:..."
}
```

Required tagged input kinds:

```rust
enum CoreInput {
    DiscoveryApplied { snapshot: CatalogSnapshotRef, markets: Vec<ListingRevisionRef> },
    VenueEvent { event: CanonicalVenueEvent, origin: Vec<RawFrameId> },
    SourceStatusChanged { source: SourceId, status: SourceStatus, origin: Option<RawFrameId> },
    RecoveryAcknowledged { source: SourceId, generation: u64, connection_epoch: u64, receive_seq: u64 },
    TimerFired { timer_id: TimerId, kind: TimerKind, logical_time: EventTime },
    OperatorAction { action: OperatorAction, actor: ActorRef, authorization: AuthorizationRef },
    ShutdownStarted { reason: String },
    ShutdownCompleted { final_time: EventTime },
}
```

The current normalized JSONL replay records already cover discovery, snapshot, delta, lifecycle, reset, source status and tick. Add an explicit `RecoveryAcknowledged` record and preserve exact source epoch/sequence rather than reconstructing both as zero. Version the replay schema. Do not confuse this core-input stream with raw wire recording.

## 5. Effect/result recording

The reducer's effects are deterministic outputs. Their execution is not deterministic and must be recorded.

```rust
struct EffectEnvelope {
    effect_id: EffectId,              // hash(core_run, input_offset, effect_ordinal)
    caused_by: CoreInputOffset,
    effect: CoreEffect,
}

enum EffectRecord {
    Requested(EffectEnvelope),
    Dispatched { effect_id: EffectId, worker_id: String },
    Succeeded { effect_id: EffectId, result: EffectResultRef },
    Failed { effect_id: EffectId, failure: StableFailureCode },
    TimedOut { effect_id: EffectId, deadline: EventTime },
}
```

If an effect result changes core state, append the corresponding `CoreInput` event before applying that transition. For example, `Control::Recover` is an effect; `RecoveryAcknowledged` is a later core input. Replay consumes the acknowledgement at the same core-input offset. It does not issue a real WebSocket request.

## 6. Time and order rules

Three axes are mandatory:

1. **Receive/capture order:** source sequence and connection identity.
2. **Core processing order:** `core_input_offset`, assigned at the single reducer boundary.
3. **Event time:** exchange-provided timestamp, when present.

Rules:

- The reducer consumes `core_input_offset` order. Never sort inputs by event timestamp or wall timestamp.
- Wall time may regress. Record the regression and preserve it.
- Monotonic time is process/boot scoped. Never compare monotonic timestamps from unrelated boot IDs.
- A missing exchange timestamp is `None`, not a fabricated exchange timestamp.
- Event-time watermarks are derived analytics. They do not rewrite ingestion order or discard late inputs.
- Source receive sequences are local to a source/connection epoch and are not a global order across venues.
- A new connection epoch does not prove event continuity; only venue-provided sequence/clock semantics can quantify gaps.

## 7. Append, durability and failure semantics

Use the existing segment writer as the first storage implementation, behind a stable `EventLog` API. Keep bounded queues and explicit backpressure. Every append returns a durable reference at the configured sync level or a typed failure.

```rust
trait EventLog {
    fn append_raw(&self, record: RawIngressRecord) -> Result<RawFrameId, AppendError>;
    fn append_core_input(&self, input: CoreInputEnvelope) -> Result<CoreInputOffset, AppendError>;
    fn append_effect(&self, record: EffectRecord) -> Result<(), AppendError>;
    fn append_output(&self, output: CoreOutputEnvelope) -> Result<(), AppendError>;
}
```

Failure classes must distinguish: queue admission timeout, writer unavailable, write error, fsync error, disk full, manifest error, checksum mismatch, partial segment and shutdown timeout. Do not mark a run complete if any required input was not accepted at the declared durability level.

The core input append and core state update need a crash-safe protocol. Recommended first implementation:

1. Append the core input to the durable log.
2. Apply the pure reducer.
3. Append deterministic core outputs/effects.
4. Advance the in-memory applied offset.
5. On restart, rebuild from the last verified checkpoint plus later log offsets. If the process dies after input append but before output append, replay the input and regenerate outputs.

Do not rely on a two-phase in-memory state mutation to be crash-atomic. The log is the source for rebuilding.

## 8. Hashing and authenticity

Current SHA-256 checksums detect accidental corruption. They do not prevent a party with write access from rewriting both a record and its checksum.

Stage integrity improvements:

1. Record hash includes canonical envelope bytes and previous record hash.
2. Segment manifest includes first/last offsets and terminal hash.
3. Run manifest references every segment in order and is itself hashed.
4. Sign the final manifest digest with a dedicated signing identity; keep the key outside the collector process.
5. Anchor the digest to a separate trust domain (for example, immutable object storage or a separately controlled timestamped registry).

Key rotation and verifier trust roots are explicit versions. Do not claim tamper evidence until signature verification and external anchoring exist.

## 9. Replay algorithms

### 9.1 Core replay (primary CI gate)

```text
manifest + config + core-input offsets
                 |
                 v
             Core::new
                 |
                 v
        for each offset in order:
            assert expected offset
            transition = Core::step(state, input)
            write deterministic outputs
            compare effect IDs / outputs
                 |
                 v
        compare output count + canonical bytes + hashes
```

The replay runner must fail on missing/duplicate offsets, schema incompatibility, unresolved external effects without a recorded result, nondeterministic output, or config/build mismatch.

### 9.2 Raw re-decode replay

```text
raw manifest -> raw frame reader -> adapter decoder(version N)
                                      |
                                      v
                           canonical events/rejections
                                      |
                recorded non-frame core inputs + event ordering
                                      |
                                      v
                              same Core::step
```

A decoder upgrade must emit a comparison report:

- Raw frames that change from rejected to accepted or vice versa.
- Event count/type changes.
- Price/quantity/native-side changes.
- Listing identity or protocol-version mapping changes.
- Core output diff by listing and offset.

Do not overwrite old canonical outputs. New decoding produces a new derived dataset/manfiest with new decoder version.

### 9.3 Exact equality contract

The CI invariant is:

`same core-input log + same core build + same config/schema versions -> same deterministic output bytes`.

Raw re-decode equality is a separate test:

`same raw log + same adapter/decoder + same non-frame inputs -> same canonical events and same core-input log`.

If the decoder version changes, output changes are allowed only with an explicit migration diff. Byte equality must exclude nondeterministic filesystem metadata or use a canonical logical-output serialization. If SBE contains build-specific or runtime-only header fields, normalize those in a test comparison while separately verifying the physical file format.

## 10. Required tests

- Raw reader round-trip preserves exact text/binary payload bytes.
- Raw frame sequence and connection epoch remain stable; control frames do not consume application receive sequence.
- REST discovery bodies and pagination cursors are preserved.
- A single frame that yields N canonical events links each event to the same raw frame with distinct ordinals.
- Rejected and unknown frames are replayable and remain rejected/unknown under the same decoder.
- Every core input type is replayed, including source disconnect, recovery acknowledgement, timer tick, discovery update and shutdown.
- Crash injection after input append, after reducer step and before output append recovers to the same output.
- Missing, duplicate, reordered and corrupt offsets fail verification.
- Core replay output matches live canonical output for deterministic mock runs.
- Raw re-decode output matches the original decoder output when the decoder version is pinned.
- New decoder versions generate a stable and inspectable diff.
