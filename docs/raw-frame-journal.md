# Raw-frame journal

The collector can retain every received public data frame in a versioned,
append-only journal next to the normalized SBE capture. The journal is a
forensic record: it keeps the exact payload, receive time, source epoch, and
decode outcome, including frames the decoder rejected. The supplied FIX SBE
schema and output stream are unchanged.

The journal contains public market-data payloads only. It does not store
credentials, orders, strategy state, or authenticated account data, and no
order-submission path exists in the collector.

## Enabling

The journal is off unless a directory is configured. Set it in the TOML file:

```toml
[journal]
directory = "captures/journal"
queue_capacity = 4096
enqueue_timeout_ms = 250
flush_interval_ms = 1000
sync_interval_ms = 5000
segment_max_bytes = 268435456
segment_max_secs = 300
sync_mode = "interval"   # or "always"
```

or use the environment or command line:

```bash
COLLECTOR_JOURNAL_DIR=captures/journal cargo run --release -- collect
cargo run --release -- --journal-dir captures/journal collect
```

`sync_mode = "always"` flushes and fsyncs each record. The default `interval`
mode flushes on `flush_interval_ms`, syncs on `sync_interval_ms`, and performs a
final flush and fsync at shutdown. `segment_max_bytes = 0` and
`segment_max_secs = 0` disable the corresponding rotation trigger.

## Directory layout

Each process run creates an exclusive subdirectory under the configured root:

```text
captures/journal/
  18f0c2a91d4-2f1c/
    manifest.json
    00000000.pmj
    00000001.pmj
```

An active segment is named `NNNNNNNN.pmj.partial`. Finalization writes the
footer, flushes and fsyncs the file, renames it to `NNNNNNNN.pmj`, fsyncs the
directory, and only then records it in the manifest. A process that stops
without finalization leaves the `.partial` file in place; it is never renamed
or described as complete.

## Record format

Each frame becomes one record:

| Field | Meaning |
|---|---|
| `schema_version` | Journal schema version (`1`). |
| `source` | Source identity such as `s0r0`, with `shard` and `replica` fields. |
| `connection_epoch` | Increments on every successful websocket connection for that source. |
| `receive_seq` | Per-source frame counter, reset to zero for each new epoch and advanced exactly once for every payload read at the ingress boundary — before any decode or protocol decision — including ignored and rejected payloads. |
| `received_at_us` | Unix-epoch microseconds captured immediately after the payload is read and before decoding. |
| `clock_source` | Fixed label `unix_epoch_microseconds`. |
| `frame_kind` | `text` or `binary`, preserved from the transport. |
| `payload_len` | Exact payload length. |
| `decode` | `{"status":"decoded","events":N}`, `{"status":"ignored","reason":"..."}`, or `{"status":"rejected","category":"..."}`. |
| payload | Exact bytes as received. No re-encoding, truncation, or normalization. |

## Capture boundary

Every application payload yielded by the WebSocket layer is journaled before
parsing or protocol decisions, exactly once per payload read. This includes:

- payloads that decode into events (`decoded`);
- valid payloads the adapter reads but does not act on, recorded with an
  explicit `ignored` reason — for example `control_frame` (PING/PONG text),
  `empty_frame`, `no_events`, `unknown_event_type`, and Kalshi's
  `stale_snapshot`, `foreign_lifecycle`, `subscription_ack`, `command_ack`,
  `handled_server_error`, `unsubscribed`, and `ignored_event`;
- payloads a decode or protocol check rejects, recorded with a stable
  category — `invalid_json`, `invalid_payload`, or `protocol_violation` —
  together with their untouched bytes, including the payload that triggered a
  reconnect.

Transport-level idle events (websocket ping/pong frames with no application
payload) are out of scope: they advance neither the receive fence nor the
journal. A protocol failure observed before any payload was read (for example
a pending-command acknowledgement timeout) likewise has no payload to journal
and does not advance the fence. A frame the venue wrote into a socket at the
moment a connection is torn down is a transport loss, not a journal accounting
failure; the journal accounts for every payload the collector actually read.

Rejected frames keep their payload and a stable category. Error text and
payload previews are not written into the record header, so a journal does not
accumulate unbounded diagnostic strings.

A record is framed as:

```text
"PMR1" | u32 record_len | u32 header_len | u32 payload_len
       | JSON header | payload | 64-byte lower-case SHA-256 hex
```

`record_len` covers the two length fields, header, payload, and checksum. The
checksum covers every preceding byte of the framed record. Readers enforce
`header_len`, `payload_len`, and `record_len` consistency and reject records
larger than 64 MiB.

## Segment format

A segment starts with a header and ends with a footer. Both are length-prefixed
JSON blocks followed by a 64-byte SHA-256 hex checksum of the block:

- Header magic `PMJSEG01`: schema version, run id, venue, source commit,
  capture-configuration digest, segment index, and creation time.
- Footer magic `PMJEND01`: finalized flag, record count, record-region byte
  count, segment checksum, first and last receive times, and per-source
  `first_epoch`/`first_seq`/`last_epoch`/`last_seq`/`records` ranges.

The segment checksum covers every complete framed record, including each
record's own checksum. Footer and manifest counts refer to framed record bytes,
not header or footer metadata.

## Manifest

`manifest.json` is rewritten atomically after each segment finalization and at
run close. It records the run id, venue, source commit, capture-configuration
digest, creation time, state, durability policy, optional failure text,
finalization time, and the finalized segment list with counts, checksums,
receive-time bounds, and source ranges.

States are `open`, `finalized`, `failed`, and `incomplete`. Only `finalized`
means every accepted record was written, flushed, synced, and recorded in a
renamed segment before the manifest claimed completion.

## Ordering and time

One writer thread consumes a bounded queue, so the journal order is the order in
which source workers enqueued records. For one source, records strictly advance
`(connection_epoch, receive_seq)`. Across sources, file order is queue arrival
order, not a global sort by receive timestamp; receive times can regress between
sources. A reader that needs a timestamp-ordered view must sort by receive time
and accept the resulting loss of observed arrival order.

The receive fence advances before decoding, so a rejected frame cannot reuse the
previous sequence number or hide itself from recovery ordering.

## Durability accounting

`JournalStatus` counters distinguish:

- `queued`: records accepted into the writer queue.
- `written`: records handed to the segment buffer.
- `flushed`: records whose bytes left the process buffer.
- `synced`: records covered by `sync_all`.
- `finalized`: records inside renamed segments recorded in the manifest.
- `segments`: finalized segments.
- `queue_depth`: current backlog.
- `bytes`: record-region bytes in active and finalized segments.

At finalization the segment footer's flush and fsync cover every written
record, so `flushed` and `synced` equal `written`, `finalized` equals the
verifier's record count, and `queue_depth` is zero on a healthy shutdown.

Only `finalized` is evidence of a complete run. The run report includes the
final `JournalStatus`, including the run directory and any failure text.

## Failure behavior

The journal fails the run instead of dropping a frame, and a failure is
terminal for the run:

- A full queue that stays full for `enqueue_timeout_ms` marks the run failed
  and returns an error to the source worker. The failure is latched: the
  writer observes it before every command, later records are rejected, and
  finalization refuses to set `finalized` on a failed run — including when the
  failure was latched by a producer and the normal shutdown path runs.
- A write, flush, sync, rotation, or finalization error marks the manifest
  `failed`, records the error, stops the writer, and rejects later records.
- A source worker whose journal record is rejected stops the collector with a
  fatal error, so the run report cannot describe a healthy capture after the
  journal lost a frame.
- Records accepted into the queue are either written to a finalized segment or
  the run ends with a failure. There is no silent drop path, and a run that
  latched a failure never verifies as complete.

`shutdown` and `abort` are bounded by a single absolute deadline computed at
entry. The deadline bounds producer quiescence (producers active in `record`
are admitted before finalization), command delivery, the response wait, and
writer-thread observation. A deadline expiry returns an error, marks the run
failed, and never claims finalization. A writer still blocked inside an
operating-system write cannot be interrupted; the call reports the timeout
instead of falsely claiming the thread stopped.

An abrupt stop or explicit abort leaves the active `.partial` segment and an
open or failed manifest. Verification reports the run incomplete.

Disk-full, permission, and fsync failures surface through the same paths. The
collector does not claim durability for a record that only reached the process
buffer.

## Verification

Verify a run directory against its manifest:

```bash
cargo run --release -- journal verify captures/journal/18f0c2a91d4-2f1c
```

The command prints a JSON summary and exits non-zero unless `complete` is true.
Verification checks the manifest schema and state, failure text, every listed
segment's existence and finalized state, header provenance, header and footer
checksums, record counts, byte counts, receive-time bounds, and source ranges. It
re-parses every record checksum, requires per-source order within and across
segments, rejects bytes after a footer, rejects unlisted `.pmj` files, and
rejects any `.pmj.partial` file.

`journal::read_records` performs the same completeness check and then returns
records in file order with their exact payloads.

The checksums are integrity, not authenticity: SHA-256 detects accidental
corruption and truncation, but an actor who can rewrite a segment can recompute
both payload and checksums. Tamper evidence requires an external copy of the
manifest or segment digests.

## Current limits

- No compression, retention policy, disk watermark, or cross-run index is
  implemented. Operators must size the directory and remove old runs.
- `read_records` loads the full run into memory.
- One writer thread serves the process; the queue bound converts overload into
  an explicit failure rather than unbounded memory growth.
- Payloads are not encrypted. Directory permissions are the access boundary.
- The journal complements, rather than replaces, the SBE capture. Existing
  consumers can continue to read the unchanged SBE output.
- The failure-state, bounded-shutdown, and ingress-coverage contracts are
  verified by deterministic fault injection and mock-exchange scenarios. A
  live-feed soak with segment rotation and a storage-exhaustion run on an
  isolated volume remain open operational evidence; neither has been executed
  yet, and no feed-completeness claim may be derived from journal integrity.
