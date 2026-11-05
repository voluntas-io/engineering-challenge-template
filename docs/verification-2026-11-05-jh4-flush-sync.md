# Verification — JH-4 flush and sync failure handling

Gate evidence for the second JH-4 storage-correctness group: failed flush and
sync handling at the aligned-writer, writer-worker and journal layers.

- Baseline `main`: `99213e13070b505f9b8f176d66784c7a91eb8eaf`.
- Change scope: `src/util/aligned_sbe_writer.rs`, `src/journal.rs`,
  `src/writer.rs`, their unit tests, the generated architecture test inventory
  and the JH-4 task list.
- This is a test and evidence change. It does not alter the journal protocol,
  manifest format, rotation policy or reported run status.

## 1. Behavior under test

A failed flush is not a successful write. Each layer must report the failure,
retain the bytes the sink already accepted, and avoid duplicating or discarding
them on retry.

- `AlignedSbeWriter` hands encoded bytes to the sink before it flushes the
  sink. A sink flush failure leaves those bytes pending. A retry flushes the
  pending stage and continues at the accepted boundary.
- `WriterHandle` records a flush failure as fatal. A later successful finish or
  sync must not erase that error or report the run as complete.
- `JournalHandle` marks the run `Failed` when the periodic or per-record flush
  fails, writes the failure into the manifest, and leaves the segment
  unfinalized. The verifier reports the run as incomplete and failed.

Sync failure coverage is the existing journal test
`storage_sync_failure_marks_run_failed` and writer test
`sync_failure_on_shutdown_is_reported`. This group adds the missing flush
failure path and the aligned-writer retry tests.

## 2. Tests

| Test | Layer | Failure boundary | Assertion |
|---|---|---|---|
| `flush_failure_keeps_accepted_bytes_and_retry_delivers_each_once` | Aligned writer | Sink flush fails once while 16 encoded bytes are staged | The error is returned with `bytes_written == 16`; the stage is not delivered. A second message and a retried flush deliver 32 bytes in order. `finish` pads to 1024 bytes and the checksum equals SHA-256 over the delivered stream. |
| `finish_retry_after_flush_failure_delivers_the_padded_chunk_once` | Aligned writer | `finish` pads to a 1024-byte chunk, then the sink flush fails | The error is returned with `bytes_written == 1024`; nothing is delivered. A retried `finish` delivers the message and padding once. A third `finish` re-flushes an empty stage without appending another chunk. |
| `storage_flush_failure_marks_run_failed` | Journal | Injected `FaultPoint::Flush` under `SyncMode::Always` | The run reaches `Failed`, the status records the injected error, and the verifier reports `complete == false` with state `Failed`. |
| `periodic_flush_failure_is_reported` | Writer worker | Injected flush failure | The flush call returns the injected error, and a later shutdown still returns an error containing the same cause. |

The aligned-writer test double is `FlushFailSink`: a deterministic `Write`
implementation with a staging buffer and a configurable number of failed
`flush` calls. It uses no sleeps, filesystem, network or wall clock.

## 3. Records that survive each boundary

| Boundary | Bytes accepted before failure | Disposition |
|---|---:|---|
| Aligned-writer flush failure | 16 | Counted in `bytes_written` and retained in the sink stage; the caller receives an error. A retried flush delivers the bytes once. |
| Aligned-writer finish failure at the flush stage | 1024 | Counted as the padded chunk; the caller receives an error. A retried finish delivers the chunk once. |
| Writer-worker flush failure | Accepted messages | The worker latches the flush error as fatal; shutdown reports the error even when finalization succeeds. |
| Journal flush failure | The record already handed to the file buffer | The run is marked `Failed`, the manifest records the error, the segment remains unfinalized, and the verifier does not report completion. |

The journal test does not claim that the record is durable after a failed
flush. It establishes that the run cannot be reported complete.

## 4. Gate results

Local results at the change commit:

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | Pass |
| `cargo clippy --all-targets --locked -- -D warnings` | Pass |
| `cargo test --all-targets --locked` | Pass: **228 library + 12 mock-exchange E2E + 2 replay = 242 passed**; 3 ignored |
| `cargo test --doc --locked` | Pass: 0 documentation tests |
| `cargo build --release --locked` | Pass |
| `python3 scripts/check_architecture.py --check` | Pass: 42 Rust source files, 245 attributed test functions, 42 files reachable from crate roots, 9 structurally checked Mermaid diagrams |
| Strict language audit excluding baseline JSON | Pass: exit 0 |

The required CI checks must also pass on the exact pull-request head. This
record does not claim that they have.

## 5. Non-goals

- No rotation, rename or manifest metadata failure test.
- No physical disk-exhaustion or quota test.
- No process kill, crash/restart or recovery-idempotency test.
- No change to journal acknowledgement, manifest finalization or verifier
  semantics.
- No live soak or live-execution gate closure.

## 6. Open JH-4 items

- Directory and manifest metadata failure.
- Rotation and rename failure.
- Physical storage exhaustion on an isolated volume.
- Kill/crash at record write, segment rotation and manifest update boundaries.
- Child-process restart and incomplete-run disposition.
- Recovery idempotency across the journal and manifest layers.
- Agreement among report, manifest, segment counts and verifier disposition.
