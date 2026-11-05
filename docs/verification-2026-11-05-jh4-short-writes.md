# Verification — JH-4 aligned short-write handling

Gate evidence for the first JH-4 storage-correctness group: deterministic short
writes and partial-write errors in the aligned SBE writer.

- Baseline `main`: `9f44990a5f89654c0125f61e53c4d304362c76ac`.
- Change scope: `src/util/aligned_sbe_writer.rs`, its unit tests, the generated
  architecture test inventory and the JH-4 task list.
- This is a library-level storage contract change. It does not alter the journal
  protocol, manifest format, rotation policy or reported run status.

## 1. Behavior under test

`AlignedSbeWriter::write_uncommitted` now retries short writes explicitly and
advances three values for every accepted byte range:

- `committed`, so a later flush resumes after the accepted prefix;
- `bytes_written`, so the reported count includes bytes accepted before an
  error;
- the optional SHA-256, so a partially written and resumed stream still
  produces the checksum of the complete stream.

`write` returning `Ok(0)` for a non-empty buffer is reported as `WriteZero`,
matching the previous `write_all` behavior. `Interrupted` is retried without
consuming buffer progress. Any other error is returned to the caller. The
writer never treats a partial write as success; the caller must still fail the
run.

## 2. Tests

| Test | Failure boundary | Assertion |
|---|---|---|
| `short_writes_are_completed_without_data_loss` | Sink accepts at most 7 bytes per call | Multiple calls complete the message, flush reports 16 bytes, finish pads to 1024, and the decoded prefix is intact |
| `write_error_before_any_byte_is_reported_without_progress` | Sink rejects the first byte | Flush returns the injected error, the sink stays empty, and `bytes_written` stays zero |
| `partial_write_error_records_the_prefix_and_resumes_without_duplication` | Sink accepts 5 bytes then fails | Flush returns the error with 5 accepted bytes counted, a healed sink resumes from byte 5 without duplication, and finish produces the correct padded stream |
| `partial_write_checksum_covers_the_accepted_prefix_and_resumed_bytes` | Same 5-byte prefix, checksum enabled | The final checksum equals SHA-256 over the complete padded stream |

The test double is `ShortSink`: a deterministic `Write` implementation with a
per-call byte cap, an absolute failure limit and an interior-mutability switch
that heals the sink. It uses no sleeps, filesystem, network or wall clock.

## 3. Records that survive each boundary

| Boundary | Accepted bytes | Run disposition |
|---|---:|---|
| Error before the first byte | 0 | The flush returns an error; no byte is counted, and the run cannot be reported complete |
| Error after a prefix | Exact accepted prefix | The prefix is counted and checksummed; the error is returned, and a resumed flush continues at the accepted boundary |
| Short writes with no error | All | Completion and padding proceed normally; no data is lost or duplicated |

This evidence is at the byte-sink layer. Journal record acknowledgement,
manifest finalization and verifier disposition are separate JH-4 groups and
remain open.

## 4. Gate results

Local results at the change commit:

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | Pass |
| `cargo clippy --all-targets --locked -- -D warnings` | Pass |
| `cargo test --all-targets --locked` | Pass: **225 library + 12 mock-exchange E2E + 2 replay = 239 passed**, 3 ignored |
| `cargo test --doc --locked` | Pass: 0 documentation tests |
| `cargo build --release --locked` | Pass |
| `python3 scripts/check_architecture.py --check` | Pass: 42 Rust source files, 242 attributed test functions, 42 files reachable from crate roots, 9 structurally checked Mermaid diagrams |

The required CI checks must also pass on the exact pull-request head. This
record does not claim that they have.

## 5. Non-goals

- No failed-flush or failed-sync injection at the aligned writer layer.
- No rotation, rename or manifest metadata failure test.
- No physical disk-exhaustion or quota test.
- No process kill, crash/restart or recovery-idempotency test.
- No journal or verifier semantic change.
- No live soak or live-execution gate closure.

## 6. Open JH-4 items

- Failed flush and sync.
- Directory and manifest metadata failure.
- Rotation and rename failure.
- Physical storage exhaustion on an isolated volume.
- Kill/crash at record write, segment rotation and manifest update boundaries.
- Child-process restart and incomplete-run disposition.
- Recovery idempotency across the journal and manifest layers.
- Agreement among report, manifest, segment counts and verifier disposition.
