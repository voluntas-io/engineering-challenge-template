# Verification — 2026-11-01 journal failure hardening

Gate evidence for the journal failure-path hardening on branch
`journal-failure-hardening`, which extends `polymarket-continuity-failure`.

- Last source commit of this gate: `ef1d0e6`
  (`Journal every application payload at the receive boundary`), preceded by
  `e2d8fb6` (`Latch journal failures across producers, writer, and
  finalization`) and `4f4b11e` (`Bound journal shutdown and abort by a single
  deadline`).
- This record is added by a documentation-only follow-up commit. Source and
  test files at that commit are identical to `ef1d0e6`. Pull-request
  acceptance additionally requires both GitHub Actions workflows to pass on
  the exact pull-request head; the run links are recorded in the pull-request
  description.
- Baseline for the diff: `d5ee18b5f9ff8039843149ba804b8cd212ce46c7`, whose
  gates are recorded in
  [`verification-2026-10-30-continuity.md`](verification-2026-10-30-continuity.md).

Environment: macOS (arm64), Rust toolchain pinned by `rust-toolchain.toml`.
The supplied FIX SBE schema and SBE output are unchanged in this work.

## Audit findings closed

| ID | Finding | Resolution and evidence |
|---|---|---|
| H1 | Queue saturation only marked the in-memory status; a subsequent normal `shutdown` could finalize the run as complete, hiding a rejected frame | Producer admission closes before finalization; a latched failure is observed by the writer before every command; `finalize_run` refuses a failed run; `mark_failed` never overrides `Finalized`; the source worker turns a rejected record into a fatal stop. Evidence: `journal::tests::queue_saturation_then_shutdown_stays_failed`, `concurrent_producers_stop_after_failure`, `successful_shutdown_reconciles_counters` |
| H2 | `shutdown`/`abort` used a blocking channel send for the control command, so a saturated queue with a blocked writer could exceed the configured timeout | One absolute deadline at method entry bounds producer quiescence, command delivery (`send_timeout`), the response wait, and writer-thread observation. Deadline expiry returns an error and never claims finalization. Evidence: `journal::tests::blocked_writer_bounds_shutdown`, `blocked_writer_bounds_abort`, `finalize_reply_timeout_stays_incomplete`, `repeated_concurrent_and_dead_writer_shutdowns_are_bounded` |
| H3 | `SessionFailure::Protocol` carried no raw payload and did not advance `receive_seq`; several Kalshi branches returned transport-style idle frames after reading application payloads | Protocol is a struct variant that preserves the exact payload and receive timestamp; the worker journals every read payload through one ingress path (`journal_ingress`) that advances the fence exactly once before any decode or protocol decision; Kalshi ignored outcomes are explicit `Ignored` records with untouched bytes. Evidence: E2E `journal_accounts_for_every_delivered_payload`, `rejected_payloads_are_journaled_across_reconnects`, `exchange::kalshi::session::tests::protocol_rejection_preserves_raw_payload`, `stale_snapshot_is_reported_as_ignored_payload` |

## Gate results

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | Pass |
| `cargo clippy --all-targets --locked -- -D warnings` | Pass |
| `cargo test --all-targets --locked` | Pass: **217 library tests + 11 mock-exchange E2E tests + 2 replay integration tests = 230 passed**, 3 ignored |
| `cargo test --doc --locked` | Pass: 0 documentation tests |
| `cargo build --release --locked` | Pass |
| `python3 scripts/check_architecture.py --check` | Pass: 42 Rust source files, 233 attributed test functions, 42 files reachable from crate roots, 9 structurally checked Mermaid diagrams |

The count grows from 220 (baseline `d5ee18b`) by the journal hardening tests:
five new or rewritten journal unit tests for the failure latch and bounded
shutdown, two Kalshi adapter tests, and two mock-exchange E2E scenarios.

The journal E2E scenarios were run repeatedly (70+ consecutive passes across
development) to check for flakiness. One intermittent failure mode was
investigated and understood: a frame the mock writes at the moment a replica
tears its socket down can be a transport loss. The strict
sent-versus-journaled equality is therefore asserted only in a scenario with
no reconnect (`journal_accounts_for_every_delivered_payload`, guarded by a
connection-stability wait), while the reconnect scenario asserts per-payload
guarantees — exact bytes, outcomes, and contiguous per-generation sequences —
without a strict count.

## Failure-state and bounded-shutdown evidence

| Plan check | Test |
|---|---|
| Saturation through the production shutdown path stays failed and verifies incomplete | `journal::tests::queue_saturation_then_shutdown_stays_failed` |
| Concurrent producers cannot treat a latched journal as healthy | `journal::tests::concurrent_producers_stop_after_failure` |
| Successful shutdown reconciles queued = written = flushed = synced = finalized, empty queue, verifier count | `journal::tests::successful_shutdown_reconciles_counters` |
| Write and sync faults remain non-finalized | `journal::tests::storage_write_failure_marks_run_failed`, `storage_sync_failure_marks_run_failed` (existing, extended by the latch) |
| Blocked writer with a saturated queue bounds `shutdown` | `journal::tests::blocked_writer_bounds_shutdown` |
| Blocked writer with a saturated queue bounds `abort` | `journal::tests::blocked_writer_bounds_abort` |
| Finalize accepted but never answered stays incomplete | `journal::tests::finalize_reply_timeout_stays_incomplete` |
| Repeated, concurrent, and dead-writer shutdowns are bounded | `journal::tests::repeated_concurrent_and_dead_writer_shutdowns_are_bounded` |

## Ingress coverage evidence

| Plan check | Test |
|---|---|
| Sent == journaled exactly, contiguous sequences, deltas plus unknown/control/empty frames | E2E `journal_accounts_for_every_delivered_payload` |
| Malformed JSON and binary payloads journaled with exact bytes and rejection categories across reconnect generations | E2E `rejected_payloads_are_journaled_across_reconnects` |
| Protocol rejection preserves the exact payload | `exchange::kalshi::session::tests::protocol_rejection_preserves_raw_payload` |
| Stale snapshot is an explicit ignored record | `exchange::kalshi::session::tests::stale_snapshot_is_reported_as_ignored_payload` |
| Existing journal record format and ordering still hold | `journal::tests::{payload_bytes_survive_write_and_read,rejected_and_unknown_frames_are_recorded,checked_in_fixtures_round_trip_byte_for_byte,record_order_matches_source_epoch_and_sequence,record_order_is_checked_across_segments,identical_input_produces_identical_records,graceful_shutdown_finalizes_rotated_segments}` |
| Coordinator still finalizes and verifies a live-path run | E2E `journal_finalizes_and_verifies_on_graceful_shutdown` |

The capture boundary is documented in
[`raw-frame-journal.md`](raw-frame-journal.md): every application payload is
journaled before parsing or protocol decisions; transport-level idle events
are out of scope; protocol rejects carry the category `protocol_violation`.

## Scope and limits

- No strategy, authentication, order management, order submission, or live
  order transmission exists in this change. The SBE schema and output are
  unchanged.
- The failure-state, bounded-shutdown, and ingress-coverage contracts are
  verified by deterministic fault injection and mock-exchange scenarios.
- A live-feed soak with segment rotation and a storage-exhaustion run on an
  isolated volume or quota remain open operational work (JH-4). Neither has
  been executed in this gate; no feed-completeness claim is made anywhere in
  this record. A checksum-valid journal proves the integrity of recorded
  payloads, not that the venue delivered every event.
- SHA-256 checksums detect corruption and truncation; they do not
  authenticate the journal against a malicious rewrite of both payload and
  checksums. This is documented in `raw-frame-journal.md`.
- Disk exhaustion is represented by write/sync fault injection; no physical
  filesystem was filled during this run.

## Reproduction

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
cargo test --doc --locked
cargo build --release --locked
python3 scripts/check_architecture.py --check
```

Journal-specific tests:

```bash
cargo test --lib journal::
cargo test --lib exchange::kalshi::session
cargo test --test e2e_mock_exchange journal
```

Mock-exchange tests bind loopback ports only and do not contact an exchange.
