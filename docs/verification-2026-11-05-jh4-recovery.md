# Verification — JH-4 recovery, crash and metadata failure handling

Gate evidence for the third JH-4 storage-correctness group: manifest
publication, segment rename, directory synchronization, process termination and
conservative recovery.

- Baseline `main`: `c5a0b502cd4f3a598f5ef4ffab52362f1f87bb65`.
- Change scope: `src/journal.rs`, `src/cli.rs`, `src/main.rs`, their unit tests,
  the generated architecture test inventory and the JH-4 task list.
- This change adds a recovery command and fault-injection coverage. It does not
  change the journal record format, segment footer, checksum or SBE output.

## 1. Recovery contract

`journal recover <RUN>` classifies a run left by a terminated process.

- The caller MUST establish that the process which owned the run has exited.
  Recovery does not take a cross-process lease and must not race a live writer.
- The manifest is read and its schema version is checked.
- `Starting` and `Open` manifests become `Incomplete`. A missing failure reason
  is filled with a fixed explanation.
- `Incomplete`, `Failed` and `Finalized` manifests are left unchanged.
- A `.partial` segment or an unmanifested `.pmj` segment is never promoted into
  the run. It remains visible to the verifier as an error.
- Repeated recovery is idempotent because a terminal manifest is not rewritten.
- The command emits the same `JournalVerification` JSON as `journal verify` and
  does not convert an incomplete run into a complete one.

The command is for disposition, not salvage. A snapshot can restore a current
book; it cannot reconstruct the individual events that were never written.

## 2. Fault points

| Fault point | Position | Purpose |
|---|---|---|
| `ManifestWrite` | Before the initial, rotation and final manifest writes | Test manifest publication failure without changing the manifest file |
| `SegmentRename` | Before `fs::rename` of a finalized segment | Test rotation and rename failure |
| `AfterSegmentRename` | After the rename, before the manifest update | Terminate the process with a visible but uncommitted segment |
| `DirectorySync` | Before the directory `sync_all` | Test directory metadata failure |
| `AfterRecordWrite` | After a record is written and counted | Terminate the process at the record-write boundary |
| `AfterManifestUpdate` | After a rotation manifest write | Terminate the process after the manifest entry is durable |
| `AfterFinalManifestUpdate` | After the finalized manifest write | Terminate the process after commit, or inject a post-commit error |

`AfterFinalManifestUpdate` errors are ignored by `finalize_run` on purpose.
Once the finalized manifest is durable, a later hook error must not rewrite the
run as `Failed`. The subprocess test terminates the process directly.

## 3. Tests

| Test | Boundary | Assertion |
|---|---|---|
| `initial_manifest_failure_leaves_no_recoverable_run` | Initial manifest write | Startup returns an error, the run directory has no manifest, and no recoverable run is created |
| `rotation_rename_failure_is_failed_and_never_reported_complete` | Segment rename | The run is `Failed`, the segment remains `.partial`, and the verifier reports an unfinalized segment |
| `directory_sync_failure_leaves_an_uncommitted_segment` | Directory sync after rename | The run is `Failed`, and the renamed segment is absent from the manifest |
| `manifest_update_failure_does_not_commit_a_renamed_segment` | Rotation manifest write | The run is `Failed`, and the renamed segment is absent from the manifest |
| `final_manifest_write_failure_is_failed_and_not_complete` | Final manifest write | Shutdown returns the injected error, the run is `Failed`, and verification is incomplete |
| `crash_recovery_is_conservative_and_idempotent` | Record write, segment rename, manifest update | A terminated child leaves an `Open` manifest; recovery produces `Incomplete`, does not promote partial or orphan segments, and rewrites nothing on a second call |
| `crash_after_final_manifest_update_keeps_committed_run_finalized` | Final manifest update | The crashed run is `Finalized` and complete; recovery leaves the manifest unchanged |
| `post_commit_failure_does_not_downgrade_finalized_run` | Post-commit hook | A post-commit error does not change a finalized, complete run |
| `crash_child_process` | Test harness | Runs the child until the selected fault point calls `process::exit` |
| `journal_recover_command_requires_a_run_directory` | CLI parsing | `journal recover <RUN>` parses into the recovery command |

The child process is the test binary itself, run with `--exact` and a fault
point in an environment variable. `process::exit` bypasses Rust destructors and
the orderly shutdown path, leaving the files at the selected persistence
boundary.

## 4. Records that survive each boundary

| Boundary | Files after termination | Manifest state | Recovery disposition |
|---|---|---|---|
| Record write | `.partial` segment; manifest unchanged | `Open` | `Incomplete`; the partial segment is not promoted and the verifier reports it |
| Segment rename | Renamed `.pmj`; manifest unchanged | `Open` | `Incomplete`; the orphan remains absent from the manifest |
| Rotation manifest update | Renamed `.pmj`; manifest contains the segment | `Open` | `Incomplete`; the committed segment remains visible |
| Final manifest update | Finalized `.pmj`; manifest contains the segment and final state | `Finalized` | Unchanged; verification is complete |
| Initial manifest write | Empty run directory; no manifest | None | Startup fails; no recoverable run |
| Segment rename failure | `.partial` segment; failure manifest | `Failed` | `Failed`; verification is incomplete |
| Directory sync failure | Renamed `.pmj`; failure manifest without the segment | `Failed` | `Failed`; the orphan is absent from the manifest |
| Manifest publication failure | Renamed `.pmj`; failure manifest without the segment | `Failed` | `Failed`; the orphan is absent from the manifest |
| Final manifest failure | Finalized `.pmj`; failure manifest with the segment | `Failed` | `Failed`; verification is incomplete |
| Post-commit fault | Finalized `.pmj`; finalized manifest | `Finalized` | Unchanged; verification is complete |

Acceptance into the writer queue is not a durability acknowledgement. The
journal returns after enqueueing a record; the writer flushes and syncs on its
configured schedule or at finalization. This table describes the file-level
disposition once the writer reached the stated boundary. It does not claim that
queued but unwritten records survive a process crash.

Queue saturation and concurrent-producer latching remain covered by
`queue_saturation_then_shutdown_stays_failed`,
`shutdown_drains_saturated_queue_and_joins` and
`concurrent_producers_stop_after_failure`.

## 5. Gate results

Local results at the change commit:

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | Pass |
| `cargo clippy --all-targets --locked -- -D warnings` | Pass |
| `cargo test --all-targets --locked` | Pass: **237 library + 1 binary + 12 mock-exchange E2E + 2 replay = 252 passed**; 3 ignored |
| `cargo test --doc --locked` | Pass: 0 documentation tests |
| `cargo build --release --locked` | Pass |
| `python3 scripts/check_architecture.py --check` | Pass: 42 Rust source files, 255 attributed test functions, 42 files reachable from crate roots, 9 structurally checked Mermaid diagrams |
| Strict language audit excluding baseline JSON | Pass: exit 0 |

The required CI checks must also pass on the exact pull-request head. This
record does not claim that they have.

## 6. Non-goals and open items

- Directory creation and permission failure is not covered by this group.
- Physical disk or quota exhaustion on an isolated volume is not covered.
- Agreement among the run report, manifest, segment counts and verifier
  disposition is not fully tested at the report layer.
- The tests do not measure live Kalshi or Polymarket completeness, latency or
  recovery time.
- No live soak or live-execution gate is closed.

Remaining JH-4 items:

- Test directory creation and permission failure.
- Exercise quota or filesystem capacity exhaustion on an isolated volume.
- Verify report, manifest, segment counts and verifier disposition agree.
