# Verification — JH-4 directory failure, disk exhaustion and report agreement

Gate evidence for the fourth JH-4 storage-correctness group: journal directory
creation and permission failure, real filesystem exhaustion (ENOSPC) on an
isolated ext4 volume, and agreement among the run report, the manifest and the
verifier.

- Baseline `main`: `51bce2fa6f01bea591a854277b590ca0d4217e06`.
- Change scope: `src/journal.rs`, `src/report.rs`, `src/coordinator.rs`,
  `.github/workflows/ci.yml`, `scripts/verify-journal-enospc.sh`, the generated
  architecture test inventory and the JH-4 task list.
- The patch was supplied against a `9ab6cd5` snapshot; it was ported to the
  current tree. Two compile-time and one test-time integration defect were
  found and corrected during the port (see sections 3 and 5).

## 1. Directory and permission failure

`JournalHandle::start` creates the run directory with
`fs::create_dir_all`. Two failures are now exercised:

- A regular file occupying a path component makes `create_dir_all` fail; the
  error names the directory creation step and no run directory is created.
- A read-only parent directory (mode `0o500`) rejects creation with
  `Permission denied`. The check is skipped when the effective UID is root,
  because root bypasses directory mode bits.

The assertions run inside the existing
`payload_bytes_survive_write_and_read` test, so the test inventory is
unchanged. The non-writable-parent assertion is Unix-only.

## 2. Real filesystem exhaustion (ENOSPC)

`scripts/verify-journal-enospc.sh` creates a 128 MiB ext4 image with no
reserved blocks, mounts it on a loop device, preallocates an 80 MiB filler
file, and runs the journal unit test with `JOURNAL_ENOSPC_TEST_ROOT` and
`JOURNAL_ENOSPC_FILLER` set. The host journal test
`storage_write_failure_marks_run_failed` delegates to
`assert_real_filesystem_enospc`, which writes 32 KiB records until the
filesystem returns ENOSPC.

The test asserts:

- the run reaches `Failed` with a reason containing `No space left on
  device` or `ENOSPC`;
- an unfinalized `.partial` segment remains;
- `handle.shutdown` fails, so the run cannot finalize;
- `recover_incomplete_run` returns a non-complete verification in
  `Incomplete` or `Failed` state;
- recovery does not promote the partial segment;
- repeated recovery is idempotent.

The script requires Linux loop devices and `sudo` for mount and unmount. CI
runs it after the main test step. The local macOS run cannot execute it; its
result is established by the CI run on the exact pull-request head.

## 3. Report, manifest and verifier agreement

`validate_report_status` reconciles a `JournalStatus` against a
`JournalVerification`:

- run ID, state, segment count and finalized record count must be equal;
- a finalized report must have no error, a complete verification, reconciled
  queued/written/flushed/synced counters, a byte count equal to the verified
  segment bytes and a queue depth of zero;
- a non-finalized report must be paired with an incomplete verification.

`RunReport` now carries `journal_verification` and
`journal_consistency_error`. At shutdown the coordinator reads back the
manifest, validates the report status, marks the verification incomplete and
latches the shutdown error when the two disagree.

The finalized and failed cases are exercised in
`successful_shutdown_reconciles_counters` and
`storage_write_failure_marks_run_failed`. Each also checks that a mutated
report (an inflated finalized count, or a state downgrade) is rejected.

### Integration defect found during the port

The supplied patch compared the report's `last_error` string byte-for-byte to
the manifest's `failure` field. That comparison fails in the write-failure
test: the manifest preserves the first writer error ("simulated write
failure"), while the in-memory status is overwritten by a later symptom
("journal writer stopped") when the test attempts a second record after the
writer has exited. The free-form string is not a stable contract, so the
checker now requires only that a failed report records an error and that a
finalized report does not.

The supplied patch also called `journal::verify_run` in `src/coordinator.rs`
without an import or `crate::` qualifier, which does not compile in the
current tree. The calls now use `crate::journal`.

## 4. Gate results

Local results at the change commit:

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | Pass |
| `cargo clippy --all-targets --locked -- -D warnings` | Pass |
| `cargo test --all-targets --locked` | Pass: **237 library + 1 binary + 12 mock-exchange E2E + 2 replay = 252 passed**; 3 ignored |
| `cargo test --doc --locked` | Pass: 0 documentation tests |
| `cargo build --release --locked` | Pass |
| `bash -n scripts/verify-journal-enospc.sh` | Pass |
| `python3 scripts/check_architecture.py --check` | Pass: 42 Rust source files, 255 attributed test functions, 42 files reachable from crate roots, 9 structurally checked Mermaid diagrams |
| Strict language audit excluding baseline JSON | Pass: exit 0 |

The required CI checks must also pass on the exact pull-request head. This
record does not claim that they have.

## 5. Non-goals and open items

- The ENOSPC test requires Linux loop devices; it has not run on macOS.
- Acknowledged-record survival is documented in the third JH-4 verification
  record; this group adds the physical boundary.
- The live soak, and every live-execution, OMS, market-making and
  tenant-execution gate, remain closed.

Remaining JH-4 item:

- State precisely which acknowledged records survive each failure boundary.
