# Verification

This page records reproducible gate evidence, the exact commit each result
belongs to, and the boundaries of what was not tested. It replaces the dated
verification reports that were consolidated here. The complete pre-restructure
documentation remains available at annotated tag
`archive/docs-full-2026-11-06`.

## Environment

- macOS 27.0.1, arm64 (Apple Silicon).
- `rustc 1.99.1 (58cf8f9c3 2026-10-13)`, pinned by `rust-toolchain.toml`.
- `cargo 1.99.1 (5f94df478 2026-08-27)`.
- Python 3.14.8 for the repository gates.
- Linux CI runs on `ubuntu-24.04` with the same pinned Rust toolchain.

## Commit provenance

| Reference | Full SHA | Meaning |
|---|---|---|
| `423d510` | `423d510705e1ae77bbd37b9870dbe8a7a308127b` | Last code-bearing baseline (PR #14 merge) |
| `6a0e9a7` | `6a0e9a77729614e80e0664044d6093c5df585d4d` | PR #15 tested head |
| `0425da5` | `0425da50e4cc99a2bf7860546fae329a38d86a18` | PR #15 squash-merge commit on `main` |

PR #15 contains three commits: `afd7a9e`, `cdf8312`, and `6a0e9a7`. `main`
requires linear history, so the PR was squash-merged. The tested head and its
parents are therefore not ancestors of `main`; the original commit history is
available in [PR #15](https://github.com/kj111422/engineering-challenge-template/pull/15).
The squash-merged tree at `0425da5` is byte-identical to the tested head
`6a0e9a7`.

The documentation commits did not change `src/`, `tests/`, `Cargo.toml`,
`Cargo.lock`, or `sbe_schemas/`; the Rust tree remains identical to `423d510`.

## Gate results

The Rust commands were run locally at `423d510` and re-run after the journal
tests moved to `src/journal/tests.rs` and the collector state tests moved to
`src/collector/state/tests.rs`; the production code is unchanged. The
matrix and architecture gates were run on the same tree:

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | Pass |
| `cargo clippy --all-targets --locked -- -D warnings` | Pass |
| `cargo test --all-targets --locked` | **254 passed, 0 failed, 3 ignored**: 239 library, 1 binary, 12 mock-exchange E2E, 0 live, 2 replay |
| `cargo test --doc --locked` | Pass: 0 documentation tests |
| `cargo build --release --locked` | Pass |
| `python3 scripts/check_architecture.py --check` | Pass: 44 Rust source files, 44 reachable from crate roots, 3 structurally checked Mermaid diagrams |
| `python3 scripts/check_matrix_tests.py` | Pass: 90 file-qualified references across 14 requirements; all 24 mandatory evidence tests present |
| `python3 -m unittest discover -s scripts -p 'test_*.py'` | Pass: 24 gate tests |
| `python3 -m unittest discover -s scripts/assurance -p 'test_*.py'` | Pass: 16 assurance-tool tests |

The three ignored tests are one opt-in mock scale scenario and two public-feed
live tests. The scale scenario is exercised separately with `SCALE_SYMBOLS=N`;
the live tests require `RUN_LIVE_TESTS=1`.

## Merged change evidence

The counts below come from the CI logs at the named commits. They are not
transferred from an earlier or later tree.

| Change | Exact commit | Rust result | Matrix gate at that commit |
|---|---|---|---|
| PR #13, matrix references | `3bd4834880cd32b315a905910b8fef845d43c3a0` | 252 passed, 3 ignored | 83 references under the old checker: 52 qualified, 19 bare, 1 file-qualified, 9 wildcard, 2 E2E helper |
| PR #14, exchange capabilities | `423d510705e1ae77bbd37b9870dbe8a7a308127b` | 254 passed, 3 ignored | 83 references under the old checker: same composition |

CI and architecture evidence:

- PR #13 head `fd3aae3`: [Rust CI 42212780235](https://github.com/kj111422/engineering-challenge-template/actions/runs/42212780235), [architecture 42212780213](https://github.com/kj111422/engineering-challenge-template/actions/runs/42212780213).
- PR #13 merge `3bd4834`: [Rust CI 42213027930](https://github.com/kj111422/engineering-challenge-template/actions/runs/42213027930), [architecture 42213027883](https://github.com/kj111422/engineering-challenge-template/actions/runs/42213027883).
- PR #14 head `fdf8873`: [Rust CI 42214714043](https://github.com/kj111422/engineering-challenge-template/actions/runs/42214714043), [architecture 42214714020](https://github.com/kj111422/engineering-challenge-template/actions/runs/42214714020).
- PR #14 merge `423d510`: [Rust CI 42215032966](https://github.com/kj111422/engineering-challenge-template/actions/runs/42215032966), [architecture 42215032887](https://github.com/kj111422/engineering-challenge-template/actions/runs/42215032887).

PR #15, documentation restructuring:

- Tested head `6a0e9a7`: [Rust CI 42217760852](https://github.com/kj111422/engineering-challenge-template/actions/runs/42217760852), [architecture 42217760920](https://github.com/kj111422/engineering-challenge-template/actions/runs/42217760920).
- Squash merge `0425da5`: [Rust CI 42218633639](https://github.com/kj111422/engineering-challenge-template/actions/runs/42218633639), [architecture 42218633828](https://github.com/kj111422/engineering-challenge-template/actions/runs/42218633828).

The PR #15 runs report 254 passed and 3 ignored, the matrix gate resolves 90
references across 14 requirements with all 24 mandatory evidence tests present,
and the architecture gate accepts 42 Rust source files, 42 reachable files, and
3 diagrams. The squash-merge tree is byte-identical to the tested head, so the
post-merge runs exercised the same content.

The PR #13 and PR #14 architecture logs report 42 source files, 42 files
reachable from crate roots, 255 attributed test functions at `3bd4834` and 257
at `423d510`, and 9 structurally checked Mermaid diagrams. The current
documentation tree reports 3 diagrams because the historical diagrams were
replaced.

The old matrix checker counted 83 references while allowing bare names,
wildcards, groups, and E2E helper names. The current checker rejects those
forms and requires `src/...rs::test` or `tests/...rs::test`; it currently
resolves 90 references and requires all 24 mandatory evidence tests.

## Journal storage evidence

The JH-4 work added deterministic storage-failure coverage. The behaviour is
part of the current tests and does not depend on a live feed.

- **Short writes.** `AlignedSbeWriter::write_uncommitted` retries short writes,
  advances `committed`, `bytes_written`, and the optional SHA-256 by each
  accepted range, and reports a non-empty `Ok(0)` as a write failure. A partial
  write followed by a retry preserves the complete stream.
- **Flush and sync failure.** A failed flush or sync is reported as a failure,
  not as a successful write. Bytes accepted before the failure remain pending;
  the writer records a fatal failure, and the journal latches it across
  producers, the writer, and finalization.
- **Recovery and crash.** `journal recover <RUN>` classifies a run left by a
  terminated process. `Starting` and `Open` manifests become `Incomplete`; a
  missing reason is filled with a fixed explanation. Recovery is conservative:
  a partial segment or an open manifest is not presented as a complete run.
- **Directory and permission failure.** A regular file occupying a path
  component makes `create_dir_all` fail, and the error names the directory.
- **Disk exhaustion.** `scripts/verify-journal-enospc.sh` creates an isolated
  ext4 loop device and fills it to force ENOSPC. This test runs only on the
  Linux CI runner; macOS cannot create the loop device. The CI logs at
  `3bd4834` and `423d510` show `1 passed; 0 failed` for the step. The run
  report, manifest, and verifier agree on the failed state.

The journal has not been soaked on a live feed, filled a production volume, or
exercised a power-loss boundary. Retention and disk-watermark policies are not
implemented. A checksum-valid journal proves the integrity of recorded
payloads, not that the venue delivered every event.

## Live and scale observations

These are short public-feed or in-process measurements. They are not a long
soak, a controlled benchmark, or a full-universe subscription claim.

- Polymarket full-universe keyset discovery: 2,746 pages, 548,942 unique
  tokens, final page with 78 records and no `next_cursor`, 9m52s, zero
  warnings.
- 90-second, ten-token, two-shard release capture: 493 SBE messages
  (33 snapshots, 460 deltas); `verify` clean.
- A pristine template reader decoded 346/346 messages from a live capture.
- Four Extension 4 latency observations, each 60 seconds with five tokens and
  two replicas. They are single observations; source-port and Nagle
  explanations remain hypotheses. Raw reports are in `measurements/`.
- 1,000-symbol loopback E2E: 4 shards, 2 replicas, 8 WebSockets, 250 symbols
  per socket, 2,000 subscription entries; every book matches the mock and
  `verify` is valid.
- Opt-in scale run at 100,000 symbols: 20 shards, 40 connections, 200,000
  subscriptions, 5.053s total, 696,448 KB RSS, 25.33 MB output.
- Live two-shard run: 6 symbols, 2 sources, 32 SBE messages, 0 errors.

## Not established

- 24-hour continuous live soak.
- Full-universe simultaneous live subscription.
- Authenticated Kalshi live validation.
- Production order entry.
- OMS and independent pre-trade risk.
- Live execution safety.

## Addendum (2026-11-13, branch `fix/kalshi-merged-subscribe-acks`)

This branch fixes a Kalshi protocol-folding defect found while cross-checking
the adapter against the venue's documented message set: a repeated
`subscribe` on one connection merges into the connection's single
`orderbook_delta` sid and is acknowledged with `ok` (carrying `sid`, `seq`,
and the full `msg.market_tickers` list), not a fresh `subscribed`. The
adapter now resolves those acknowledgements into the tracked subscription;
see design decision 10. Four regression tests cover the merged-`ok` path,
the no-sid fallback, the authoritative full-list replacement, and a reused
`subscribed` sid. Gate results re-run on this branch:

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | Pass |
| `cargo clippy --all-targets --locked -- -D warnings` | Pass |
| `cargo test --all-targets --locked` | **311 passed, 0 failed, 3 ignored**: 274 library, 1 binary, 12 mock-exchange E2E, 9 inspect, 10 numeric-model, 2 replay, 3 session-recovery |
| `cargo test --doc --locked` | Pass: 1 documentation test |
| `python3 scripts/check_architecture.py --check` | Pass: 46 Rust source files, 46 reachable from crate roots |
| `python3 scripts/check_matrix_tests.py` | Pass: 128 file-qualified references across 14 requirements; all 24 mandatory evidence tests present |
| `python3 -m unittest discover -s scripts -p 'test_*.py'` | Pass: 24 gate tests |
| `python3 -m unittest discover -s scripts/assurance -p 'test_*.py'` | Pass: 16 assurance-tool tests |

The fix is verified against unit-level mock-server scenarios only; Kalshi
live validation remains blocked without credentials. No gate above was
transferred from an earlier tree.
