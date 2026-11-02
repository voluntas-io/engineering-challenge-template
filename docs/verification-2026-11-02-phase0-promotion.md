# Verification — 2026-11-02 Phase 0 branch promotion

**Status:** Phase 0 promotion review record. Documentation only; this record
changes no runtime behavior.  
**Frozen charter baseline:** `df87cb1c207a06e87e29a2cae8cd3b77e110a67e`
(`Adopt event-market platform engineering charter`).  
**Promotion source:** `polymarket-continuity-failure`.  
**Promotion target:** `main`.  
**Baseline `main` before promotion:** `9ab6cd5a584fea58c0086efa50d55c796e9aac4e`.  
**Reviewed source tip:** `df87cb1c207a06e87e29a2cae8cd3b77e110a67e`.

The promoted tip is the documentation-only commit that adds this record. Its
parent is `df87cb1`, so the frozen charter baseline remains an ancestor of
`main` and the charter file content is unchanged.

## 1. Promotion decision

`main` may be fast-forwarded to the promotion tip. The reviewed range is a
coherent collector-hardening sequence:

- Polymarket feed-contract fixtures and decoder coverage;
- continuity/failover state and receive-order fencing;
- the optional raw-frame journal and its failure, shutdown, and verification
  paths;
- the engineering charter and documentation navigation.

The range does not add order management, strategy evaluation, execution,
authentication, tenant behavior, or live order transmission. It changes no
dependency, SBE schema, CI workflow, deployment unit, or toolchain file. The
journal remains disabled unless `journal.directory` is configured.

Promotion is a fast-forward only. It does not create a merge commit, rewrite
history, or force-push.

## 2. Commit inventory

`main..polymarket-continuity-failure` contains 18 commits:

| Commit | Subject | Workstream |
|---|---|---|
| `0235b10` | P1: Polymarket feed-contract fixtures and decoder tests | Feed contract |
| `ce61a34` | P1.1: Polymarket continuity and failover state machine | Continuity |
| `20b4a01` | Merge branch `p1/polymarket-feed-contract` into `polymarket-continuity-failover` | Integration |
| `36a0cff` | P1.1: Polymarket continuity and failover state machine (integrated with P1) | Continuity |
| `74140c4` | P1.1: Fix continuity gate - resolve_incident for Stale, block deltas in non-Live states | Continuity |
| `ce6f243` | P1.1: continuity gate implementation and regression tests | Continuity |
| `13bc7e0` | P1.1: fence clockless recovery on acknowledged receive order | Continuity |
| `4362283` | Add checksummed raw-frame journal for public data frames | Journal |
| `d5ee18b` | Record journal verification evidence and update test counts | Journal evidence |
| `e2d8fb6` | Latch journal failures across producers, writer, and finalization | Journal failure paths |
| `4f4b11e` | Bound journal shutdown and abort by a single deadline | Journal failure paths |
| `ef1d0e6` | Journal every application payload at the receive boundary | Journal ingress |
| `39b2eb3` | Document journal hardening contracts and pin gate evidence | Journal evidence |
| `08e92bf` | Close journal admission, receive-fence, and abort review gaps | Journal review closure |
| `5924eb6` | Record follow-up journal review gate evidence | Journal evidence |
| `e19a85c` | Merge pull request #2 from `kj111422/journal-failure-hardening` | Integration |
| `df87cb1` | Adopt event-market platform engineering charter | Charter |
| _this record_ | Record Phase 0 promotion audit for charter baseline `df87cb1` | Promotion record |

## 3. Exact diff inventory

`git diff --numstat main..polymarket-continuity-failure`:

| Area | Files | Added | Deleted |
|---|---:|---:|---:|
| Rust source | 21 | 5,792 | 365 |
| Rust tests | 3 | 638 | 13 |
| Fixtures | 9 | 139 | 0 |
| Documentation | 30 | 2,817 | 121 |
| Scripts | 1 | 24 | 5 |
| Config examples | 2 | 26 | 0 |
| Root `README.md` | 1 | 78 | 20 |
| **Total** | **67** | **9,514** | **524** |

No binary files are in the range. The only new Rust module is
`src/journal.rs` (2,960 lines, including its test module). The changed source
files are:

| File | Material change |
|---|---|
| `src/journal.rs` | New opt-in raw-frame journal: manifest, rotated checksummed segments, verification, bounded shutdown/abort, failure latch, test-only fault injection |
| `src/collector/state.rs` | Continuity states, discontinuity incidents, recovery requests/fencing, readiness projection |
| `src/session.rs` | Receive fence stamped before decode, payload journaling at ingress, protocol/decode failures preserve raw frames |
| `src/exchange/polymarket/codec.rs` | Snapshot, multi-asset price change, tick size, lifecycle, best bid/ask, unknown additive event handling |
| `src/exchange/polymarket/session.rs` | Connection/receive sequencing and control-frame handling |
| `src/exchange/kalshi/session.rs` | Payload-preserving failures and explicit ignored/stale records |
| `src/config.rs` | `[journal]` settings, `Settings::for_exchange`, capture-config SHA-256 excluding credentials |
| `src/coordinator.rs` | Journal startup/finalization and recovery-ready worker event |
| `src/cli.rs`, `src/main.rs` | `--journal-dir`, `journal verify`, journal status in the run report |
| `src/metrics.rs`, `src/model.rs` | Continuity states, counters/gauges, readiness checks, receive-fence metadata |
| Remaining source files | Small API/comment adjustments required by the above |

Not changed: `Cargo.toml`, `Cargo.lock`, `.github/`, `sbe_schemas/`,
`deploy/`, `rust-toolchain.toml`, `clippy.toml`.

## 4. Behavioral delta and risk review

- **Journal is opt-in.** `JournalSettings::default().directory` is `None`.
  Enablement requires `journal.directory`, `--journal-dir`, or
  `COLLECTOR_JOURNAL_DIR`. Replay and inspection commands do not write it.
- **SBE compatibility is preserved.** The supplied schema is unchanged. The
  only `aligned_sbe_writer` change is a documentation comment.
- **Readiness is stricter.** New readiness checks fail for unseeded,
  resyncing, stale-detail, and terminal symbols. A connected socket no longer
  implies a live book. This is intentional and covered by continuity tests.
- **Failure behavior is explicit.** Queue saturation, write failure, sync
  failure, and finalization timeout leave the journal run failed/incomplete
  rather than claiming durability.
- **No trading authority was added.** The range contains no OMS, strategy,
  signer, ledger, authenticated order route, or live execution path.
- **Intermediate CI was not uniformly green.** Several work-in-progress
  commits failed CI and were fixed by later commits. Promotion rests on the
  reviewed tip and the merge commit, not on the assumption that every
  intermediate push passed. Section 5 records this explicitly.

## 5. CI evidence

| Ref | Workflow | Result | Run |
|---|---|---|---|
| `13bc7e0` | CI | Pass | <https://github.com/kj111422/engineering-challenge-template/actions/runs/40522906788> |
| `13bc7e0` | Architecture documentation | Pass | <https://github.com/kj111422/engineering-challenge-template/actions/runs/40522907015> |
| `4362283` | CI | Pass | <https://github.com/kj111422/engineering-challenge-template/actions/runs/40956405372> |
| `4362283` | Architecture documentation | Pass | <https://github.com/kj111422/engineering-challenge-template/actions/runs/40956405414> |
| `39b2eb3` | CI | Pass | <https://github.com/kj111422/engineering-challenge-template/actions/runs/41250035808> |
| `39b2eb3` | Architecture documentation | Pass | <https://github.com/kj111422/engineering-challenge-template/actions/runs/41250035894> |
| `5924eb6` | CI | Pass | <https://github.com/kj111422/engineering-challenge-template/actions/runs/41447322076> |
| `5924eb6` | Architecture documentation | Pass | <https://github.com/kj111422/engineering-challenge-template/actions/runs/41447322106> |
| `e19a85c` | CI | Pass | <https://github.com/kj111422/engineering-challenge-template/actions/runs/41449135053> |
| `e19a85c` | Architecture documentation | Pass | <https://github.com/kj111422/engineering-challenge-template/actions/runs/41449135084> |
| `df87cb1` | CI | Pass | <https://github.com/kj111422/engineering-challenge-template/actions/runs/41456653043> |
| `df87cb1` | Architecture documentation | Pass | <https://github.com/kj111422/engineering-challenge-template/actions/runs/41456652900> |

`36a0cff`, `74140c4`, and `ce6f243` have failing intermediate CI runs;
their fixes are contained in later commits and are not used as promotion
evidence. `0235b10`, `ce61a34`, `20b4a01`, `e2d8fb6`, `4f4b11e`, `ef1d0e6`,
and `08e92bf` have no branch-level run because they were pushed in batches or
superseded before a run was recorded; the tree at `39b2eb3` and `5924eb6`
covers their content. The promotion tip must additionally pass both workflows
on its exact pushed SHA; that result is recorded in the pull request or push
run for the record commit.

## 6. Local verification at `df87cb1`

Environment: macOS (arm64), Rust 1.99.1 pinned by `rust-toolchain.toml`,
release profile for the binary. Run on 2026-11-02.

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | Pass |
| `cargo clippy --all-targets --locked -- -D warnings` | Pass |
| `cargo test --all-targets --locked` | Pass: **221 library + 12 mock-exchange E2E + 2 replay integration = 235 passed**, 3 ignored |
| `cargo test --doc --locked` | Pass: 0 documentation tests |
| `cargo build --release --locked` | Pass |
| `python3 scripts/check_architecture.py --check` | Pass: 42 Rust source files, 238 attributed test functions, 42 files reachable from crate roots, 9 structurally checked Mermaid diagrams |
| `git diff --check` | Pass |

Ignored tests:

- `tests/e2e_mock_exchange.rs::opt_in_scale_run` requires `SCALE_SYMBOLS` and
  `--ignored`.
- `tests/live_polymarket.rs::discovers_and_subscribes_to_public_polymarket_books`
  and `resubscribing_an_asset_returns_a_fresh_book` require
  `RUN_LIVE_TESTS=1` and public network access.

## 7. Promotion procedure

The promotion is a fast-forward, performed only after this review:

```bash
git fetch origin
test "$(git rev-parse main)" = "$(git rev-parse origin/main)"
git merge-base --is-ancestor origin/main polymarket-continuity-failure
git switch main
git merge --ff-only polymarket-continuity-failure
git push origin main
```

`AGENTS.md` remains untracked and outside every commit. No merge commit,
rebase, reset, force-push, or history rewrite is used. If the fast-forward
check fails, promotion stops and the range is re-reviewed.

## 8. Open gates and non-claims

This record promotes code; it does not close the remaining operational gates.

**JH-4 journal failure cases still open:**

| Case | Current evidence | Status |
|---|---|---|
| Short writes | Writes use `write_all`, but there is no test-only partial-write injection | Open |
| Failed writes | `journal::tests::storage_write_failure_marks_run_failed` | Covered |
| Failed syncs | `journal::tests::storage_sync_failure_marks_run_failed` | Covered |
| Disk exhaustion | Write/sync fault injection only; no physical filesystem or quota exhaustion run | Open |
| Recovery | Truncated record, partial segment, corrupt manifest, and checksum tests | Covered at verifier level |
| Process restart | No restart-after-crash run; unfinalized runs are reported incomplete | Open |

Also open:

- A live Polymarket soak with segment rotation has not been run.
- Physical disk-exhaustion and power-loss behavior have not been exercised.
- SHA-256 checksums detect corruption and truncation; they do not
  authenticate the journal against a malicious rewrite of payload and
  checksums.
- A valid journal proves the integrity of recorded payloads, not that the
  venue published or delivered every event.
- Live trading remains disabled. Green CI and a successful documentation
  check do not prove storage durability or trading readiness.
- No OMS, market making, strategy, tenant execution, or live order work is
  authorized by this promotion.

**Next gate:** storage correctness (JH-4), then live soak, then release review.
