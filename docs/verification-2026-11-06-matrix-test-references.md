# Verification — requirement-matrix test references

Evidence for finding 1 of the source audit: the requirement-to-test matrix
cited test names that no longer existed, and no gate detected the drift. This
change corrects the references and adds a CI check that resolves them against
the test harness inventory.

- Pull request: [#13](https://github.com/kj111422/engineering-challenge-template/pull/13)
- Reviewed code commit: `5f104d1ef4af91d20f9953f2639728b7028c05d3`
- Baseline `main`: `a465707b55b888d9a35f103051c5766d6f51c4b4`
- Change scope: the matrix, the living README and assurance index, two CI
  workflows, and the new checker with its fixtures. No Rust behavior changed.

## 1. References corrected

The audit named four stale references. The checker found two additional
module misattributions and several short forms that were not fully qualified.

| Location | Before | After |
|---|---|---|
| M8 | `coordinator::tests::{explicit_shard_count_must_satisfy_the_per_socket_bound,automatic_sharding_derives_the_required_count}` | `capacity::tests::{...}` |
| M8 | `collector::state::tests::shadow_replica_takes_over_after_source_loss` | `collector::state::tests::clocked_shadow_replica_takes_over_after_source_loss` |
| M9, E1, cross-cutting | `failover_snapshot_anchors_resumed_deltas_and_preserves_receive_times` | `clocked_failover_anchors_resumed_deltas_and_preserves_receive_times` |
| E1 | `sequence_reset_fails_over_to_valid_shadow` | `collector::state::tests::clocked_sequence_reset_fails_over_to_valid_shadow` |
| E2 | `stale_shadow_snapshot_does_not_emit_after_source_loss` | `unacknowledged_shadow_snapshot_does_not_emit_after_source_loss` |

The fourth replacement was verified against the source. The existing test
sends a shadow snapshot after a sequence-gap reset and asserts that it does not
emit until the replacement source is acknowledged. It is the only test with
that behavior; no test named for a stale shadow snapshot remains.

The journal status is also corrected. It previously said no physical
disk-exhaustion test existed, which stopped being true when the isolated ext4
ENOSPC test landed in `a465707`.

## 2. Checker behavior

`scripts/check_matrix_tests.py`:

- reads only table columns whose heading contains "evidence";
- extracts backticked references and ignores implementation paths, config
  keys and prose;
- expands group forms such as `module::tests::{first,second}`;
- resolves qualified, file-qualified, wildcard and bare names against
  `cargo test --all-targets -- --list`;
- resolves `E2E assert_*` references against helper definitions in the
  integration-test sources;
- requires a bare name to resolve to exactly one test, and a wildcard to match
  at least one test;
- exits non-zero with the matrix line number for every unresolved or ambiguous
  reference.

The checker reports the reference mix: 83 references resolve as 52 qualified,
19 bare, 1 file-qualified, 9 wildcard and 2 E2E helper. The wildcards cover
whole modules (`journal::tests::*`, `verify::tests::*`, `orderbook::tests::*`,
and similar) and each must match at least one test. Historical audit and
verification documents are not rewritten.

## 3. Gate results

Local results at the change commit:

| Command | Result |
|---|---|
| `python3 scripts/check_matrix_tests.py` | Pass: 83 references resolve |
| `python3 -m unittest discover -s scripts -p 'test_*.py'` | Pass: 19 tests |
| `cargo fmt --all -- --check` | Pass |
| `cargo clippy --all-targets --locked -- -D warnings` | Pass |
| `cargo test --all-targets --locked` | Pass: **237 library + 1 binary + 12 mock-exchange E2E + 2 replay = 252 passed**; 3 ignored |
| `cargo test --doc --locked` | Pass: 0 documentation tests |
| `cargo build --release --locked` | Pass |
| `python3 scripts/check_architecture.py --check` | Pass: 42 sources, 255 test functions, 9 diagrams |
| Strict language audit excluding baseline JSON | Pass: exit 0 |
| Relative-link check | Pass: 71 links across 51 files, 0 missing |
| `git diff --check` | Pass |

Required pull-request checks on `5f104d1`:

| Context | Result | Run |
|---|---|---|
| Format, lint, test, release build | Pass, 2m10s | [42212171530](https://github.com/kj111422/engineering-challenge-template/actions/runs/42212171530) |
| Traceability and Mermaid validation | Pass, 9s | [42212171465](https://github.com/kj111422/engineering-challenge-template/actions/runs/42212171465) |
| Dependency Quality | Pass | FOSSA at `5f104d1` |
| License Compliance | Pass | FOSSA at `5f104d1` |
| Security Analysis | Pass | FOSSA at `5f104d1` |

## 4. Non-goals and open items

- The Rust core still imports `ExchangeKind` in `CoreConfig::from_settings`.
  Finding 2 (`ExchangeCapabilities`) is a separate change.
- Module-level wildcards remain where the evidence covers a whole suite. They
  are visible in the checker output and cannot resolve to zero tests.
- The bare-name heuristic treats a snake_case inline-code span in an evidence
  column as a test reference. A future evidence cell that cites a configuration
  key alone must qualify it or include its value so the span is not mistaken
  for a test name.
- The live soak and every live-execution gate remain closed.
