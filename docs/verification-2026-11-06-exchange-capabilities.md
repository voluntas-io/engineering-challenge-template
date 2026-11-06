# Verification — exchange capabilities at the adapter boundary

Evidence for finding 2 of the source audit: the collector state machine
identified a venue by name to decide whether a sequence clock was available.
The core now consumes an explicit capability value reported by the exchange
adapter.

- Pull request: [#14](https://github.com/kj111422/engineering-challenge-template/pull/14)
- Reviewed code commit: `cc363bbf95264b5b27d168de8c6b219880135e62`
- Baseline `main`: `3bd4834880cd32b315a905910b8fef845d43c3a0`
- Change scope: the exchange boundary, `CoreConfig`, the coordinator and replay
  composition roots, tests, and the architecture inventory. No state-machine
  decision changed.

## 1. Boundary change

`ExchangeCapabilities` carries the features the core may rely on. Both
adapters implement `Exchange::capabilities`; one mapping in the exchange module
keeps the live and replay paths aligned:

| Venue | Before | After |
|---|---|---|
| Kalshi | `settings.exchange == ExchangeKind::Kalshi` inside `CoreConfig::from_settings` | `ExchangeCapabilities { has_sequence_clock: true }` from the adapter |
| Polymarket | the same comparison produced `false` | `ExchangeCapabilities { has_sequence_clock: false }` from the adapter |

`CoreConfig::from_settings` now requires the capability value as an argument.
`src/collector/state.rs` no longer imports or matches `ExchangeKind`. The
coordinator passes `exchange.capabilities()` after constructing the adapter.
Replay has no adapter, so it calls the same exchange-module mapping for its
configured venue.

The `exchange` string in `CoreConfig` remains for reporting. It does not affect
a decision.

## 2. Regression evidence

New tests:

- `collector::state::tests::capabilities_are_not_derived_from_the_venue_name`
  configures Kalshi settings with a clockless capability and Polymarket
  settings with a clocked capability. The supplied capability survives in both
  cases, so the configured venue cannot override it.
- `exchange::tests::configured_venue_capabilities_are_explicit` checks the
  venue mapping used by replay.

The existing clocked and clockless state-machine tests continue to pass with
capabilities supplied explicitly.

## 3. Gate results

Local results at the change commit:

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | Pass |
| `cargo clippy --all-targets --locked -- -D warnings` | Pass |
| `cargo test --all-targets --locked` | Pass: **239 library + 1 binary + 12 mock-exchange E2E + 2 replay = 254 passed**; 3 ignored |
| `cargo test --doc --locked` | Pass: 0 documentation tests |
| `cargo build --release --locked` | Pass |
| `python3 scripts/check_matrix_tests.py` | Pass: 83 references resolve |
| `python3 -m unittest discover -s scripts -p 'test_*.py'` | Pass: 19 tests |
| `python3 scripts/check_architecture.py --check` | Pass: 42 sources, 257 test functions, 9 diagrams |
| Strict language audit excluding baseline JSON | Pass: exit 0 |
| Relative-link check | Pass: 72 links across 52 files, 0 missing |
| `git diff --check` | Pass |

Required pull-request checks on `cc363bb`:

| Context | Result | Run |
|---|---|---|
| Format, lint, test, release build | Pass, 2m11s | [42214329302](https://github.com/kj111422/engineering-challenge-template/actions/runs/42214329302) |
| Traceability and Mermaid validation | Pass, 8s | [42214329333](https://github.com/kj111422/engineering-challenge-template/actions/runs/42214329333) |
| Dependency Quality | Pass | FOSSA at `cc363bb` |
| License Compliance | Pass | FOSSA at `cc363bb` |
| Security Analysis | Pass | FOSSA at `cc363bb` |

## 4. Non-goals and open items

- The state-machine decisions are unchanged. The same venue reports the same
  capability value as before.
- `has_sequence_clock` is the only capability today. A future venue-specific
  feature belongs in the same struct and must not be inferred from the venue
  name in the core.
- Splitting the large modules (`journal.rs`, `collector/state.rs`, the E2E
  file) is finding 3 and remains a separate behaviour-preserving change.
- The live soak and every live-execution gate remain closed.
