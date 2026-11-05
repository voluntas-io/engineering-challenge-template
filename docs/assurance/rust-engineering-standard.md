# Rust Engineering Standard

**Normative language:** `MUST`, `MUST NOT`, `SHOULD`, `SHOULD NOT`, and `MAY` have their RFC 2119 meanings when written in uppercase.

## 1. Compilation and toolchain

- The repository MUST pin the Rust toolchain and commit `Cargo.lock` for applications.
- CI MUST use `--locked` for Cargo operations.
- CI MUST run formatting and Clippy with warnings denied.
- Build features MUST be explicit and tested. A feature that changes serialization, persistence, financial arithmetic, security, or runtime behavior requires a separate test configuration.
- The minimum supported Rust version MUST be declared if the project promises one. The CI toolchain and MSRV are distinct declarations.
- Release profiles and target triples MUST be recorded in release evidence.

## 2. Types and units

- Domain identifiers MUST use newtypes when values from different namespaces could be confused: `VenueId`, `TenantId`, `AccountId`, `InstrumentId`, `OrderId`, `ExecutionId`, `LedgerTransactionId`.
- Time values MUST encode their clock and unit in the type or field name. Wall-clock time, monotonic time, exchange event time, and receive time MUST remain distinct.
- Prices MUST identify the quote asset and base unit. Quantities MUST identify the asset or contract unit.
- Sequence numbers MUST identify their scope: connection, source, venue, journal, or core-processing order.
- A raw integer or string MUST NOT cross a domain boundary without validation and conversion into the relevant type.

## 3. Financial arithmetic

- Financial amounts MUST NOT use `f32` or `f64` as their authoritative representation.
- Money, price, quantity, fee, notional, balance and position arithmetic MUST define scale, unit, precision, overflow, underflow, rounding and conversion behavior.
- Use checked integer/fixed-point operations for authoritative values.
- Every rounding operation MUST identify mode and stage; fee rounding and price rounding are not interchangeable.
- Conversion between venue-native and canonical values MUST be explicit and preserve source value, conversion rule, and provenance.
- Tests MUST cover zero, minimum, maximum, negative values where permitted, scale mismatch, overflow, rounding boundaries and invalid units.

## 4. Errors and control flow

- Runtime failures MUST use typed errors or structured error categories.
- Errors MUST retain causal context without logging secrets or private payloads.
- Code MUST NOT discard a result with `let _ =` when the result affects persistence, order state, risk reservation, recovery, or accounting.
- `unwrap()` and `expect()` MUST NOT process untrusted network, disk, configuration, or customer input on runtime paths. They MAY be used in tests or after a locally proven invariant with an explanatory message.
- Fallback behavior MUST be explicit, tested, and safe for the assurance tier. An unknown price, state, account, or order outcome MUST NOT silently become zero, empty, valid, or accepted.
- Error conversion MUST preserve distinctions needed for retry, operator action, and incident classification.

## 5. State machines and invariants

- Lifecycle state MUST be represented as an enum or an equivalently closed type where feasible.
- State transitions MUST be centralized or delegated to a named reducer.
- Each transition MUST define preconditions, guards, state mutation, emitted effects, error outcome and idempotency.
- Invalid transitions MUST be rejected or recorded as explicit protocol violations.
- A state name MUST not imply evidence absent from the state definition. For example, `Connected` MUST NOT mean `BookValid`.
- State-machine tests MUST cover every transition, guard failure, repeated event, reordered event and terminal state.

## 6. Concurrency and async execution

- Every shared mutable state MUST have a documented synchronization owner.
- Channels, queues, frame sizes, buffers, retry counts, reconnect loops, task counts and shutdown waits MUST be bounded.
- Backpressure behavior MUST be explicit: block, reject, shed, persist, or fail. Silent loss is prohibited for evidence and financial events.
- Cancellation MUST have a documented effect on locks, reservations, queue admission, writes, order submission and finalization.
- Tasks that own sockets, journal writers, risk reservations, orders or ledger operations MUST have supervised lifetimes.
- Detached tasks MUST NOT own financial or persistence authority.
- Lock acquisition order MUST be documented where multiple locks can be held concurrently.
- Use Loom or an equivalent model checker for finite synchronization protocols with tractable state spaces; do not claim that unit tests exhaust all interleavings.

## 7. Unsafe Rust

- `unsafe` MUST be prohibited by default at crate level unless a reviewed exception is approved.
- Each unsafe block MUST have a `// SAFETY:` comment that states the exact invariant established by the caller and relied on by the operation.
- Unsafe abstractions MUST expose a safe interface where possible.
- Unsafe code MUST have focused tests, boundary tests, and Miri coverage where supported.
- A safety comment MUST describe a proof obligation, not restate the operation.
- FFI and memory-mapped I/O require explicit layout, lifetime, aliasing, alignment, ownership and failure analysis.

## 8. Parsing, protocol and serialization

- Network and file input MUST be treated as untrusted.
- Parsers MUST enforce size, nesting, count, numeric range and allocation bounds.
- Unknown additive protocol fields MAY be ignored only where the venue contract permits it; unknown event types MUST have an explicit policy and recordable disposition.
- A decoded value MUST preserve the raw payload or durable provenance where forensic reconstruction is required.
- Serialization formats MUST be versioned and have compatibility fixtures.
- Canonicalization MUST NOT erase venue-native semantics needed for execution or reconciliation.
- Deserialization MUST reject invalid financial scales, units, identifiers and enum states.

## 9. Persistence and durability

- APIs MUST distinguish accepted, enqueued, written, flushed, synced and finalized states.
- Durability promises MUST name the persistence boundary and failure model.
- A crash after acknowledgement MUST have a documented outcome and test.
- Partial writes, sync errors, directory metadata errors, rename failures, truncation and corrupt manifests MUST be tested.
- Recovery MUST be deterministic and idempotent.
- A checksum MUST NOT be described as authentication, authenticity, or feed completeness.
- Readers MUST bound memory use; verification of large captures SHOULD use streaming readers.
- Persistent schema changes require an ADR and compatibility/migration tests.

## 10. Security and dependencies

- Secrets MUST NOT enter logs, public market-data journals, fixtures, reports or error text.
- Credential material MUST be scoped to the minimum required capability and lifetime.
- Dependency changes MUST record purpose, version, license, maintenance status, known vulnerabilities and supply-chain provenance.
- Prefer maintained protocol clients and official conformance vectors over custom cryptographic or signing implementations.
- Do not add cryptography; if required, use a maintained library and independent review.
- CI actions MUST be pinned to full commit SHA. Workflow permissions MUST be least privilege.

## 11. Observability and performance

- Logs MUST be structured and must include stable correlation identifiers without secrets.
- Metrics MUST have bounded label cardinality.
- Metrics MUST define unit, aggregation, sampling behavior and reset semantics.
- Latency measurements MUST identify start and end boundaries, clock source and percentile calculation.
- Benchmarks MUST pin commit, toolchain, hardware, workload, input data, warmup, repetitions and measurement method.
- Performance claims MUST distinguish mock, replay, public-feed and production workloads.

## 12. Bounded-complexity guidance adapted from JPL

JPL's Power of 10 was written for C in a safety-critical context. It is a source of analyzability principles, not a Rust standard and not a universal rule set for this repository. Apply the principles where they reduce unbounded behavior or make invariants reviewable:

- Control flow MUST have explicit bounds or an explicit cancellation/resource budget.
- Recursion MUST NOT process untrusted or unbounded input. Prefer iterative parsing with explicit depth limits.
- Dynamic allocation MUST have an admission or capacity policy on ingestion, storage, and execution paths.
- Functions SHOULD have one responsibility and a reviewable control-flow structure. Use measured complexity and change risk rather than an arbitrary line-count ceiling.
- Preconditions, postconditions and state invariants MUST be stated at trust boundaries.
- Shared mutable state SHOULD be minimized; ownership and synchronization must be explicit.
- Build and test configurations MUST cover feature flags and platform-specific code that affect behavior.
- Assertions MUST check invariants, not replace input validation or recoverable error handling.
- Every long-running loop MUST define termination, shutdown, timeout, and backpressure behavior.
- Error handling MUST not silently continue after loss of data, authority, or accounting consistency.

For market-data and financial workloads, a bounded loop or allocation policy must not cause silent frame loss. Backpressure, rejection, storage admission and incomplete-run behavior must be explicit.

## 13. Required project gates

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
cargo test --doc --locked
cargo build --release --locked
python3 scripts/check_architecture.py --check
git diff --check
```

Additional tests are required by the assurance tier and change impact: property, fuzz, Miri, Loom, fault-injection, differential, security, benchmark and operational tests. These commands are a baseline, not a complete assurance argument.
