# Security, eligibility, capital and operations

## 1. Trust boundaries

### Process roles

| Process | Permissions | Explicitly denied |
|---|---|---|
| Collector/ingest | Public market data, discovery, raw/core log append | Trading keys, account withdrawal, strategy configuration writes |
| Research/backtest | Read authorized historical partitions, create derived datasets | Live order route, signing credentials |
| Strategy | Read allowed market/portfolio views, emit typed intents | Change risk limits, write ledger, sign orders, query secrets |
| Risk | Read market, account, position, order and eligibility state; approve/deny and reserve risk | Sign orders or create venue orders directly |
| OMS | Manage order lifecycle, query/cancel/submit through approved executor | Private signing keys, risk-policy edits |
| Signer | Sign an exact validated digest under fixed policy | Arbitrary transactions, market-data mutation, strategy evaluation |
| Ledger/reconciliation | Append postings from verified events; query venue truth through read-only credentials | Generate strategy intents or sign orders |
| Platform control plane | Tenant, roles, quota, access, audit and retention policy | Cross-tenant raw data visibility by default |

For the initial single-operator stage these can be modules and separate binaries. Before any live order, the risk/OMS/signer boundaries must be enforced by OS identity, network policy, secret scopes and process permissions—not only Rust module visibility.

## 2. Signer boundary

- No private key in collector, research, backtest or strategy process.
- Signer accepts a typed request containing account, venue, chain/environment, listing, order fields, nonce/deadline and a risk authorization/reservation reference.
- Signer independently validates the digest domain, contract/address allowlist, amount/price bounds, account and policy generation.
- It refuses arbitrary calldata or an unknown signing domain.
- It records request hash, key identifier/version, policy decision, signature result and correlation IDs. It never logs the secret.
- Key custody uses a KMS/HSM or equivalent isolated signing boundary. Rotation and emergency revocation are tested.
- Kalshi RSA-PSS and Polymarket EIP-712 are venue implementations behind the signer interface. They are not exposed as strategy concerns.

## 3. Kill switch

The kill switch must not depend on the strategy process. It is monotonic in authority:

- `RUNNING` → `HALT_NEW_RISK` → `CANCEL_OPEN_ORDERS` (venue-specific) → `RECONCILE` → `LOCKED`.
- An emergency stop denies new reservations and submissions immediately.
- Cancellation is a best-effort external effect and may time out. Unknown open orders remain reserved.
- Restart does not clear a kill-switch state without an audited human action and a new policy generation.
- Test process death, network partition, signer outage, database outage, expired credentials, queue saturation and venue timeout.

## 4. Eligibility and jurisdiction

Eligibility is a typed, versioned decision for an account/tenant, venue, instrument and time. It is a hard prerequisite to order creation.

- No geo-routing or network circumvention to bypass venue policy.
- No assumption that public-data access implies permission to trade.
- Store minimal decision outputs and evidence references; avoid putting raw KYC/identity documents or unnecessary personal data into the event log.
- Expired, missing or contradictory eligibility returns `Unknown` and denies trading.
- Legal interpretation, residency/account constraints and venue terms require an authorized human/compliance owner and qualified counsel. The software enforces the policy; it does not create permission.

## 5. Account and ledger reconciliation

Reconcile continuously and at every session boundary:

- Open orders and order states.
- Fills and trade IDs.
- Positions by listing/claim.
- Balances by asset/network.
- Locked collateral and pending conversions/transfers.
- Fees, rebates and settlement movements.
- Resolution/dispute state.

A reconciliation record stores venue observation time, receive time, raw response reference, account/listing scope, diff and decision. Any unexplained difference sets the affected scope to `RECONCILIATION_HOLD`; the risk service denies new exposure until the break is resolved or formally accepted under policy.

## 6. Market-making controls

Market making needs stronger inventory and execution controls than simple directional trading:

- Worst-case exposure across all outcomes of an event, not only per token.
- Open-order exposure included before fill.
- Queue position is uncertain unless the venue provides sufficient evidence. Simulators report a distribution or conservative bound, not a precise guessed fill rank.
- Fee, rebate, tick, maker/taker role and match-time fee semantics are evaluated per fill.
- Latency and adverse selection are measured from receive → decode → reduce → risk → submit → venue acknowledgement → fill.
- Quote withdrawal follows kill state, stale data, inventory limits, collateral shortfall, protocol drift and event-specific policy.
- Inventory caps include correlated event/theme exposure and settlement concentration.

## 7. Platform multi-tenancy

Do not expose internal account or market state by default across customers.

- Tenant-scoped auth, data access, encryption context, retention and export.
- API schemas are versioned contracts with explicit backwards-compatibility rules.
- Rate limits and storage quotas per tenant; quota exhaustion becomes an explicit service state.
- Dataset manifests and completeness certificates are customer-visible evidence.
- Audit every read of restricted raw payloads, every configuration/policy change and every export.
- Separate customer strategy code execution from platform processes. Treat customer code as untrusted.

## 8. Storage and operations

Before a sustained soak, implement:

- Disk low-watermark and high-watermark alerts.
- Retention rules that never delete an unfinalized, referenced or legally held segment.
- A restart scanner for partial segments and missing/corrupt manifests.
- Compression chosen by measurements and versioned in the manifest.
- A documented recovery procedure for full disk, permission failure, fsync failure, process kill and power loss.
- Backup/restore drill with independent verification.
- Runbooks with alerts that are proven to fire during fault injection.

The live collector must expose: accepted/rejected/unknown frames, queue depth, append latency, fsync latency, source sequence continuity, reconnects, stale duration, recovery attempts, time regressions, disk watermarks and completeness state.

## 9. Evidence policy

A test run report pins:

- Git commit and clean/dirty state.
- Rust toolchain, target and dependency lock hash.
- Config digest with secret fields removed.
- Venue, protocol version and account mode (public/paper/live).
- Data/fixture manifest and schema versions.
- Commands, test totals, ignored tests and exit status.
- Hardware/OS for performance and soak runs.
- Fault schedule and injected failure points.
- Known failures and untested dimensions.

Never carry test counts from one commit to another. A static inventory count is not a passing test count.
