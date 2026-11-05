# Financial-Market Systems Control Standard

## 1. Business and product boundary

The company provides B2B market-data and trading infrastructure to professional prediction-market firms. It is not positioned as a prediction-market exchange or a consumer betting service.

The first commercial product is limited to:
1. Live public market data.
2. Data normalization and continuity status.
3. Historical market records, inspection and replay.

Research tools and backtesting follow after data quality is established. Risk, OMS and execution follow after customer demand, venue rights, financial controls, and operational evidence are established. Multi-tenant customer services follow after isolation, identity, security, support and compliance controls are tested.

Pricing figures in the business strategy are hypotheses, not validated prices. Customer interviews, pilots, willingness to pay, retention, data redistribution rights and gross margins require independent evidence.

## 2. Market model

Keep these entities separate:

- `EconomicEvent`: the real-world proposition.
- `Outcome`: a possible result of the event.
- `ContingentClaim`: an economic claim tied to an outcome and resolution rule.
- `VenueListing`: a venue-specific market representation.
- `Instrument`: an explicitly tradable object exposed by a venue.
- `OutcomeConstraint`: a relationship that constrains combinations of outcomes.
- `CrossVenueEquivalence`: a reviewed relationship between listings; not inferred from similar text alone.

Store venue identifier scheme, contract generation, protocol version, signing domain version, settlement source, collateral asset, tick size, price ranges, minimum size, fees, quantity unit and lifecycle separately. A venue API version is not the same as an on-chain contract generation or an identifier encoding version.

## 3. Market-data controls

- Preserve raw application payloads before decoding or validation.
- Preserve the distinction between receive order, exchange event time, core processing order and journal order.
- Record connection epoch, source identity, receive sequence and clock provenance.
- Distinguish `Connected`, `BookSeeded`, `BookLive`, `Stale`, `Resyncing`, `Terminal` and any other approved state. Socket state alone does not establish book validity.
- Do not infer feed completeness from a checksum-valid journal.
- Treat source sequence gaps, clock uncertainty, stale metadata, unknown event types and conflicting replicas as explicit conditions.
- Recovery must obtain authoritative state and apply a documented fence before declaring a book valid.
- Store snapshots or reconciliation observations with venue, instrument version, timestamps and comparison outcome.
- Do not silently sort receive-order data by exchange timestamp; preserve each ordering dimension.

## 4. Research and backtesting controls

Every experiment must pin:

- Source commit and strategy revision.
- Dataset manifest and content digest.
- Venue and instrument metadata versions.
- Fee schedule and fee calculation version.
- Price, quantity, rounding and collateral assumptions.
- Clock model and event ordering.
- Fill, queue position, partial-fill, cancellation, latency and slippage models.
- Resolution and settlement assumptions.
- Train, validation and holdout time windows.
- Random seeds and simulation versions.

Backtests MUST account for fees, spread, slippage, queue priority uncertainty, partial fills, cancellations, market suspension, stale data, latency, adverse selection, inventory, market resolution and settlement risk where relevant. Avoid look-ahead, survivorship, selection and leakage bias. Report gross and net P&L, drawdown, turnover, exposure, fill ratio, markout, fee attribution and uncertainty. A backtest is evidence under stated assumptions; it is not a promise of profit.


## 5. Model risk and strategy governance

Treat every signal, fair-value estimate, execution model, queue-position model, fill simulator and market-making policy as a versioned model with an explicit owner and operating scope. Apply model controls according to purpose, complexity, exposure, decision authority and potential loss. Distinguish statistical/economic/financial models from deterministic software rules; both require correctness and change controls, but not every deterministic rule is a statistical model. Use the revised April 17, 2026 Interagency Supervisory Guidance on Model Risk Management (SR-26-2) as a reference for model inventory, materiality, effective challenge, development, validation, outcome analysis, ongoing monitoring, governance and third-party model oversight. That guidance is directed to banking organizations, is most relevant to organizations above $30 billion in assets, and is not automatically binding on this project or its customers. Obtain legal advice for applicability. Its stated scope excludes generative and agentic AI models; govern coding agents and generative systems under separate AI and software-supply-chain controls.

- Maintain a model inventory with purpose, owner, version, data lineage, deployment locations, dependencies, assumptions, limitations and approval status.
- Separate research code, backtest artifacts, paper configurations and live configurations. A research result must not acquire order authority by configuration alone.
- Pin source commit, data manifest, feature definitions, parameter set, random seeds, fee schedule, venue metadata and simulator version for every experiment.
- Validate conceptual soundness, implementation correctness, input data, output behavior, sensitivity and limitations before a model is used for a decision.
- Use independent review for models that influence capital, order price, position size, exposure or market-making quotes.
- Monitor realized outcomes against declared assumptions. Track calibration, residuals, markouts, drift, fill error, slippage, drawdown and failure rates.
- Define model change classes. Material changes to data, features, labels, training windows, optimization objectives, cost assumptions or execution logic require revalidation.
- Prohibit look-ahead, data leakage, survivorship bias, post-selection reporting and hidden manual overrides.
- Record model approval, restrictions, validation evidence, rollback version and monitoring thresholds.
- Disable the model or reduce its authority when input quality, drift, venue conditions or validation limits breach declared thresholds.

A backtest is not independent validation of its own model. A profitable historical result does not prove that a strategy has an economic edge after live execution costs.

## 6. Market integrity and conduct

Market access must enforce venue rules and applicable law at the order boundary. Controls must be defined for the venue, instrument, account, user type and jurisdiction.

- Define self-trade prevention and account relationships before order submission.
- Detect duplicate or circular orders, wash-trading patterns, spoofing, layering, manipulative quote behavior, prohibited coordination and abusive cancel/replace rates where applicable.
- Preserve order intent, risk decision, venue submission, acknowledgement, cancellation, execution and operator action records.
- Detect orders inconsistent with venue tick sizes, price bands, quantity rules, market status, trading hours or eligibility.
- Define controls for market suspension, resolution, disputed outcomes and exceptional venue announcements.
- Maintain escalation paths for suspected market abuse, venue-rule violations and customer misuse.
- Separate monitoring alerts from findings. A detected pattern is not automatically a legal conclusion.
- Obtain legal/compliance review before enabling surveillance rules, reporting duties or customer market access for a regulated activity.

## 7. Order and execution controls

Before any live order can be submitted:

- Strategy produces a typed `OrderIntent`; it has no direct venue authority.
- Independent pre-trade risk validates account, tenant, instrument, price, size, exposure, rate, market state, eligibility and limits.
- Risk reservation is atomic with the decision to authorize the intent.
- OMS persists intent and state before submission under a documented ordering contract.
- Venue submission uses an idempotency/reconciliation strategy.
- Timeout after possible acceptance becomes `Unknown`, not `Rejected`.
- Unknown order state blocks conflicting exposure until reconciliation.
- Every acknowledgement, rejection, partial fill, fill, cancel, replace and expiry is persisted and reconciled.
- Cancellation and replace races have defined state transitions.
- Risk reservation release is tied to explicit order and fill events.
- Kill switch exists outside strategy control and blocks new exposure.
- Signing service holds keys and validates authorization; strategies do not read private keys.

The system must distinguish local acceptance, venue acknowledgement, execution/fill, clearing, payout determination and settlement finality.

## 8. Risk controls

At minimum define and test:

- Per-order notional and quantity limits.
- Per-instrument and per-outcome position limits.
- Gross and net exposure limits.
- Account and tenant capital limits.
- Concentration and correlated-outcome limits.
- Daily loss and drawdown thresholds.
- Price collars and stale-price rejection.
- Maximum order and cancel rates.
- Liquidity and market-depth requirements.
- Venue-specific fees, collateral and settlement constraints.
- Counterparty, venue and operational dependency limits.
- Market suspension and resolution controls.

Limits must have authoritative owners, units, effective times, change records and an emergency override policy. An override requires authentication, authorization, audit, expiry and independent approval proportionate to the documented risk level.

## 9. Ledger, collateral and settlement

- Maintain an append-only double-entry ledger for financial movements.
- Each posting transaction must balance within the defined currency/asset and precision rules.
- Use explicit accounts for cash, collateral, positions, fees, realized P&L, unsettled receivables/payables and adjustments as applicable.
- Fills create the defined trade and fee entries; settlement is a separate lifecycle.
- Corrections use reversals or compensating entries; do not mutate historical postings.
- Idempotency keys prevent duplicate posting.
- Positions and balances must be derivable from ledger entries and reconcilable against authoritative records.
- Settlement finality must be defined per venue and claim type.
- Unresolved discrepancies enter explicit states and require controlled resolution.
- Custody, withdrawal, and signing authority must be separated from strategy and analytics processes.

## 10. Market-making controls

Market making requires a separate risk model from directional trading. Define:

- Inventory, delta, concentration and capital limits.
- Quote price and size constraints.
- Spread floors and tick-size rules.
- Quote lifetime and freshness.
- Queue-position model and assumptions.
- Cancellation latency and replacement behavior.
- Adverse-selection and markout measurements.
- Volatility and event-risk response.
- Fee/rebate treatment.
- Inventory skew and hedging policy.
- Market suspension, resolution and settlement handling.
- Kill conditions and independent quote cancellation.

Paper simulation must model queue uncertainty and adverse selection; fills must not be assumed from touch alone.

## 11. B2B service and tenant controls

Before serving external customers:

- Authenticate users and services.
- Authorize every API and administrative action.
- Isolate tenant market access, private data, credentials, accounts, orders, positions, limits, ledgers and audit logs.
- Test cross-tenant read, write and inference attempts.
- Define customer data ownership, license rights, retention and deletion.
- Define service availability, support response, incident notification and data correction policies.
- Define customer-specific rate limits and capacity isolation.
- Define export, termination and recovery paths.
- Keep platform operators' access audited and least-privileged.

Public venue data is not automatically redistributable. Obtain and record the relevant venue terms, data licenses and customer rights before offering feeds commercially.

## 12. Regulatory applicability register

Maintain a dated register with jurisdiction, activity, legal entity, customer type, venue, instrument classification, applicable authority, source, effective date, obligations, owner, counsel disposition and next review date.

The following are reference frameworks, not automatic classifications:

- CPMI-IOSCO PFMI: principles for financial market infrastructures within their scope, including risk management, settlement finality and operational risk.
- SEC Rule 15c3-5: market-access risk controls for covered broker-dealers and market access arrangements.
- SEBI retail algorithmic-trading framework: requirements and implementation standards for covered Indian securities-market activities, broker/member and retail-algo arrangements. SEBI issued the February 4, 2025 circular and a September 30, 2025 timeline extension. NSE's implementation page was updated May 27, 2026; verify current exchange circulars before any covered activity.
- CFTC event-contract framework: CFTC staff issued prediction-market/event-contract advisories in 2026, including Letter 26-08 (March 12, 2026), Letter 26-27 (September 22, 2026), and an August 12, 2026 advisory on DCM incentive-program self-certifications. These materials concern designated contract markets and their obligations; they do not establish that a data vendor or software provider is itself a DCM. Review product scope and venue relationships with counsel.
- Venue terms: eligibility, API use, rate limits, market conduct, data use, custody and settlement rules.

Obtain legal and compliance advice before customer order routing, handling customer assets, offering execution, redistributing venue data, or entering a new jurisdiction. Do not design technical routing to bypass legal, geographic, identity, account or venue restrictions.

## 13. Release gates for financial authority

| Gate | Required evidence | Authority granted |
|---|---|---|
| Data service | JH-4, replay, completeness limits, live soak, data rights, customer isolation | Commercial data service within approved scope |
| Research service | Reproducible datasets, fee model, time-split controls, simulation validation | Research outputs, no order authority |
| Paper OMS | Order state, risk simulation, ledger, reconciliation, crash tests | Simulation only |
| Restricted execution | Legal/compliance approval, independent risk, signer, kill switch, reconciliation, canary and incident controls | Limited live orders under written limits |
| Customer execution | Tenant isolation, customer authorization, segregation, service controls, legal approval and independent assurance | Customer execution within approved service perimeter |

The current repository has no OMS, risk service, signer, double-entry ledger, customer identity, tenant isolation or settlement engine. No financial authority is granted by the existence of this document.
