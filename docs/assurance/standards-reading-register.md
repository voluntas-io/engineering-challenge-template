# Standards Reading and Applicability Register

**Status:** Initial source inventory. Full standards review is outstanding.
**Review date:** 2026-11-02.
**Owner:** Engineering lead, with legal/compliance owner for regulatory interpretation.

This register distinguishes source discovery from complete review. A link or search result is not evidence that the complete publication has been read. Each source must be reviewed at the applicable level, its requirements extracted, and its applicability decided by an accountable owner.

## NASA software engineering

| Source | Scope | Project extraction task | Review status |
|---|---|---|---|
| NASA-HDBK-2203 Version D | NASA software engineering and assurance handbook | Read the full applicable handbook; build chapter-to-project control map for requirements, architecture, implementation, verification, configuration, operations, maintenance and retirement | Introduction and SWE-061-related guidance screened; full handbook reading outstanding |
| NPR 7150.2D | NASA software engineering requirements over lifecycle | Read all requirements, applicability/class rules, tailoring, verification and maintenance sections; record source requirements rather than summaries alone | Purpose, scope and validity dates checked; full requirement-by-requirement review outstanding |
| NASA-STD-8739.8B | NASA software assurance, software safety and IV&V | Read standard; extract assurance, safety, risk, IV&V, supplier and evidence controls; determine which practices map to financial software | Version B and active status checked; full text review outstanding |
| SWE-061 | Coding standards | Adopt language-, usage- and severity-classified project rules; define legacy-code maintenance policy; verify by review and tools | Guidance reviewed |
| SWE-060 | Coding software | Map design-to-code implementation, unit tests and static analysis practices to repository workflow | Read requirement identifier and summary; full guidance review outstanding |
| SWE-135 | Static analysis | Define tools, result triage, false-positive disposition and analysis evidence | Requirement and guidance reviewed at summary level |
| SWE-062 | Unit testing | Define repeatability, unit boundaries, test data, and coverage policy by assurance tier | Requirement and repeatability guidance reviewed at summary level |
| SWE-186 | Unit test repeatability | Extract repeatability controls, fixtures, seeds, and environment capture | Full review outstanding |
| SWE-219 | MC/DC for safety-critical software | Determine whether and where decision logic requires MC/DC; do not claim this applies automatically to the project | Full review outstanding; applicability decision required |

Important status note: NPR 7150.2D lists an expiration date of 2027-03-08. Re-check NASA's official standards page for a successor before using it as a current baseline after that date. NASA-HDBK-2203 Version D is a guidance handbook; NASA's standards catalogue identifies it as active and not a mandatory NASA standard. NASA-STD-8739.8 Version B is identified as active and mandatory within NASA's system. These classifications do not make NASA requirements binding on this project.

## Secure software development and supply chain

| Source | Project use | Review status |
|---|---|---|
| NIST SP 800-218 SSDF v1.1 | Secure development practices across preparation, protection, production and vulnerability response | Publication and practice family screened; complete task mapping outstanding |
| NIST SP 800-161 Rev. 1 Update 1 | Supplier, dependency, build and software supply-chain risk | Publication and update date checked; implementation profile outstanding |
| NIST SP 800-53 Rev. 5 | Security control catalogue, selected controls only | Not yet reviewed; optional mapping after threat model |
| NIST Cybersecurity Framework 2.0 | Governance and cybersecurity risk management | Not yet reviewed; map ownership, measurement and review |
| SLSA provenance | Build provenance and artifact integrity | Not yet reviewed; select target level based on build pipeline |

## Financial markets and prediction markets

| Source | Scope boundary | Project extraction task | Review status |
|---|---|---|---|
| CPMI-IOSCO Principles for Financial Market Infrastructures | Financial market infrastructures within PFMI scope | Review governance, risk, settlement finality, custody, operational risk, access, disclosures and recovery | Official PFMI overview screened; full principles and applicability review outstanding |
| SEC Rule 15c3-5 (17 CFR 240.15c3-5; https://www.ecfr.gov/current/title-17/section-240.15c3-5) | Covered broker-dealers with market access | Extract pre-trade risk and supervisory control concepts only if a covered business/relationship exists | Rule identified; legal applicability not determined |
| SEBI Circular SEBI/HO/MIRSD/MIRSD-PoD/P/CIR/2025/0000013 (2025-02-04) | Indian retail investor algorithmic trading through covered broker/member arrangements | Review scope, broker/vendor responsibilities, access controls, registration, order controls and audit requirements with counsel | Circular located; scope review outstanding |
| SEBI Circular SEBI/HO/MIRSD/MIRSD-PoD/P/CIR/2025/132 (2025-09-30) | Timeline extension for 2025 retail algo framework | Record changed dates and dependencies | Published extension located; legal effect to be confirmed against later exchange circulars |
| NSE algorithmic-trading procedures | Exchange implementation details; page updated 2026-05-27 | Re-check applicable 2026 circulars and client/API requirements before any covered NSE activity | Current page screened; underlying circulars not fully reviewed |
| CFTC Staff Advisory 26-08 (2026-03-12; https://www.cftc.gov/csl/26-08/download) | Staff advisory to designated contract markets concerning event-contract listing/trading | Review market integrity, contract design, manipulation risk and DCM responsibilities if product/venue perimeter enters US derivatives activity | Advisory located and summary screened; full legal analysis outstanding |
| CFTC Staff Advisory 26-27 (2026-09-22; https://www.cftc.gov/csl/26-27/download) | Staff views on mention-market event contracts at DCMs | Review settlement verifiability, manipulation and contract-specific analysis for markets in scope | Advisory located and summary screened; full legal analysis outstanding |
| CFTC Staff Advisory on incentive programs (2026-08-12; https://www.cftc.gov/PressRoom/PressReleases/9282-26) | DCM self-certification of market-maker, liquidity, trading and incentive programs | Review implications for market-making program design and venue rules when applicable | Advisory located and summary screened; full legal analysis outstanding |
| FIX Protocol and FIX Session Layer | Electronic trading message and session conventions | Study ordered streams, sequence numbers, resend/gap fill, duplicate handling, session recovery, order state and execution reports; do not force FIX onto non-FIX venue APIs | Session overview and 2020 session document screened; full versioned protocol study outstanding |

## Model risk management for quantitative and financial models

| Source | Scope | Project extraction task | Review status |
|---|---|---|---|
| Interagency Supervisory Guidance on Model Risk Management (SR-26-2; 2026-04-17; https://www.federalreserve.gov/frrs/guidance/supervisory-guidance-on-model-risk-management.htm) | Banking organizations; most relevant to organizations above $30 billion in assets, with tailored treatment based on size, complexity and model exposure | Use as a reference for model inventory, model materiality, conceptual soundness, independent challenge, out-of-sample/out-of-time testing, outcomes analysis, ongoing monitoring, governance, and vendor model oversight. Determine legal applicability separately. | Current 2026 guidance located and reviewed at section level; full control-by-control mapping outstanding |
| SR 11-7 (2011) | Prior interagency model-risk guidance | Do not use as the current baseline. Preserve references only in historical records; migrate current controls to SR-26-2 where relevant. | Superseded by the April 17, 2026 revised interagency guidance |

**Scope and interpretation:** SR-26-2 is supervisory guidance for banking organizations, not a generally applicable regulation for this software vendor or every trading firm. Its stated model definition covers complex quantitative methods applying statistical, economic or financial theories; simple arithmetic and deterministic rule-based processes without such theories are excluded. The guidance also states that generative and agentic AI models are outside its scope. Apply its lifecycle concepts to project research and strategy models by analogy, with rigor proportionate to model purpose, materiality, exposure and use. Maintain separate assurance controls for AI coding agents, AI-generated code and generative/agentic systems. Do not describe this adaptation as regulatory compliance.

## JPL coding guidance

| Source | Project use | Review status |
|---|---|---|
| Holzmann, The Power of 10 (2006) | Finite set of analyzable safety-critical coding rules for C | Paper summary and abstract screened; full rule-by-rule adaptation outstanding |
| McDougall and Holzmann, Experience Using the Power of 10 | Lessons from applying rules in practice | Not yet reviewed |

The Power of 10 targets C and safety-critical code. It must not be adopted wholesale in Rust. Adopt bounded control flow, explicit allocation budgets, bounded, reviewable functions, limited shared state, input validation, and checked return/error handling where they fit Rust and async systems. Do not prohibit streaming loops solely because they lack a compile-time iteration count; require cancellation, resource, timeout and backpressure contracts.

## Required reading workflow

For each source:

1. Obtain the complete official publication and record its version/date/hash where possible.
2. Read scope, applicability, definitions, requirements, tailoring, verification and maintenance sections.
3. Separate binding requirements from guidance and examples.
4. Extract atomic controls with source section and exact reference.
5. Classify each control as adopt, adapt, not applicable, or legal review required.
6. Record project rationale and owner.
7. Map adopted controls to `REQ-*`, `THR-*`, `HAZ-*`, `ADR-*`, `INV-*`, `TEST-*` and `OPS-*` identifiers.
8. Add verification evidence and periodic review date.
9. Review source revisions and supersession status on a defined schedule.

## Legal interpretation rule

Engineering must not decide that an entity, instrument, data product, market maker, broker, execution agent or customer platform is regulated or exempt. Maintain a jurisdiction/activity matrix and obtain qualified legal advice before external data redistribution, customer account access, order routing, holding assets, signing, settlement, or market-making incentives. A technical architecture control does not itself create regulatory compliance.
