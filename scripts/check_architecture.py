#!/usr/bin/env python3
"""Check source/test inventory drift and architecture traceability without dependencies.

Run from any working directory. --update rewrites generated inventory
files after a reviewed source/test change; --check is read-only and is used by CI.
This is a structural/traceability gate, not a substitute for Mermaid's syntax parser.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs/architecture.md"
SOURCE_INVENTORY = ROOT / "docs/architecture/source-inventory.md"
TEST_INVENTORY = ROOT / "docs/architecture/test-inventory.md"

# Every tracked Rust source module must have an assigned responsibility.
SOURCE_RESPONSIBILITIES = {
    "src/cli.rs": "clap command model and CLI-to-settings overrides",
    "src/config.rs": "configuration defaults, deserialization, and validation",
    "src/coordinator.rs": "startup, orchestration loop, discovery reconciliation, controls, shutdown",
    "src/fixed.rs": "fixed-point price/quantity parsing and checked arithmetic",
    "src/inspect.rs": "offline SBE capture inspection command",
    "src/journal.rs": "raw-frame journal writer, segment/manifest format, verification, ordered reading",
    "src/latency.rs": "latency experiment orchestration and result output",
    "src/lib.rs": "library module exports and public entry points",
    "src/main.rs": "process entry point and collect/discover/replay/inspect/verify/latency dispatch",
    "src/metrics.rs": "metrics registry, Prometheus text encoding, HTTP health/metrics endpoint",
    "src/model.rs": "normalized market, source, event, control, and output domain types",
    "src/orderbook.rs": "normalized order-book representation and mutation invariants",
    "src/replay.rs": "deterministic JSONL replay into the collector/output path",
    "src/report.rs": "market metadata and run-report sidecars; atomic JSON helper",
    "src/session.rs": "generic websocket session/factory contract and supervised worker lifecycle",
    "src/side.rs": "normalized buy/sell side semantics",
    "src/time.rs": "wall-clock and monotonic timing helpers",
    "src/verify.rs": "SBE delta-to-snapshot self-consistency verification",
    "src/writer.rs": "bounded writer queue, dedicated writer thread, SBE sink lifecycle",
    "src/ws.rs": "websocket connection options, source binding and connection experiments",
    "src/capacity.rs": "startup and fixed-topology discovery capacity admission",
    "src/catalog.rs": "versioned discovery checkpoint with integrity envelope, age, and counts",
    "src/persistence.rs": "latest-wins metadata/catalog background writer and lifecycle status",
    "src/collector/mod.rs": "collector core module boundary",
    "src/collector/state.rs": "serialized per-symbol/source state machine, discovery and failover decisions",
    "src/exchange/mod.rs": "exchange capabilities and adapter-independent session factory abstraction",
    "src/exchange/kalshi/auth.rs": "Kalshi request signing and auth header construction",
    "src/exchange/kalshi/codec.rs": "Kalshi wire-frame parsing and normalized events",
    "src/exchange/kalshi/discovery.rs": "Kalshi REST pagination, market eligibility and metadata conversion",
    "src/exchange/kalshi/mod.rs": "Kalshi adapter construction and Exchange implementation",
    "src/exchange/kalshi/session.rs": "Kalshi websocket protocol, subscriptions, acknowledgements and sequence recovery",
    "src/exchange/polymarket/codec.rs": "Polymarket CLOB payload validation and normalized events",
    "src/exchange/polymarket/discovery.rs": "Gamma keyset pagination, version-aware identifiers and outcome expansion",
    "src/exchange/polymarket/mod.rs": "Polymarket adapter construction and Exchange implementation",
    "src/exchange/polymarket/session.rs": "public CLOB websocket protocol and subscription/resubscription behavior",
    "src/util/aligned_sbe_reader.rs": "aligned SBE capture reader",
    "src/util/aligned_sbe_writer.rs": "aligned SBE chunk writer and sink helpers",
    "src/util/build.rs": "build-time SBE DTO/codec generation integration",
    "src/util/dtos.rs": "generated domain DTO bridge for supplied SBE schema",
    "src/util/mod.rs": "SBE utility module exports",
    "src/util/orderbook_schema.rs": "schema-facing book message definitions/adaptation",
    "src/util/sbe_yaml.rs": "supplied SBE schema loading/validation support",
}

REQUIRED_TESTS = {
    "snapshot_anchors_delta_application_in_per_symbol_order",
    "cross_symbol_output_order_is_processing_order_and_not_globally_sorted",
    "clocked_shadow_replica_takes_over_after_source_loss",
    "clocked_failover_anchors_resumed_deltas_and_preserves_receive_times",
    "clockless_full_recovery_requires_acknowledged_fence_and_fresh_snapshot",
    "recovery_snapshot_before_ack_is_rejected_without_mutation",
    "single_socket_loss_fails_over_and_emits_anchoring_snapshot",
    "discovery_adds_and_removes_with_controls",
    "discovery_churn_retires_and_adds_symbols",
    "normal_shutdown_drains_and_joins",
    "shutdown_drains_saturated_queue_and_joins",
    "finish_failure_on_shutdown_is_reported",
    "negative_control_missing_update_is_detected_by_verify",
    "payload_bytes_survive_write_and_read",
    "checksum_mismatch_and_corrupt_manifest_fail_verification",
    "queue_saturation_then_shutdown_stays_failed",
    "concurrent_producers_stop_after_failure",
    "successful_shutdown_reconciles_counters",
    "blocked_writer_bounds_shutdown",
    "blocked_writer_bounds_abort",
    "graceful_shutdown_finalizes_rotated_segments",
    "journal_accounts_for_every_delivered_payload",
    "rejected_payloads_are_journaled_across_reconnects",
    "journal_finalizes_and_verifies_on_graceful_shutdown",
}

# Anchors that make the main structural claims falsifiable against code.
REQUIRED_ANCHORS = {
    "src/main.rs": [
        "Command::Collect", "Command::Replay", "Command::Inspect",
        "Command::Verify", "Command::Latency", "Command::Discover",
        "Command::Journal",
    ],
    "src/coordinator.rs": [
        "run_with_shutdown", "discover_with_retry", "process_worker_event",
        "dispatch_controls", "tokio::select!", "core.finalize",
        "writer.shutdown_with_stats",
    ],
    "src/capacity.rs": [
        "derive_shard_count", "pub fn plan", "CapacityPlan",
    ],
    "src/collector/state.rs": [
        "apply_discovery", "handle_event", "handle_snapshot",
        "handle_level_change", "handle_lifecycle", "handle_stream_reset",
        "handle_status", "pub fn tick", "pub fn finalize", "fn reseat",
    ],
    "src/session.rs": [
        "pub trait MarketDataSession", "pub trait SessionFactory",
        "pub async fn run_worker", "backoff_or_stop", "handle_control",
    ],
    "src/writer.rs": [
        "pub struct WriterHandle", "pub async fn send", "shutdown_inner",
        "blocking_recv", "sync_on_close",
    ],
    "src/exchange/kalshi/auth.rs": ["pub struct KalshiAuth", "pub fn headers"],
    "src/exchange/kalshi/discovery.rs": ["fetch_markets", "fetch_page"],
    "src/exchange/kalshi/session.rs": ["struct SequenceTracker", "on_snapshot", "on_delta"],
    "src/exchange/polymarket/discovery.rs": ["fetch_markets", "fetch_page", "identifier_kind"],
    "src/exchange/polymarket/codec.rs": ["price_change", "unknown_event_types_are_counted_but_not_fatal"],
    "src/exchange/polymarket/session.rs": ["send_initial", "next_frame", "send_keepalive"],
    "src/verify.rs": ["pub fn run"],
    "src/journal.rs": [
        "pub struct JournalHandle", "pub fn verify_run", "pub fn read_records",
        "struct Manifest", "struct SegmentFooter", "RunState::Finalized",
    ],
    "src/util/build.rs": ["generate_str_to_out_dir", "GenerationConfig"],
}

TEST_RE = re.compile(
    r"#\[(?P<attr>(?:tokio::test|test)(?:[^\]]*)?)\]\s*"
    r"(?:(?:#\[[^\]]*\])\s*)*"
    r"(?:async\s+)?fn\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)",
    re.MULTILINE | re.DOTALL,
)
MERMAID_RE = re.compile(r"```mermaid\s*\n(.*?)\n```", re.DOTALL)
NODE_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:\[[^\n]*\]|\([^\n]*\)|\{[^\n]*\})\s*$")
EDGE_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+-->\s+([A-Za-z_][A-Za-z0-9_]*)\s*$")


MODULE_DECL_RE = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*;\s*(?://.*)?$",
    re.MULTILINE,
)


def collect_module_reachability(root: Path = ROOT) -> set[str]:
    """Return source files reachable through `mod foo;` from lib/bin crate roots.

    This is a conservative source-tree check, not a Rust parser. Inline modules
    are not treated as separate files.
    """
    # `src/util/build.rs` is Cargo's configured build-script crate root (`build = ...` in Cargo.toml).
    roots = [root / "src/lib.rs", root / "src/main.rs", root / "src/util/build.rs"]
    reachable: set[str] = set()
    pending = [path for path in roots if path.exists()]
    while pending:
        current = pending.pop()
        rel = current.relative_to(root).as_posix()
        if rel in reachable:
            continue
        reachable.add(rel)
        content = current.read_text(encoding="utf-8")
        if current.name in {"lib.rs", "main.rs", "build.rs", "mod.rs"}:
            module_dir = current.parent
        else:
            module_dir = current.parent / current.stem
        for match in MODULE_DECL_RE.finditer(content):
            name = match.group(1)
            candidates = [module_dir / f"{name}.rs", module_dir / name / "mod.rs"]
            found = [candidate for candidate in candidates if candidate.exists()]
            if len(found) == 1:
                pending.append(found[0])
    return reachable


def module_integration_errors(root: Path = ROOT) -> list[str]:
    """Check falsifiable runtime call-site integration, not stale verdicts."""
    errors: list[str] = []

    # Check active source files reference missing dependencies
    config = (root / "src/config.rs").read_text(encoding="utf-8")
    capacity_path = root / "src/capacity.rs"
    if capacity_path.exists():
        capacity = capacity_path.read_text(encoding="utf-8")
        collector_struct = re.search(r"pub struct CollectorSettings\s*\{(.*?)\n\}", config, re.DOTALL)
        declared = set(re.findall(r"pub\s+(\w+)\s*:", collector_struct.group(1))) if collector_struct else set()
        referenced = set(re.findall(r"settings\.collector\.(\w+)", capacity))
        for field in sorted(referenced - declared):
            errors.append(f"src/capacity.rs references CollectorSettings.{field}, absent from src/config.rs")
    catalog_path = root / "src/catalog.rs"
    if catalog_path.exists() and "util::sha256_hex" in catalog_path.read_text(encoding="utf-8"):
        util_text = "\n".join(path.read_text(encoding="utf-8") for path in (root / "src/util").rglob("*.rs"))
        if not re.search(r"\bfn\s+sha256_hex\b", util_text):
            errors.append("src/catalog.rs references util::sha256_hex, but no such function exists under src/util/")

    coordinator = (root / "src/coordinator.rs").read_text(encoding="utf-8")
    required = {
        "capacity startup": "capacity::plan(&settings, &initial_markets)",
        "capacity refresh admission": "capacity::plan_fixed_topology(&settings, &markets, &loads)",
        "catalog loading": "Catalog::load",
        "persistence startup": "PersistenceHandle::start(",
        "persistence submit": "persistence.submit(",
        "persistence final drain": "persistence.submit_final(",
        "persistence shutdown": "persistence.shutdown(send_timeout)",
    }
    for name, anchor in required.items():
        if anchor not in coordinator:
            errors.append(f"runtime integration missing: {name} anchor `{anchor}`")
    return errors


def collect_sources() -> list[str]:
    return sorted(p.relative_to(ROOT).as_posix() for p in (ROOT / "src").rglob("*.rs"))


def collect_tests() -> list[tuple[str, str, str]]:
    records: list[tuple[str, str, str]] = []
    for base in (ROOT / "src", ROOT / "tests"):
        for path in sorted(base.rglob("*.rs")):
            rel = path.relative_to(ROOT).as_posix()
            content = path.read_text(encoding="utf-8")
            for match in TEST_RE.finditer(content):
                attr = match.group("attr")
                name = match.group("name")
                line = content.count("\n", 0, match.start()) + 1
                records.append((rel, name, f"{attr} (line {line})"))
    return sorted(records, key=lambda x: (x[0], x[1]))


def render_source_inventory() -> str:
    sources = collect_sources()
    reachable = collect_module_reachability()
    lines = [
        "<!-- Generated by scripts/check_architecture.py --update; do not hand-edit. -->",
        "# Rust source inventory", "",
        f"Rust source files found: **{len(sources)}**; reachable from the library, binary, or Cargo build-script root: **{len(set(sources) & reachable)}**.", "",
        "Reachability means a file is referenced through Rust module declarations; separate call-site assertions check critical runtime integration.", "",
        "| Source file | Responsibility | Crate-root reachability |", "|---|---|---|",
    ]
    for path in sources:
        description = SOURCE_RESPONSIBILITIES.get(path)
        if not description:
            description = "[REVIEW REQUIRED: add a reviewed responsibility in scripts/check_architecture.py]"
        if path in reachable:
            reach = "reachable from library, binary, or build-script root"
        else:
            reach = "[REVIEW REQUIRED: classify this unrooted file]"
        lines.append(f"| `{path}` | {description} | {reach} |")
    lines.append("")
    return "\n".join(lines)


def render_test_inventory() -> str:
    tests = collect_tests()
    reachable = collect_module_reachability()
    active_count = sum(1 for path, _, _ in tests if path.startswith("tests/") or path in reachable)
    inactive_count = len(tests) - active_count
    lines = [
        "<!-- Generated by scripts/check_architecture.py --update; do not hand-edit. -->",
        "# Statically declared Rust test inventory", "",
        f"Test functions discovered from `#[test]` and `#[tokio::test]` attributes: **{len(tests)}**.",
        f"Declarations in a module file reachable from a Cargo crate root or under `tests/`: **{active_count}**; declarations in integration-blocked source files: **{inactive_count}**.",
        "This is source inventory, not a claim that tests passed or that a build succeeded. Test functions inside integration-blocked source files are compiled but their advertised capabilities are not available at runtime.",
        "", "| File | Test function | Attribute / line | Cargo target reachability |", "|---|---|---|---|",
    ]
    for path, name, attr in tests:
        if path.startswith("tests/"):
            reach = "integration-test target"
        elif path in reachable:
            reach = "crate-root reachable"
        else:
            reach = "[REVIEW REQUIRED: classify this unrooted file]"
        lines.append(f"| `{path}` | `{name}` | `{attr}` | {reach} |")
    lines.append("")
    return "\n".join(lines)


def check_mermaid_structure(document: str) -> list[str]:
    """Check graph structure conservatively; GitHub Actions runs the full Mermaid parser."""
    errors: list[str] = []
    blocks = MERMAID_RE.findall(document)
    if len(blocks) < 7:
        errors.append(f"expected at least 7 Mermaid diagrams, found {len(blocks)}")
    # Mermaid edge variants used here: --> and -->|edge label|. Keep each edge on one line.
    arrow_re = re.compile(r"-->(?:\|[^|]+\|)?")
    side_re = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)(.*)$")
    shaped_re = re.compile(r"^\s*(?:\[[^\n]*\]|\([^\n]*\)|\{[^\n]*\})")
    for index, body in enumerate(blocks, start=1):
        lines = body.splitlines()
        if not lines or not re.match(r"^\s*flowchart\s+(?:LR|RL|TB|TD|BT)\s*$", lines[0]):
            errors.append(f"diagram {index}: first line must be a supported flowchart direction")
            continue
        declared: set[str] = set()
        edges: list[tuple[str, str, int]] = []
        for line_number, line in enumerate(lines[1:], start=2):
            stripped = line.strip()
            if not stripped or stripped.startswith("%%") or stripped.startswith("subgraph ") or stripped == "end":
                continue
            if arrow_re.search(line):
                parts = arrow_re.split(line, maxsplit=1)
                if len(parts) != 2:
                    errors.append(f"diagram {index}, line {line_number}: malformed edge: {stripped}")
                    continue
                lhs, rhs = parts
                left = side_re.match(lhs)
                right = side_re.match(rhs)
                if not left or not right:
                    errors.append(f"diagram {index}, line {line_number}: cannot parse edge endpoints: {stripped}")
                    continue
                source_id, source_tail = left.group(1), left.group(2)
                target_id, target_tail = right.group(1), right.group(2)
                if shaped_re.match(source_tail):
                    declared.add(source_id)
                if shaped_re.match(target_tail):
                    declared.add(target_id)
                # A bare endpoint must have been declared on another line.
                edges.append((source_id, target_id, line_number))
                continue
            node = NODE_RE.match(line)
            if node:
                declared.add(node.group(1))
                continue
            errors.append(f"diagram {index}, line {line_number}: unsupported/unparsed Mermaid statement: {stripped}")
        for source, target, line_number in edges:
            for node_id in (source, target):
                if node_id not in declared:
                    errors.append(f"diagram {index}, line {line_number}: edge references undeclared node {node_id}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check", action="store_true", help="check generated inventories and architecture traceability")
    group.add_argument("--update", action="store_true", help="regenerate source/test inventories after review")
    args = parser.parse_args()

    errors: list[str] = []
    errors.extend(module_integration_errors())
    actual_sources = set(collect_sources())
    documented_sources = set(SOURCE_RESPONSIBILITIES)
    missing_docs = sorted(actual_sources - documented_sources)
    stale_docs = sorted(documented_sources - actual_sources)
    if missing_docs:
        errors.append("source files need reviewed responsibility mappings: " + ", ".join(missing_docs))
    if stale_docs:
        errors.append("source responsibility mappings point to removed files: " + ", ".join(stale_docs))

    test_records = collect_tests()
    actual_test_names = {name for _, name, _ in test_records}
    missing_tests = sorted(REQUIRED_TESTS - actual_test_names)
    if missing_tests:
        errors.append("required evidence test(s) not found: " + ", ".join(missing_tests))

    if not DOC.exists():
        errors.append(f"missing architecture document: {DOC.relative_to(ROOT)}")
        document = ""
    else:
        document = DOC.read_text(encoding="utf-8")
    for anchor_path, anchors in REQUIRED_ANCHORS.items():
        file_path = ROOT / anchor_path
        if not file_path.exists():
            errors.append(f"required source anchor file is missing: {anchor_path}")
            continue
        source = file_path.read_text(encoding="utf-8")
        for anchor in anchors:
            if anchor not in source:
                errors.append(f"source anchor `{anchor}` not found in `{anchor_path}`")
    for test_name in REQUIRED_TESTS:
        if f"`{test_name}`" not in document:
            errors.append(f"required evidence test `{test_name}` is not cited in docs/architecture.md")
    for required_text in [
        "docs/architecture/source-inventory.md",
        "docs/architecture/test-inventory.md",
        "docs/requirement-test-matrix.md",
        "Current operational caveat",
        "not a production certification",
    ]:
        if required_text not in document:
            errors.append(f"architecture document is missing required traceability text: {required_text!r}")
    errors.extend(check_mermaid_structure(document))

    expected_files = {
        SOURCE_INVENTORY: render_source_inventory(),
        TEST_INVENTORY: render_test_inventory(),
    }
    if args.update:
        for path, expected in expected_files.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(expected, encoding="utf-8")
        if errors:
            print(f"Updated inventories for {len(collect_sources())} source files and {len(collect_tests())} test declarations; integration checks:", file=sys.stderr)
            for error in errors:
                print(f"INTEGRATION CHECK: {error}", file=sys.stderr)
            return 1
        reachable = collect_module_reachability()
        print(f"Updated inventories: {len(collect_sources())} Rust source files; {len(collect_tests())} attributed test functions; {len(reachable)} files reachable from crate roots.")
        return 0

    for path, expected in expected_files.items():
        if not path.exists():
            errors.append(f"missing generated inventory: {path.relative_to(ROOT)} (run --update after review)")
        elif path.read_text(encoding="utf-8") != expected:
            errors.append(f"stale inventory: {path.relative_to(ROOT)} (review changes, then run --update)")

    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    reachable = collect_module_reachability()
    print(f"Architecture sync check passed: {len(collect_sources())} Rust source files, {len(collect_tests())} attributed test functions, {len(reachable)} files reachable from crate roots, and {len(MERMAID_RE.findall(document))} structurally checked Mermaid diagrams.")
    print("Note: actual Mermaid grammar is parsed in the separate GitHub Actions Mermaid-validation job.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
