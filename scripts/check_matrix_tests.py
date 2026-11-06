#!/usr/bin/env python3
"""Verify the requirement-to-test matrix against declared Rust tests.

Every evidence reference must be an exact, file-qualified test name:

    src/path/to/file.rs::test_name
    tests/path/to/file.rs::test_name

The checker parses `#[test]` and `#[tokio::test]` declarations from the source
tree. It therefore verifies both that the source file exists and that the named
test is declared in that exact file. Wildcards, grouped references, bare names,
and module-qualified Rust paths are rejected as ambiguous.

The matrix must contain M1-M9 and E1-E5, every requirement must have at least
one test reference, and the 24 mandatory evidence tests must remain
represented. This is the single test-evidence gate; the architecture checker no
longer keeps a duplicate list.
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MATRIX = ROOT / "docs/requirement-test-matrix.md"

REQUIRED_REQUIREMENTS = tuple(f"M{index}" for index in range(1, 10)) + tuple(
    f"E{index}" for index in range(1, 6)
)

# Moved here from scripts/check_architecture.py. These names must remain in the
# matrix even as individual rows are edited.
REQUIRED_EVIDENCE_TESTS = frozenset(
    {
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
)

ID_RE = re.compile(r"^([ME])(\d+)$")
REFERENCE_RE = re.compile(
    r"^(?P<file>(?:src|tests)/[A-Za-z0-9_./-]+\.rs)::"
    r"(?P<test>[A-Za-z_][A-Za-z0-9_]*)$"
)
BARE_RE = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)+$")
BACKTICK_RE = re.compile(r"`([^`]+)`")

# Discover `#[test]`, `#[tokio::test]`, and `#[tokio::test(...)]` declarations.
# Other attributes between the test attribute and `fn` are allowed.
TEST_DECL_RE = re.compile(
    r"#\[\s*(?:tokio::)?test(?:\s*\([^\]]*\))?\s*\]"
    r"(?:\s*(?:#\[[^\]]*\]|//[^\n]*))*"
    r"\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+"
    r"([A-Za-z_][A-Za-z0-9_]*)",
    re.DOTALL,
)


@dataclass(frozen=True)
class Span:
    """One backticked span taken from an evidence column."""

    text: str
    line: int


@dataclass
class Row:
    """One requirement row and the evidence spans attached to it."""

    requirement: str
    line: int
    spans: list[Span] = field(default_factory=list)


@dataclass(frozen=True)
class Reference:
    """A file-qualified test reference."""

    file: str
    test: str
    line: int


def split_row(line: str) -> list[str]:
    """Split a Markdown table row on unescaped pipes."""
    text = line.strip()
    if text.startswith("|"):
        text = text[1:]
    if text.endswith("|") and not text.endswith("\\|"):
        text = text[:-1]
    cells: list[str] = []
    current: list[str] = []
    escaped = False
    for char in text:
        if escaped:
            current.append(char)
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == "|":
            cells.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    cells.append("".join(current).strip())
    return cells


def evidence_columns(header: list[str]) -> list[int]:
    """Column indexes whose heading declares evidence content."""
    return [index for index, name in enumerate(header) if "evidence" in name.lower()]


def scan_matrix(markdown: str) -> list[Row]:
    """Return requirement rows with the backticked spans from evidence columns."""
    rows: list[Row] = []
    header: list[str] | None = None
    evidence: list[int] = []
    in_table = False
    current: Row | None = None

    for number, line in enumerate(markdown.splitlines(), start=1):
        if not line.lstrip().startswith("|"):
            header = None
            evidence = []
            in_table = False
            current = None
            continue
        cells = split_row(line)
        if not in_table:
            header = cells
            evidence = evidence_columns(header)
            in_table = True
            current = None
            continue
        if all(set(cell) <= {"-", ":", " "} for cell in cells if cell):
            continue

        first = cells[0] if cells else ""
        current = Row(first, number) if ID_RE.fullmatch(first) else None
        if current is not None:
            rows.append(current)

        for index in evidence:
            if index >= len(cells):
                continue
            for match in BACKTICK_RE.finditer(cells[index]):
                span = Span(match.group(1).strip(), number)
                if current is not None:
                    current.spans.append(span)
    return rows


def discover_tests(root: Path = ROOT) -> dict[str, set[str]]:
    """Map every Rust source/integration file to its declared test names.

    All `.rs` files under `src/` and `tests/` are included, even when they
    declare no tests, so a reference can be distinguished from a missing file.
    """
    discovered: dict[str, set[str]] = {}
    for base_name in ("src", "tests"):
        base = root / base_name
        if not base.is_dir():
            continue
        for path in sorted(base.rglob("*.rs")):
            relative = path.relative_to(root).as_posix()
            content = path.read_text(encoding="utf-8")
            discovered[relative] = {
                match.group(1) for match in TEST_DECL_RE.finditer(content)
            }
    return discovered


def classify(span: Span, known_names: set[str]) -> tuple[Reference | None, str | None]:
    """Resolve one span, returning either a reference or a rejection reason."""
    text = span.text
    if not text:
        return None, None
    if "*" in text and ("::" in text or "/" in text or text == "*"):
        return None, "wildcards are not allowed; name each test exactly"
    if "{" in text or "}" in text:
        return None, "grouped references are not allowed; list each test separately"
    if "::" in text:
        match = REFERENCE_RE.fullmatch(text)
        if not match:
            return (
                None,
                "reference must be file-qualified as "
                "src/...rs::test_name or tests/...rs::test_name",
            )
        return Reference(match.group("file"), match.group("test"), span.line), None
    if BARE_RE.fullmatch(text) or text in known_names:
        return None, "bare test name is ambiguous; qualify it with its source file"
    return None, None


def validate(
    markdown: str,
    tests_by_file: dict[str, set[str]],
    required_requirements: tuple[str, ...] = REQUIRED_REQUIREMENTS,
    required_tests: frozenset[str] = REQUIRED_EVIDENCE_TESTS,
) -> tuple[list[str], list[Reference]]:
    """Return validation errors and every resolved reference."""
    rows = scan_matrix(markdown)
    known_names = set().union(*tests_by_file.values()) if tests_by_file else set()
    errors: list[str] = []
    references: list[Reference] = []
    seen: dict[str, int] = {}
    row_references: dict[str, list[Reference]] = {}

    for row in rows:
        if row.requirement in seen:
            errors.append(
                f"line {row.line}: duplicate requirement ID {row.requirement} "
                f"(first at line {seen[row.requirement]})"
            )
        else:
            seen[row.requirement] = row.line
        if row.requirement not in required_requirements:
            errors.append(f"line {row.line}: unexpected requirement ID {row.requirement}")

        resolved: list[Reference] = []
        for span in row.spans:
            reference, error = classify(span, known_names)
            if error:
                errors.append(f"line {span.line}: {span.text}: {error}")
            elif reference is not None:
                resolved.append(reference)
        row_references[row.requirement] = resolved
        references.extend(resolved)

    for requirement in required_requirements:
        if requirement not in seen:
            errors.append(f"missing required requirement {requirement}")
        elif not row_references.get(requirement):
            errors.append(f"requirement {requirement} has no test reference")

    for reference in references:
        if reference.file not in tests_by_file:
            errors.append(f"line {reference.line}: {reference.file} does not exist")
        elif reference.test not in tests_by_file[reference.file]:
            errors.append(
                f"line {reference.line}: {reference.test} is not declared in "
                f"{reference.file}"
            )

    present = {reference.test for reference in references}
    for name in sorted(required_tests):
        if name not in present:
            errors.append(f"mandatory evidence test is missing from the matrix: {name}")

    return errors, references


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path, default=MATRIX)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    try:
        markdown = args.matrix.read_text(encoding="utf-8")
    except OSError as error:
        print(f"[FAIL] cannot read {args.matrix}: {error}", file=sys.stderr)
        return 1

    tests_by_file = discover_tests(args.root)
    errors, references = validate(markdown, tests_by_file)

    if args.verbose:
        for reference in references:
            print(f"[ok] line {reference.line}: {reference.file}::{reference.test}")

    if errors:
        for error in errors:
            print(f"[FAIL] {error}", file=sys.stderr)
        print(
            f"[FAIL] {len(errors)} requirement-matrix error(s) in {args.matrix.name}",
            file=sys.stderr,
        )
        return 1

    rows = scan_matrix(markdown)
    print(
        f"[PASS] {len(references)} file-qualified references across "
        f"{len(rows)} requirements; all {len(REQUIRED_EVIDENCE_TESTS)} "
        "mandatory evidence tests are present."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
