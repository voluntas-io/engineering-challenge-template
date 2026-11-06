#!/usr/bin/env python3
"""Verify that every test reference in the requirement matrix resolves.

The requirement-to-test matrix is an evidence contract. A reference to a test
that no longer exists silently weakens that contract, so this gate resolves the
names against the inventory reported by `cargo test --all-targets -- --list`
and fails when a reference is missing or ambiguous.

Only table columns whose heading contains "evidence" are scanned. Inline code
elsewhere (implementation paths, status text) is not treated as a test
reference. Supported reference forms:

  module::tests::test_name
  module::tests::{first,second}
  module::tests::*
  tests/file.rs::test_name
  tests/file.rs::*
  test_name            (bare name, for E2E tests and short references)
  E2E `assert_helper`  (assertion helper defined in an integration-test file)

Wildcards must match at least one test and are reported separately so exact
references are preferred. Bare names must resolve to exactly one test. An
`assert_*` name marked E2E is an assertion helper, not a `#[test]`, so it is
resolved against helper definitions in the integration-test sources.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MATRIX = ROOT / "docs/requirement-test-matrix.md"

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SNAKE_CASE = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)+$")
HELPER = re.compile(r"^assert_[a-z0-9_]+$")
HELPER_DEFINITION = re.compile(r"^\s*fn\s+(assert_[a-z0-9_]+)\s*\(", re.MULTILINE)
BACKTICK = re.compile(r"`([^`]+)`")
FILE_REF = re.compile(r"^(tests/[A-Za-z0-9_./-]+\.rs)::(.+)$")
GROUP = re.compile(r"^(.*)::\{(.*)\}$")
WILDCARD = re.compile(r"^(.*)::\*$")


@dataclass(frozen=True)
class Reference:
    """One test reference extracted from an evidence cell."""

    text: str
    line: int
    source: str
    e2e: bool = False


@dataclass
class Inventory:
    """Test names reported by the test harness, plus E2E assertion helpers."""

    lib: set[str] = field(default_factory=set)
    files: dict[str, set[str]] = field(default_factory=dict)
    helpers: dict[str, set[str]] = field(default_factory=dict)

    def all_names(self) -> set[str]:
        names = set(self.lib)
        for file_names in self.files.values():
            names.update(file_names)
        return names

    def to_json(self) -> dict[str, object]:
        return {
            "lib": sorted(self.lib),
            "files": {name: sorted(names) for name, names in sorted(self.files.items())},
            "helpers": {
                name: sorted(names) for name, names in sorted(self.helpers.items())
            },
        }

    @classmethod
    def from_json(cls, payload: dict[str, object]) -> "Inventory":
        lib = {str(name) for name in payload.get("lib", [])}
        raw_files = payload.get("files", {})
        files = {
            str(name): {str(test) for test in tests}
            for name, tests in dict(raw_files).items()
        }
        raw_helpers = payload.get("helpers", {})
        helpers = {
            str(name): {str(test) for test in tests}
            for name, tests in dict(raw_helpers).items()
        }
        return cls(lib=lib, files=files, helpers=helpers)


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


def expand_reference(text: str) -> list[str]:
    """Expand a grouped reference into its named members."""
    match = GROUP.match(text)
    if not match:
        return [text]
    prefix, members = match.groups()
    expanded: list[str] = []
    for member in members.split(","):
        member = member.strip()
        if not member:
            continue
        expanded.append(f"{prefix}::{member}" if prefix else member)
    return expanded


def extract_references(markdown: str) -> list[Reference]:
    """Extract test references from evidence columns only."""
    references: list[Reference] = []
    header: list[str] | None = None
    evidence: list[int] = []
    in_table = False

    for number, line in enumerate(markdown.splitlines(), start=1):
        if not line.lstrip().startswith("|"):
            header = None
            evidence = []
            in_table = False
            continue
        cells = split_row(line)
        if not in_table:
            header = cells
            evidence = evidence_columns(header)
            in_table = True
            continue
        if all(set(cell) <= {"-", ":", " "} for cell in cells if cell):
            # Separator row: the next row is data.
            continue
        for index in evidence:
            if index >= len(cells):
                continue
            cell = cells[index]
            for match in BACKTICK.finditer(cell):
                span = match.group(1).strip()
                if not is_reference(span, cell, match.start()):
                    continue
                e2e = cell[: match.start()].rstrip().endswith("E2E")
                for expanded in expand_reference(span):
                    references.append(Reference(expanded, number, span, e2e))
    return references


def is_reference(span: str, cell: str, start: int) -> bool:
    """Decide whether a backticked span is intended as a test reference."""
    if not span:
        return False
    if "::" in span or span.startswith("tests/"):
        return True
    before = cell[:start].rstrip()
    if before.endswith("E2E") and IDENTIFIER.match(span):
        return True
    # A bare snake_case name in an evidence column is a test reference. This
    # catches short forms such as `sequence_reset_fails_over_to_valid_shadow`
    # that were previously allowed to drift. Other inline code (config keys,
    # field names, function names without underscores) is not a reference.
    return bool(SNAKE_CASE.match(span))


def exact_matches(
    name: str, inventory: Inventory, allow_short: bool = False
) -> list[tuple[str, str]]:
    """Return (location, resolved name) matches for an exact reference."""
    matches: list[tuple[str, str]] = []
    if name in inventory.lib:
        matches.append(("lib", name))
    if allow_short:
        for qualified in inventory.lib:
            if qualified.rsplit("::", 1)[-1] == name:
                matches.append(("lib", qualified))
    for file_name, tests in inventory.files.items():
        if name in tests:
            matches.append((file_name, name))
    return matches


def helper_matches(name: str, inventory: Inventory) -> list[tuple[str, str]]:
    """Return (file, helper name) matches for an E2E assertion helper."""
    return [
        (file_name, name)
        for file_name, names in inventory.helpers.items()
        if name in names
    ]


def prefix_matches(prefix: str, inventory: Inventory) -> list[tuple[str, str]]:
    matches: list[tuple[str, str]] = []
    for name in inventory.lib:
        if name.startswith(prefix):
            matches.append(("lib", name))
    for file_name, tests in inventory.files.items():
        for name in tests:
            if name.startswith(prefix):
                matches.append((file_name, name))
    return matches


def resolve(reference: Reference, inventory: Inventory) -> tuple[str, str]:
    """Resolve one reference; return (kind, resolved name) or raise ValueError."""
    text = reference.text

    if reference.e2e and HELPER.match(text):
        matches = helper_matches(text, inventory)
        if not matches:
            raise ValueError(f"no E2E assertion helper named `{text}`")
        if len(matches) > 1:
            locations = ", ".join(sorted({file for file, _ in matches}))
            raise ValueError(
                f"E2E helper `{text}` is ambiguous across {locations}"
            )
        return "helper", matches[0][1]

    file_match = FILE_REF.match(text)
    if file_match:
        file_name, test = file_match.groups()
        if file_name not in inventory.files:
            raise ValueError(f"{file_name} is not a known integration-test target")
        if test == "*":
            prefix = ""
            matches = [
                ("file", name)
                for name in inventory.files[file_name]
                if name.startswith(prefix)
            ]
        else:
            matches = [
                ("file", name)
                for name in inventory.files[file_name]
                if name == test
            ]
        if not matches:
            raise ValueError(f"no test in {file_name} matches `{test}`")
        return "file", matches[0][1]

    wildcard = WILDCARD.match(text)
    if wildcard:
        prefix = wildcard.group(1)
        matches = prefix_matches(prefix, inventory)
        if not matches:
            raise ValueError(f"wildcard `{text}` matches no test")
        return "wildcard", f"{prefix}::* ({len(matches)} tests)"

    if "::" in text:
        matches = exact_matches(text, inventory)
        if not matches:
            raise ValueError(f"no test named `{text}`")
        return "qualified", text

    matches = exact_matches(text, inventory, allow_short=True)
    if len(matches) == 1:
        return "bare", matches[0][1]
    if not matches:
        raise ValueError(f"no test named `{text}`")
    locations = ", ".join(sorted({location for location, _ in matches}))
    raise ValueError(f"bare name `{text}` is ambiguous across {locations}")


def validate(
    references: list[Reference], inventory: Inventory, matrix_name: str
) -> tuple[list[str], dict[str, int]]:
    """Resolve every reference, returning errors and a kind summary."""
    errors: list[str] = []
    kinds = {"qualified": 0, "bare": 0, "file": 0, "wildcard": 0, "helper": 0}
    for reference in references:
        try:
            kind, _ = resolve(reference, inventory)
        except ValueError as error:
            errors.append(f"{matrix_name}:{reference.line}: {reference.text}: {error}")
            continue
        kinds[kind] += 1
    return errors, kinds


def run_list(root: Path, args: list[str]) -> set[str]:
    result = subprocess.run(
        ["cargo", "test", *args, "--locked", "--", "--list"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"`cargo test {' '.join(args)} -- --list` failed:\n{result.stderr.strip()}"
        )
    names = set()
    for line in result.stdout.splitlines():
        if line.endswith(": test"):
            names.add(line[: -len(": test")].strip())
    return names


def load_inventory_from_cargo(root: Path) -> Inventory:
    inventory = Inventory()
    inventory.lib = run_list(root, ["--lib"])
    tests_dir = root / "tests"
    if tests_dir.is_dir():
        for path in sorted(tests_dir.glob("*.rs")):
            file_name = f"tests/{path.name}"
            inventory.files[file_name] = run_list(root, ["--test", path.stem])
            helpers = set(
                HELPER_DEFINITION.findall(path.read_text(encoding="utf-8"))
            )
            if helpers:
                inventory.helpers[file_name] = helpers
    return inventory


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path, default=MATRIX)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument(
        "--inventory",
        type=Path,
        help="read the test inventory from JSON instead of invoking cargo",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    matrix = args.matrix
    try:
        markdown = matrix.read_text(encoding="utf-8")
    except OSError as error:
        print(f"[FAIL] cannot read {matrix}: {error}", file=sys.stderr)
        return 1

    if args.inventory:
        inventory = Inventory.from_json(
            json.loads(args.inventory.read_text(encoding="utf-8"))
        )
    else:
        try:
            inventory = load_inventory_from_cargo(args.root)
        except (OSError, RuntimeError) as error:
            print(f"[FAIL] could not build test inventory: {error}", file=sys.stderr)
            return 1

    references = extract_references(markdown)
    errors, kinds = validate(references, inventory, matrix.name)

    if args.verbose:
        for reference in references:
            try:
                kind, resolved = resolve(reference, inventory)
            except ValueError as error:
                print(f"[FAIL] {matrix.name}:{reference.line}: {error}")
            else:
                print(f"[ok] line {reference.line}: {reference.text} -> {resolved} ({kind})")

    if errors:
        for error in errors:
            print(f"[FAIL] {error}", file=sys.stderr)
        print(
            f"[FAIL] {len(errors)} unresolved test reference(s) in {matrix.name}",
            file=sys.stderr,
        )
        return 1

    print(
        "[PASS] All matrix test references resolve: "
        f"{len(references)} refs "
        f"({kinds['qualified']} qualified, {kinds['bare']} bare, "
        f"{kinds['file']} file-qualified, {kinds['wildcard']} wildcard, "
        f"{kinds['helper']} E2E helper)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
