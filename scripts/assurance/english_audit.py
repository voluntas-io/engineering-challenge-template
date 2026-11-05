#!/usr/bin/env python3
"""Read-only English and terminology candidate audit for Git-tracked text."""
from __future__ import annotations

import argparse
import fnmatch
import json
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class Pattern:
    category: str
    expression: str
    severity: str
    rationale: str


PATTERNS: tuple[Pattern, ...] = (
    Pattern(
        "tautology",
        r"\b(?:exact same|same exact|end result|final outcome|each and every|basic fundamentals|added bonus|free gift|pre[- ]plan(?:ned|ning)?|reason why|consensus of opinion|refer back|repeat again|revert back|merge together|combine together|currently existing|unexpected surprise|true fact|necessary prerequisite|past history|future plans|close proximity|mutual cooperation|first introduced)\b",  # english-audit-ignore: matcher vocabulary required to detect this category
        "error",
        "High-confidence redundant construction; inspect context before correction.",
    ),
    Pattern(
        "promotional_or_banned_candidate",
        r"\b(?:bulletproof|world[- ]class|best[- ]in[- ]class|revolutionary|game[- ]changing|cutting[- ]edge|enterprise[- ]grade|nasa[- ]level|state[- ]of[- ]the[- ]art|perfect(?:ly)?|forever|unparalleled|unmatched\s+(?:performance|quality|results?|speed|reliability|precision))\b",  # english-audit-ignore: matcher vocabulary required to detect this category
        "review",
        "Promotional or absolute wording; replace with measured evidence where appropriate.",
    ),
    Pattern(
        "filler",
        r"\b(?:obviously|clearly|simply|just|really|very|extremely|basically|actually|undoubtedly|certainly|of course|truly|merely|essentially)\b",  # english-audit-ignore: matcher vocabulary required to detect this category
        "review",
        "Potential filler or unsupported emphasis; some technical uses are valid.",  # english-audit-ignore: matcher rationale uses a review vocabulary term
    ),
    Pattern(
        "first_person",
        r"(?<![A-Za-z0-9_/])(?:I|We|Us|Our|Ours|My|Mine|Me|Myself|Ourselves|we|us|our|ours|my|mine|me|myself|ourselves)(?:['’](?:m|re|ve|ll|d))?(?![A-Za-z0-9_/])",  # english-audit-ignore: matcher vocabulary required to detect this category
        "review",
        "Review voice in normative and technical text; retain in quotations, founder narratives and historical evidence.",
    ),
    Pattern(
        "intent_adverb",
        r"\b(?:intentionally|deliberately|purposely|purposefully|knowingly|unintentionally|accidentally|inadvertently|willfully|voluntarily)\b",  # english-audit-ignore: matcher vocabulary required to detect this category
        "review",
        "Replace intent claims with observable conditions when author intent is not material.",
    ),
    Pattern(
        "vague_quantifier_or_scope",
        r"\b(?:some|many|few|several|various|numerous|often|usually|sometimes|soon|later|fast|slow|large|small|enough|reasonable|appropriate|significant|substantial)\b",  # english-audit-ignore: matcher vocabulary required to detect this category
        "review",
        "Replace with an exact threshold or explicit qualification when behavior depends on scope.",
    ),
    Pattern(
        "unsupported_absolute_or_guarantee",
        r"\b(?:always|never|guarantees?|ensures?|exactly once|lossless|production[- ]ready|zero downtime|never fails|always available|100%)\b",  # english-audit-ignore: matcher vocabulary required to detect this category
        "review",
        "Require a defined scope, failure model and supporting evidence; valid normative uses remain.",
    ),
    Pattern(
        "informal_or_ambiguous_verb",
        r"\b(?:do the needful|make sure|works fine|goes wrong|fix it|handle it|stuff|things|nice|good|bad|huge|tiny)\b",  # english-audit-ignore: matcher vocabulary required to detect this category
        "review",
        "Replace with a named operation, state, object or acceptance criterion.",
    ),
)

COMPILED = tuple((p, re.compile(p.expression, re.IGNORECASE)) for p in PATTERNS)


@dataclass
class Finding:
    source: str
    line: int
    category: str
    severity: str
    phrase: str
    rationale: str
    context: str


def git(repo: Path, *args: str) -> bytes:
    proc = subprocess.run(
        ["git", "-C", str(repo), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if proc.returncode != 0:
        detail = proc.stderr.decode("utf-8", "replace").strip()
        raise RuntimeError(f"git {' '.join(args)} failed ({proc.returncode}): {detail}")
    return proc.stdout


def tracked_paths(repo: Path) -> list[Path]:
    raw = git(repo, "ls-files", "-z", "--cached")
    return [Path(p.decode("utf-8", "surrogateescape")) for p in raw.split(b"\0") if p]


def _prose_lines(text: str, source: str) -> list[tuple[int, str]]:
    """Extract authored prose and comments, excluding code and markup attributes.

    Scanning identifiers such as Rust's `Some` variant, SVG geometry attributes,
    or the license text produces review noise without evidence about authored
    prose. Code, fixtures, protocol payloads and legal text remain available in
    the tree; this function selects only lines whose wording a project author
    controls.
    """
    if source.startswith("git-commit:"):
        return [(n, line) for n, line in enumerate(text.splitlines(), 1)]

    path = source.split(":", 1)[0]
    suffix = Path(path).suffix.lower()
    lines = text.splitlines()

    if suffix == ".svg":
        try:
            root = ET.fromstring(text)
        except ET.ParseError:
            return []
        visible = []
        for element in root.iter():
            if element.tag.rsplit("}", 1)[-1] in {"text", "title", "desc"}:
                visible.append(" ".join(element.itertext()))
        return [(1, value) for value in visible]

    if suffix == ".rs":
        # Scan line comments; Rust identifiers and units such as `us` are not prose.  # english-audit-ignore: literal identifier example
        return [(n, line.lstrip()[2:].strip()) for n, line in enumerate(lines, 1)
                if line.lstrip().startswith("//")]

    if suffix in {".py", ".yml", ".yaml", ".toml", ".sh", ".service"}:
        return [(n, line.lstrip()[1:].strip()) for n, line in enumerate(lines, 1)
                if line.lstrip().startswith("#")]

    if suffix == ".md":
        visible: list[tuple[int, str]] = []
        in_fence = False
        fence_marker = ""
        for n, line in enumerate(lines, 1):
            fence = re.match(r"^\s*(```+|~~~+)", line)
            if fence:
                marker = fence.group(1)[0]
                if not in_fence:
                    in_fence, fence_marker = True, marker
                elif marker == fence_marker:
                    in_fence = False
                continue
            if in_fence:
                continue
            # Link destinations and URLs are references, not authored prose.
            clean = re.sub(r"\]\([^)]*\)", "]", line)
            clean = re.sub(r"https?://\S+", "", clean)
            # Inline code is a literal value or identifier, not prose.
            clean = re.sub(r"`[^`]*`", "", clean)
            # Mermaid node identifiers such as `I --> J` are diagram symbols.  # english-audit-ignore: literal diagram identifier example
            clean = re.sub(r"(?<!\w)I(?=\s*(?:-->|---|==>|-.->|\[|\{))", "NODE", clean)
            clean = re.sub(r"(?<=-->)\s*I(?=\s*(?:-->|---|==>|-.->|\[|\{))", "NODE", clean)
            visible.append((n, clean))
        return visible

    return []


def scan_text(text: str, source: str) -> list[Finding]:
    findings: list[Finding] = []
    for line_number, candidate in _prose_lines(text, source):
        # Suppressions require a reason and apply to exactly one line.
        if re.search(r"english-audit-ignore:\s*\S", candidate, re.IGNORECASE):
            continue
        for pattern, compiled in COMPILED:
            for match in compiled.finditer(candidate):
                if pattern.category == "first_person" and match.group(0).isupper() and match.group(0) != "I":
                    continue
                findings.append(
                    Finding(
                        source=source,
                        line=line_number,
                        category=pattern.category,
                        severity=pattern.severity,
                        phrase=match.group(0),
                        rationale=pattern.rationale,
                        context=candidate.strip()[:500],
                    )
                )
    return findings

def is_excluded(path: str, excludes: Iterable[str]) -> bool:
    return any(fnmatch.fnmatch(path, rule) for rule in excludes)


def scan_tracked_tree(repo: Path, excludes: Iterable[str] = ()) -> tuple[list[Finding], dict]:
    findings: list[Finding] = []
    stats = {"tracked_paths": 0, "scanned_text_files": 0, "binary_files": [], "unreadable_files": [], "excluded_files": []}
    for rel in tracked_paths(repo):
        path = rel.as_posix()
        stats["tracked_paths"] += 1
        if is_excluded(path, excludes):
            stats["excluded_files"].append(path)
            continue
        absolute = repo / rel
        try:
            raw = absolute.read_bytes()
        except OSError as exc:
            stats["unreadable_files"].append({"path": path, "reason": str(exc)})
            continue
        if b"\0" in raw:
            stats["binary_files"].append(path)
            continue
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            stats["unreadable_files"].append({"path": path, "reason": f"not UTF-8: {exc}"})
            continue
        stats["scanned_text_files"] += 1
        findings.extend(scan_text(text, path))
    return findings, stats


def scan_commit_history(repo: Path, revision_range: str | None = None) -> list[Finding]:
    # Record delimiter and field delimiter are control bytes that cannot occur in Git subjects.
    args = ["log", "--format=%H%x1f%s%x1f%b%x1e"]
    if revision_range:
        args.append(revision_range)
    else:
        args.append("--all")
    raw = git(repo, *args)
    findings: list[Finding] = []
    for record in raw.split(b"\x1e"):
        if not record.strip():
            continue
        fields = record.decode("utf-8", "replace").split("\x1f", 2)
        if len(fields) != 3:
            continue
        sha, subject, body = fields
        source = f"git-commit:{sha[:12]}"
        findings.extend(scan_text(subject, source + ":subject"))
        findings.extend(scan_text(body, source + ":body"))
    return findings


def summarize(findings: list[Finding]) -> dict:
    by_category: dict[str, int] = {}
    by_severity: dict[str, int] = {}
    for f in findings:
        by_category[f.category] = by_category.get(f.category, 0) + 1
        by_severity[f.severity] = by_severity.get(f.severity, 0) + 1
    return {"total_findings": len(findings), "by_category": dict(sorted(by_category.items())), "by_severity": dict(sorted(by_severity.items()))}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd(), help="Git repository root or a path inside it")
    parser.add_argument("--include-history", action="store_true", help="Scan commit subjects and bodies reachable from local refs")
    parser.add_argument("--history-range", help="Scan commit subjects and bodies in a Git revision range, such as origin/main..HEAD")
    parser.add_argument("--strict", action="store_true", help="Return non-zero for high-confidence tautology findings")
    parser.add_argument("--fail-on-review", action="store_true", help="Return non-zero for any candidate finding; use only for reviewed baselines")
    parser.add_argument("--exclude", action="append", default=[], help="Git path glob to exclude; repeat as needed")
    parser.add_argument("--json-out", type=Path, help="Write machine-readable report")
    args = parser.parse_args(argv)

    repo = args.repo.resolve()
    try:
        top = Path(git(repo, "rev-parse", "--show-toplevel").decode().strip())
        findings, stats = scan_tracked_tree(top, args.exclude)
        if args.include_history and args.history_range:
            raise RuntimeError("choose either --include-history or --history-range")
        if args.include_history:
            findings.extend(scan_commit_history(top))
        elif args.history_range:
            findings.extend(scan_commit_history(top, args.history_range))
    except (RuntimeError, OSError) as exc:
        print(f"english-audit: {exc}", file=sys.stderr)
        return 2

    payload = {
        "schema_version": 1,
        "repository": str(top),
        "head": git(top, "rev-parse", "HEAD").decode().strip(),
        "tree": git(top, "rev-parse", "HEAD^{tree}").decode().strip(),
        "scope": "tracked working-tree text" + (" plus all reachable commit subjects and bodies" if args.include_history else (f" plus commit range {args.history_range}" if args.history_range else "")),
        "stats": stats,
        "summary": summarize(findings),
        "findings": [asdict(f) for f in findings],
        "policy": {
            "strict_fails_on": "tautology",
            "review_categories_are_candidates": True,
            "automatic_rewrites": False,
        },
    }
    print(json.dumps({k: v for k, v in payload.items() if k != "findings"}, indent=2))
    for f in findings:
        print(f"{f.severity.upper():6} {f.source}:{f.line}: [{f.category}] {f.phrase!r} :: {f.context}")
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    if args.fail_on_review and findings:
        return 1
    if args.strict and any(f.category == "tautology" for f in findings):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
