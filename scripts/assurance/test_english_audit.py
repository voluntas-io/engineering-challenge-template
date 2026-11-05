from __future__ import annotations

import importlib.util
import subprocess
import tempfile
import unittest
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "english_audit.py"
SPEC = importlib.util.spec_from_file_location("english_audit", SCRIPT)
assert SPEC and SPEC.loader
english_audit = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = english_audit
SPEC.loader.exec_module(english_audit)


class EnglishAuditTests(unittest.TestCase):
    def test_detects_high_confidence_tautology(self):
        findings = english_audit.scan_text("The exact same input must produce the same output.", "sample.md")  # english-audit-ignore: detector unit-test fixture
        self.assertEqual([(f.category, f.phrase.lower()) for f in findings], [("tautology", "exact same")])  # english-audit-ignore: expected matcher phrase

    def test_detects_first_person_and_intent_adverb_as_review(self):
        findings = english_audit.scan_text("We intentionally retain the receive sequence.", "sample.md")  # english-audit-ignore: detector unit-test fixture
        categories = {f.category for f in findings}
        self.assertEqual(categories, {"first_person", "intent_adverb"})
        self.assertTrue(all(f.severity == "review" for f in findings))

    def test_first_person_detector_does_not_flag_acronyms_or_io(self):
        findings = english_audit.scan_text("US markets use FFI and I/O interfaces. We review them.", "sample.md")  # english-audit-ignore: test fixture
        self.assertEqual([(f.phrase, f.category) for f in findings], [("We", "first_person")])  # english-audit-ignore: expected detector result

    def test_does_not_flag_domain_term_by_itself(self):
        findings = english_audit.scan_text("The order intent enters the unknown state during recovery.", "sample.rs")
        self.assertEqual(findings, [])

    def test_scans_all_tracked_text_and_reports_binary(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            (repo / "README.md").write_text("The end result is recorded.\n", encoding="utf-8")  # english-audit-ignore: detector fixture
            (repo / "capture.bin").write_bytes(b"\x00\x01\x02")
            subprocess.run(["git", "-C", str(repo), "add", "README.md", "capture.bin"], check=True)
            subprocess.run(["git", "-C", str(repo), "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "baseline"], check=True)
            findings, stats = english_audit.scan_tracked_tree(repo)
            self.assertEqual(stats["tracked_paths"], 2)
            self.assertEqual(stats["scanned_text_files"], 1)
            self.assertEqual(stats["binary_files"], ["capture.bin"])
            self.assertEqual(findings[0].category, "tautology")

    def test_line_suppression_requires_a_reason_marker(self):
        findings = english_audit.scan_text(
            "The exact same result is shown. <!-- english-audit-ignore: literal example -->\n"
            "The exact same result is shown.",  # english-audit-ignore: detector fixture
            "sample.md",
        )
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].line, 2)

    def test_commit_history_range_is_supported(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            (repo / "a.txt").write_text("baseline\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(repo), "add", "a.txt"], check=True)
            env = {"GIT_AUTHOR_NAME": "Test", "GIT_AUTHOR_EMAIL": "test@example.invalid", "GIT_COMMITTER_NAME": "Test", "GIT_COMMITTER_EMAIL": "test@example.invalid"}
            import os
            base_env = os.environ.copy(); base_env.update(env)
            subprocess.run(["git", "-C", str(repo), "commit", "-qm", "baseline"], check=True, env=base_env)
            base = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
            (repo / "a.txt").write_text("change\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(repo), "commit", "-qam", "Remove the exact same value"], check=True, env=base_env)  # english-audit-ignore: commit-message detector fixture
            findings = english_audit.scan_commit_history(repo, f"{base}..HEAD")
            self.assertTrue(any(f.category == "tautology" and f.source.endswith(":subject") for f in findings))

    def test_exclusion_is_explicit_and_path_based(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            (repo / "old.md").write_text("repeat again\n", encoding="utf-8")  # english-audit-ignore: detector fixture
            subprocess.run(["git", "-C", str(repo), "add", "old.md"], check=True)
            findings, stats = english_audit.scan_tracked_tree(repo, ["old.md"])
            self.assertEqual(findings, [])
            self.assertEqual(stats["excluded_files"], ["old.md"])

    def test_first_person_scans_rust_comments_not_identifiers(self):
        code = "fn timestamp(us: i64) {}\n// We reject this state.\n"
        findings = english_audit.scan_text(code, "src/sample.rs")
        self.assertEqual([(f.phrase, f.line) for f in findings], [("We", 2)])

    def test_first_person_skips_mermaid_identifiers_and_link_destinations(self):
        text = "```mermaid\nH --> I[Input]\nI --> J[Output]\n```\n[Reference](https://example.invalid/what-we-do)\n"
        findings = english_audit.scan_text(text, "docs/diagram.md")
        self.assertEqual(findings, [])

    def test_svg_first_person_scans_visible_text_not_attributes_or_css(self):
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><style>#my-svg { fill: black; }</style><text>We require a review.</text></svg>'
        findings = english_audit.scan_text(svg, "diagram.svg")
        self.assertEqual([(f.phrase, f.category) for f in findings], [("We", "first_person")])

    def test_rust_code_identifiers_are_not_prose(self):
        code = "let value = Some(1);\nfn ensure() -> bool { true }\nlet never = false;\n"
        findings = english_audit.scan_text(code, "src/sample.rs")
        self.assertEqual(findings, [])

    def test_rust_comments_are_prose(self):
        code = "// This must never be silently dropped.\nlet value = Some(1);\n"
        findings = english_audit.scan_text(code, "src/sample.rs")
        self.assertEqual([(f.phrase, f.category) for f in findings], [("never", "unsupported_absolute_or_guarantee")])

    def test_svg_attributes_are_not_prose(self):
        svg = '<svg xmlns="http://www.w3.org/2000/svg" font-size="small"><text>small</text></svg>'
        findings = english_audit.scan_text(svg, "diagram.svg")
        self.assertEqual([(f.phrase, f.category) for f in findings], [("small", "vague_quantifier_or_scope")])

    def test_license_text_is_not_scanned(self):
        findings = english_audit.scan_text("copies or substantial portions of the Software.", "LICENSE")
        self.assertEqual(findings, [])

    def test_markdown_inline_code_is_not_prose(self):
        findings = english_audit.scan_text("Use `Some` and `never` as identifiers.\n", "sample.md")
        self.assertEqual(findings, [])


if __name__ == "__main__":
    unittest.main()
