"""Regression tests for the requirement-matrix evidence gate (stdlib unittest)."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import check_matrix_tests as gate


SOURCE_FILES = {
    "src/alpha.rs": """
#[test]
fn present_test() {}

#[tokio::test(flavor = "multi_thread")]
async fn async_test() {}
""",
    "src/beta.rs": """
#[test]
fn other_test() {}
""",
    "src/gamma.rs": """
#[test]
fn foo() {}
""",
    "tests/e2e.rs": """
#[test]
fn e2e_test() {}
""",
    "tests/live.rs": """
#[test]
#[ignore]
fn live_test() {}
""",
}

REQUIREMENTS = ("M1", "E1")


def matrix(*rows: tuple[str, tuple[str, ...]]) -> str:
    lines = [
        "| ID | Requirement | Offline / deterministic evidence | Status |",
        "|---|---|---|---|",
    ]
    for requirement, references in rows:
        evidence = ", ".join(f"`{reference}`" for reference in references) or "none"
        lines.append(f"| {requirement} | Example requirement | {evidence} | ok |")
    return "\n".join(lines) + "\n"


class MatrixFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        for name, content in SOURCE_FILES.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
        self.tests = gate.discover_tests(self.root)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def check(self, markdown: str) -> list[str]:
        errors, _ = gate.validate(
            markdown,
            self.tests,
            required_requirements=REQUIREMENTS,
            required_tests=frozenset(),
        )
        return errors


class DiscoveryTests(MatrixFixture):
    def test_test_attributes_are_discovered_in_the_named_file(self) -> None:
        self.assertEqual({"present_test", "async_test"}, self.tests["src/alpha.rs"])
        self.assertEqual({"other_test"}, self.tests["src/beta.rs"])
        self.assertEqual({"e2e_test"}, self.tests["tests/e2e.rs"])

    def test_ignored_tests_are_still_declared_tests(self) -> None:
        self.assertEqual({"live_test"}, self.tests["tests/live.rs"])


class MatrixReferenceTests(MatrixFixture):
    def test_valid_file_qualified_references_pass(self) -> None:
        errors = self.check(
            matrix(
                (
                    "M1",
                    (
                        "src/alpha.rs::present_test",
                        "src/alpha.rs::async_test",
                        "tests/e2e.rs::e2e_test",
                    ),
                ),
                ("E1", ("src/beta.rs::other_test",)),
            )
        )
        self.assertEqual([], errors)

    def test_non_reference_inline_code_is_ignored(self) -> None:
        errors = self.check(
            matrix(
                ("M1", ("src/alpha.rs::present_test", "SIGINT", "SCALE_SYMBOLS=N")),
                ("E1", ("src/beta.rs::other_test",)),
            )
        )
        self.assertEqual([], errors)

    def test_missing_source_file_is_rejected(self) -> None:
        errors = self.check(
            matrix(
                ("M1", ("src/missing.rs::present_test",)),
                ("E1", ("src/beta.rs::other_test",)),
            )
        )
        self.assertEqual(1, len(errors))
        self.assertIn("src/missing.rs does not exist", errors[0])

    def test_test_declared_in_a_different_file_is_rejected(self) -> None:
        errors = self.check(
            matrix(
                ("M1", ("src/beta.rs::present_test",)),
                ("E1", ("src/beta.rs::other_test",)),
            )
        )
        self.assertEqual(1, len(errors))
        self.assertIn("present_test is not declared in src/beta.rs", errors[0])

    def test_absent_test_in_an_existing_file_is_rejected(self) -> None:
        errors = self.check(
            matrix(
                ("M1", ("src/alpha.rs::removed_test",)),
                ("E1", ("src/alpha.rs::present_test",)),
            )
        )
        self.assertEqual(1, len(errors))
        self.assertIn("removed_test is not declared in src/alpha.rs", errors[0])

    def test_wildcard_reference_is_rejected(self) -> None:
        errors = self.check(
            matrix(
                ("M1", ("src/alpha.rs::*",)),
                ("E1", ("src/beta.rs::other_test",)),
            )
        )
        self.assertTrue(any("wildcards are not allowed" in error for error in errors))

    def test_grouped_reference_is_rejected(self) -> None:
        errors = self.check(
            matrix(
                ("M1", ("src/alpha.rs::{present_test,async_test}",)),
                ("E1", ("src/beta.rs::other_test",)),
            )
        )
        self.assertTrue(any("grouped references are not allowed" in error for error in errors))

    def test_bare_snake_case_name_is_rejected(self) -> None:
        errors = self.check(
            matrix(
                ("M1", ("present_test",)),
                ("E1", ("src/beta.rs::other_test",)),
            )
        )
        self.assertTrue(any("bare test name is ambiguous" in error for error in errors))

    def test_bare_single_word_test_name_is_rejected(self) -> None:
        errors = self.check(
            matrix(
                ("M1", ("foo",)),
                ("E1", ("src/beta.rs::other_test",)),
            )
        )
        self.assertTrue(any("bare test name is ambiguous" in error for error in errors))

    def test_module_qualified_rust_path_is_rejected(self) -> None:
        errors = self.check(
            matrix(
                ("M1", ("alpha::tests::present_test",)),
                ("E1", ("src/beta.rs::other_test",)),
            )
        )
        self.assertTrue(any("must be file-qualified" in error for error in errors))


class RequirementSetTests(MatrixFixture):
    def test_missing_required_requirement_is_rejected(self) -> None:
        errors = self.check(matrix(("M1", ("src/alpha.rs::present_test",))))
        self.assertEqual(1, len(errors))
        self.assertIn("missing required requirement E1", errors[0])

    def test_duplicate_requirement_is_rejected(self) -> None:
        errors = self.check(
            matrix(
                ("M1", ("src/alpha.rs::present_test",)),
                ("M1", ("src/alpha.rs::async_test",)),
                ("E1", ("src/beta.rs::other_test",)),
            )
        )
        self.assertTrue(any("duplicate requirement ID M1" in error for error in errors))

    def test_requirement_without_a_reference_is_rejected(self) -> None:
        errors = self.check(
            matrix(
                ("M1", ()),
                ("E1", ("src/beta.rs::other_test",)),
            )
        )
        self.assertIn("requirement M1 has no test reference", errors)

    def test_unexpected_requirement_id_is_rejected(self) -> None:
        errors = self.check(
            matrix(
                ("M1", ("src/alpha.rs::present_test",)),
                ("E1", ("src/beta.rs::other_test",)),
                ("M10", ("src/gamma.rs::foo",)),
            )
        )
        self.assertTrue(any("unexpected requirement ID M10" in error for error in errors))

    def test_missing_mandatory_evidence_test_is_rejected(self) -> None:
        errors, _ = gate.validate(
            matrix(
                ("M1", ("src/alpha.rs::present_test",)),
                ("E1", ("src/beta.rs::other_test",)),
            ),
            self.tests,
            required_requirements=REQUIREMENTS,
            required_tests=frozenset({"present_test", "absent_test"}),
        )
        self.assertIn(
            "mandatory evidence test is missing from the matrix: absent_test", errors
        )


class RepositoryMatrixTest(unittest.TestCase):
    def test_checked_in_matrix_passes_the_complete_contract(self) -> None:
        errors, references = gate.validate(
            gate.MATRIX.read_text(encoding="utf-8"), gate.discover_tests(gate.ROOT)
        )
        self.assertEqual([], errors)
        self.assertGreaterEqual(len(references), len(gate.REQUIRED_EVIDENCE_TESTS))
        self.assertEqual(
            gate.REQUIRED_EVIDENCE_TESTS,
            gate.REQUIRED_EVIDENCE_TESTS
            & {reference.test for reference in references},
        )


if __name__ == "__main__":
    unittest.main()
