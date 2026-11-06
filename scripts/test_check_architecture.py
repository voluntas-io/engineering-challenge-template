"""Regression tests for the architecture synchronization gate (stdlib unittest)."""
from __future__ import annotations

import unittest

import check_architecture as gate


class ArchitectureGateTests(unittest.TestCase):
    def test_every_current_rust_source_has_a_reviewed_responsibility(self) -> None:
        self.assertEqual(set(gate.collect_sources()), set(gate.SOURCE_RESPONSIBILITIES))

    def test_architecture_has_at_least_three_structurally_valid_diagrams(self) -> None:
        document = gate.DOC.read_text(encoding="utf-8")
        self.assertEqual([], gate.check_mermaid_structure(document))
        self.assertGreaterEqual(len(gate.MERMAID_RE.findall(document)), 3)

    def test_phase2_runtime_paths_are_integrated(self) -> None:
        self.assertEqual([], gate.module_integration_errors())

    def test_missing_runtime_anchor_is_reported(self) -> None:
        from pathlib import Path
        from tempfile import TemporaryDirectory
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "src").mkdir()
            for name in ("config.rs", "capacity.rs", "catalog.rs", "coordinator.rs"):
                (root / "src" / name).write_text("", encoding="utf-8")
            errors = gate.module_integration_errors(root)
            self.assertTrue(any("capacity startup" in error for error in errors), errors)

    def test_structural_checker_rejects_undeclared_nodes(self) -> None:
        invalid = "```mermaid\nflowchart LR\nA[\"Declared\"] --> B\n```"
        errors = gate.check_mermaid_structure(invalid)
        self.assertTrue(any("undeclared node B" in error for error in errors), errors)

    def test_structural_checker_rejects_unparsed_statements(self) -> None:
        invalid = "```mermaid\nflowchart LR\nA[\"Declared\"]\nnot a Mermaid statement\n```"
        errors = gate.check_mermaid_structure(invalid)
        self.assertTrue(any("unsupported/unparsed Mermaid statement" in error for error in errors), errors)


if __name__ == "__main__":
    unittest.main()
