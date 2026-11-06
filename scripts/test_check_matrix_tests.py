"""Regression tests for the requirement-matrix test-reference gate (stdlib unittest)."""
from __future__ import annotations

import unittest

import check_matrix_tests as gate


def matrix(evidence: str, implementation: str = "`src/example.rs`") -> str:
    return (
        "| ID | Requirement | Implementation | Offline / deterministic evidence | Status |\n"
        "|---|---|---|---|---|\n"
        f"| M1 | Example requirement | {implementation} | {evidence} | ok |\n"
    )


INVENTORY = gate.Inventory(
    lib={
        "collector::state::tests::clocked_failover_anchors_resumed_deltas_and_preserves_receive_times",
        "journal::tests::first_journal_test",
        "journal::tests::second_journal_test",
        "verify::tests::empty_snapshot_is_a_retirement_boundary_not_a_mismatch",
    },
    files={
        "tests/e2e_mock_exchange.rs": {
            "steady_state_history_matches_mock_exchange_truth",
            "single_socket_loss_fails_over_and_emits_anchoring_snapshot",
        }
    },
    helpers={
        "tests/e2e_mock_exchange.rs": {
            "assert_per_symbol_anchor",
            "assert_delta_receive_times_monotonic",
        }
    },
)


def check(evidence: str, implementation: str = "`src/example.rs`") -> tuple[list[str], dict[str, int]]:
    references = gate.extract_references(matrix(evidence, implementation))
    return gate.validate(references, INVENTORY, "fixture.md")


class MatrixReferenceTests(unittest.TestCase):
    def test_valid_reference_forms_resolve(self) -> None:
        evidence = (
            "`collector::state::tests::clocked_failover_anchors_resumed_deltas_and_preserves_receive_times`; "
            "`journal::tests::{first_journal_test,second_journal_test}`; "
            "`journal::tests::*`; "
            "`tests/e2e_mock_exchange.rs::steady_state_history_matches_mock_exchange_truth`; "
            "E2E `single_socket_loss_fails_over_and_emits_anchoring_snapshot`; "
            "E2E `assert_per_symbol_anchor`"
        )
        errors, kinds = check(evidence)
        self.assertEqual([], errors)
        self.assertEqual(3, kinds["qualified"])
        self.assertEqual(1, kinds["bare"])
        self.assertEqual(1, kinds["wildcard"])
        self.assertEqual(1, kinds["file"])
        self.assertEqual(1, kinds["helper"])

    def test_short_bare_lib_reference_resolves(self) -> None:
        errors, kinds = check("`empty_snapshot_is_a_retirement_boundary_not_a_mismatch`")
        self.assertEqual([], errors)
        self.assertEqual(1, kinds["bare"])

    def test_only_evidence_columns_are_scanned(self) -> None:
        references = gate.extract_references(
            matrix("E2E `steady_state_history_matches_mock_exchange_truth`", "`module::tests::missing`")
        )
        self.assertEqual(
            ["steady_state_history_matches_mock_exchange_truth"],
            [reference.text for reference in references],
        )

    def test_missing_qualified_test_is_rejected(self) -> None:
        errors, _ = check("`journal::tests::removed_test`")
        self.assertEqual(1, len(errors))
        self.assertIn("removed_test", errors[0])

    def test_missing_bare_test_is_rejected(self) -> None:
        errors, _ = check("`sequence_reset_fails_over_to_valid_shadow`")
        self.assertEqual(1, len(errors))
        self.assertIn("sequence_reset_fails_over_to_valid_shadow", errors[0])

    def test_missing_group_member_is_rejected(self) -> None:
        errors, _ = check("`journal::tests::{first_journal_test,removed_test}`")
        self.assertEqual(1, len(errors))
        self.assertIn("journal::tests::removed_test", errors[0])

    def test_wildcard_without_matches_is_rejected(self) -> None:
        errors, _ = check("`removed_module::tests::*`")
        self.assertEqual(1, len(errors))
        self.assertIn("matches no test", errors[0])

    def test_unknown_integration_target_is_rejected(self) -> None:
        errors, _ = check("`tests/missing_fixture.rs::any_test`")
        self.assertEqual(1, len(errors))
        self.assertIn("not a known integration-test target", errors[0])

    def test_missing_file_qualified_test_is_rejected(self) -> None:
        errors, _ = check("`tests/e2e_mock_exchange.rs::removed_scenario`")
        self.assertEqual(1, len(errors))
        self.assertIn("removed_scenario", errors[0])

    def test_missing_e2e_helper_is_rejected(self) -> None:
        errors, _ = check("E2E `assert_removed_condition`")
        self.assertEqual(1, len(errors))
        self.assertIn("assert_removed_condition", errors[0])

    def test_ambiguous_bare_name_is_rejected(self) -> None:
        inventory = gate.Inventory(
            lib={
                "first::tests::same_name",
                "second::tests::same_name",
            }
        )
        references = gate.extract_references(matrix("`same_name`"))
        errors, _ = gate.validate(references, inventory, "fixture.md")
        self.assertEqual(1, len(errors))
        self.assertIn("ambiguous", errors[0])

    def test_inventory_json_round_trip_preserves_helpers(self) -> None:
        restored = gate.Inventory.from_json(INVENTORY.to_json())
        self.assertEqual(INVENTORY.lib, restored.lib)
        self.assertEqual(INVENTORY.files, restored.files)
        self.assertEqual(INVENTORY.helpers, restored.helpers)


if __name__ == "__main__":
    unittest.main()
