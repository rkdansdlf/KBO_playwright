"""Tests for Coverage Contract Matrix and 6-State Status Classifier."""

from __future__ import annotations

import pytest

from src.validators.coverage_contract_matrix import (
    TABLE_CONTRACTS,
    TableContractStatus,
    evaluate_table_contract,
)


class TestCoverageContractMatrix:
    def test_table_contracts_presence(self) -> None:
        assert "game" in TABLE_CONTRACTS
        assert "player_game_batting" in TABLE_CONTRACTS
        assert "game_events" in TABLE_CONTRACTS
        assert "futures_schedule" in TABLE_CONTRACTS

    def test_evaluate_contract_pass(self) -> None:
        eval_res = evaluate_table_contract("game", 2024, 720)
        assert eval_res.status == TableContractStatus.PASS
        assert eval_res.row_count == 720

    def test_evaluate_contract_early_era_known_limitation(self) -> None:
        # game_events starts in 2018
        eval_res = evaluate_table_contract("game_events", 2015, 0)
        assert eval_res.status == TableContractStatus.KNOWN_LIMITATION
        assert "prior to era 2018" in eval_res.message

    def test_evaluate_contract_defect_when_missing_rows(self) -> None:
        eval_res = evaluate_table_contract("game", 2024, 0, expected_row_min=100)
        assert eval_res.status == TableContractStatus.DEFECT
        assert "Expected >= 100 rows" in eval_res.message

    def test_evaluate_contract_defect_on_high_mandatory_null_rate(self) -> None:
        eval_res = evaluate_table_contract(
            "player_game_batting",
            2024,
            1000,
            null_rates={"player_id": 0.12},
        )
        assert eval_res.status == TableContractStatus.DEFECT
        assert "player_id" in eval_res.message
