"""Unit tests for quality_gate_check CLI."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from src.cli.quality_gate_check import main


class TestQualityGateCheckCLI:
    def test_main_default_year(self, caplog) -> None:
        mock_result = {
            "ok": True,
            "batting": {"ok": True, "mismatches": []},
            "pitching": {"ok": True, "mismatches": []},
            "pa_formula": {"ok": True, "mismatches": []},
            "team_batting": {"ok": True, "mismatches": []},
            "team_pitching": {"ok": True, "mismatches": []},
        }
        with patch("src.cli.quality_gate_check.SessionLocal") as mock_session:
            mock_session.return_value.__enter__return_value = MagicMock()
            with patch("src.cli.quality_gate_check.run_quality_gate", return_value=mock_result):
                result = main([])
                assert result == 0

    def test_main_with_year(self) -> None:
        mock_result = {
            "ok": True,
            "batting": {"ok": True, "mismatches": []},
            "pitching": {"ok": True, "mismatches": []},
            "pa_formula": {"ok": True, "mismatches": []},
            "team_batting": {"ok": True, "mismatches": []},
            "team_pitching": {"ok": True, "mismatches": []},
        }
        with patch("src.cli.quality_gate_check.SessionLocal") as mock_session:
            mock_session.return_value.__enter__return_value = MagicMock()
            with patch("src.cli.quality_gate_check.run_quality_gate", return_value=mock_result) as mock_run:
                result = main(["--year", "2025"])
                assert result == 0
                mock_run.assert_called_once()

    def test_main_json_output(self, capsys) -> None:
        mock_result = {
            "ok": True,
            "batting": {"ok": True, "mismatches": []},
            "pitching": {"ok": True, "mismatches": []},
            "pa_formula": {"ok": True, "mismatches": []},
            "team_batting": {"ok": True, "mismatches": []},
            "team_pitching": {"ok": True, "mismatches": []},
        }
        with patch("src.cli.quality_gate_check.SessionLocal") as mock_session:
            mock_session.return_value.__enter__return_value = MagicMock()
            with patch("src.cli.quality_gate_check.run_quality_gate", return_value=mock_result):
                result = main(["--year", "2025", "--json"])
                assert result == 0

    def test_main_not_ok(self) -> None:
        mock_result = {
            "ok": False,
            "batting": {"ok": False, "mismatches": [{"player_id": 1}]},
            "pitching": {"ok": True, "mismatches": []},
            "pa_formula": {"ok": True, "mismatches": []},
            "team_batting": {"ok": True, "mismatches": []},
            "team_pitching": {"ok": True, "mismatches": []},
        }
        with patch("src.cli.quality_gate_check.SessionLocal") as mock_session:
            mock_session.return_value.__enter__return_value = MagicMock()
            with patch("src.cli.quality_gate_check.run_quality_gate", return_value=mock_result):
                result = main(["--year", "2025"])
                assert result == 1
