"""Unit tests for monthly_pa_audit CLI."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.cli.monthly_pa_audit import main


class TestMonthlyPaAuditCLI:
    def test_main_default_year(self) -> None:
        with patch("src.cli.monthly_pa_audit.run_monthly_pa_audit", return_value=5) as mock_run:
            result = main([])
            assert result == 5
            mock_run.assert_called_once()

    def test_main_with_year(self) -> None:
        with patch("src.cli.monthly_pa_audit.run_monthly_pa_audit", return_value=10) as mock_run:
            result = main(["--year", "2025"])
            assert result == 10
            mock_run.assert_called_once_with(2025)

    def test_main_skip_before_2020(self, caplog) -> None:
        with patch("src.cli.monthly_pa_audit.run_monthly_pa_audit") as mock_run:
            result = main(["--year", "2019"])
            assert result == 0
            mock_run.assert_not_called()

    def test_main_exception_exits(self) -> None:
        with patch("src.cli.monthly_pa_audit.run_monthly_pa_audit", side_effect=RuntimeError("fail")):
            with pytest.raises(SystemExit):
                main(["--year", "2025"])
