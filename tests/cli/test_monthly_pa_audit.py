"""Tests for src.cli.monthly_pa_audit."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from src.cli import monthly_pa_audit


class TestRunMonthlyPaAudit:
    def test_success_returns_fixed_rows(self) -> None:
        with patch("src.cli.monthly_pa_audit.fix_year_formula", return_value=42) as mock_fix:
            result = monthly_pa_audit.run_monthly_pa_audit(2025)
        mock_fix.assert_called_once_with(2025, dry_run=False)
        assert result == 42

    def test_exception_raises_runtime_error(self) -> None:
        from sqlalchemy.exc import SQLAlchemyError

        with patch("src.cli.monthly_pa_audit.fix_year_formula", side_effect=SQLAlchemyError("db fail")):
            with pytest.raises(RuntimeError, match="PA formula audit failed"):
                monthly_pa_audit.run_monthly_pa_audit(2025)


class TestCrawlMonthlyPaAuditJob:
    def test_skips_before_2020(self) -> None:
        with patch("src.cli.monthly_pa_audit.datetime") as mock_dt:
            mock_dt.now.return_value = MagicMock(year=2020)
            with patch("src.cli.monthly_pa_audit.run_monthly_pa_audit") as mock_run:
                monthly_pa_audit.crawl_monthly_pa_audit_job()
            mock_run.assert_not_called()

    def test_runs_for_previous_year(self) -> None:
        with patch("src.cli.monthly_pa_audit.datetime") as mock_dt:
            mock_dt.now.return_value = MagicMock(year=2026)
            with patch("src.cli.monthly_pa_audit.run_monthly_pa_audit", return_value=10) as mock_run:
                monthly_pa_audit.crawl_monthly_pa_audit_job()
            mock_run.assert_called_once_with(2025)


class TestMain:
    def test_specific_year(self) -> None:
        with patch("src.cli.monthly_pa_audit.run_monthly_pa_audit", return_value=3) as mock_run:
            monthly_pa_audit.main(["--year", "2023"])
        mock_run.assert_called_once_with(2023)

    def test_skip_before_2020(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.INFO):
            monthly_pa_audit.main(["--year", "2019"])
        assert "Skipping" in caplog.text

    def test_failure_exits_with_code_1(self) -> None:
        with patch("src.cli.monthly_pa_audit.run_monthly_pa_audit", side_effect=RuntimeError("fail")):
            with pytest.raises(SystemExit) as exc_info:
                monthly_pa_audit.main(["--year", "2025"])
            assert exc_info.value.code == 1
