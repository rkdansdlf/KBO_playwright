"""Unit tests for run_unified_quality_check CLI."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from src.cli.reports.run_unified_quality_check import build_parser, main
from src.services.quality_hub import (
    FreshnessSummary,
    QualityGateSummary,
    RegressionPackSummary,
    StandingsSummary,
    UnifiedQualityReport,
)


class TestUnifiedQualityCheckCLI:
    """Test CLI arguments, execution paths, and exit codes."""

    def test_build_parser_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([])
        assert args.year is None
        assert args.freshness_days == 7
        assert args.json is False
        assert args.strict is False

    def test_cli_execution_pass(self, capsys: object) -> None:
        mock_report = UnifiedQualityReport(
            timestamp="2026-08-21T00:00:00",
            overall_status="PASS",
            quality_score=100,
            quality_gate=QualityGateSummary(
                season=2025,
                league="REGULAR",
                batting_ok=True,
                pitching_ok=True,
                pa_formula_ok=True,
                team_batting_ok=True,
                team_pitching_ok=True,
                mismatch_count=0,
            ),
        )

        with (
            patch("src.cli.reports.run_unified_quality_check.get_db_session"),
            patch("src.cli.reports.run_unified_quality_check.QualityHub") as mock_hub_cls,
        ):
            mock_hub_instance = MagicMock()
            mock_hub_instance.run_full_audit.return_value = mock_report
            mock_hub_instance.format_markdown.return_value = "# PASS Report"
            mock_hub_cls.return_value = mock_hub_instance

            exit_code = main(["--year", "2025"])

        assert exit_code == 0

    def test_cli_json_output(self, capsys: object) -> None:
        mock_report = UnifiedQualityReport(
            timestamp="2026-08-21T00:00:00",
            overall_status="PASS",
            quality_score=100,
        )

        with (
            patch("src.cli.reports.run_unified_quality_check.get_db_session"),
            patch("src.cli.reports.run_unified_quality_check.QualityHub") as mock_hub_cls,
        ):
            mock_hub_instance = MagicMock()
            mock_hub_instance.run_full_audit.return_value = mock_report
            mock_hub_cls.return_value = mock_hub_instance

            exit_code = main(["--json"])

        assert exit_code == 0

    def test_cli_strict_warn_returns_1(self) -> None:
        mock_report = UnifiedQualityReport(
            timestamp="2026-08-21T00:00:00",
            overall_status="WARN",
            quality_score=85,
        )

        with (
            patch("src.cli.reports.run_unified_quality_check.get_db_session"),
            patch("src.cli.reports.run_unified_quality_check.QualityHub") as mock_hub_cls,
        ):
            mock_hub_instance = MagicMock()
            mock_hub_instance.run_full_audit.return_value = mock_report
            mock_hub_instance.format_markdown.return_value = "# WARN Report"
            mock_hub_cls.return_value = mock_hub_instance

            exit_code = main(["--strict"])

        assert exit_code == 1

    def test_cli_fail_returns_2(self) -> None:
        mock_report = UnifiedQualityReport(
            timestamp="2026-08-21T00:00:00",
            overall_status="FAIL",
            quality_score=50,
        )

        with (
            patch("src.cli.reports.run_unified_quality_check.get_db_session"),
            patch("src.cli.reports.run_unified_quality_check.QualityHub") as mock_hub_cls,
        ):
            mock_hub_instance = MagicMock()
            mock_hub_instance.run_full_audit.return_value = mock_report
            mock_hub_instance.format_markdown.return_value = "# FAIL Report"
            mock_hub_cls.return_value = mock_hub_instance

            exit_code = main([])

        assert exit_code == 2
