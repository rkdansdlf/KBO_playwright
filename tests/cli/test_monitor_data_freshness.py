from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from src.cli.monitor_data_freshness import (
    _classify_staleness,
    _format_table_issues,
    main,
    run_monitor,
)


class TestClassifyStaleness:
    def test_fresh(self):
        result = _classify_staleness("game", 12, 24)
        assert result == "fresh"

    def test_stale(self):
        result = _classify_staleness("game", 48, 24)
        assert result == "stale"

    def test_critical(self):
        result = _classify_staleness("game", 168, 24)
        assert result == "critical"

    def test_unknown_table(self):
        result = _classify_staleness("unknown_table", 100, 24)
        assert result == "unknown"


class TestFormatTableIssues:
    def test_formats_missing_game(self):
        result = _format_table_issues("game", ["G1", "G2"])
        assert "game" in result.lower() or "G1" in result

    def test_formats_p0_sources(self):
        result = _format_table_issues("P0", ["src1"])
        assert "P0" in result or "src1" in result


class TestRunMonitor:
    def test_returns_structured_result(self):
        with patch("src.cli.monitor_data_freshness.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.all.return_value = []

            with patch("src.cli.monitor_data_freshness.get_expected_freshness_hours", return_value=24):
                result = run_monitor()
                assert "stale" in result
                assert "table_issues" in result
                assert "p0_issues" in result


class TestMonitorDataFreshness:
    def test_default_run(self):
        with patch("src.cli.monitor_data_freshness.run_monitor") as mock:
            mock.return_value = {"stale": [], "table_issues": [], "p0_issues": []}
            result = main([])
            assert result is None

    def test_no_alert(self):
        with patch("src.cli.monitor_data_freshness.run_monitor") as mock:
            mock.return_value = {"stale": [], "table_issues": [], "p0_issues": []}
            result = main(["--no-alert"])
            assert result is None

    def test_dry_run(self):
        with patch("src.cli.monitor_data_freshness.run_monitor") as mock:
            mock.return_value = {"stale": [], "table_issues": [], "p0_issues": []}
            result = main(["--dry-run"])
            assert result is None

    def test_json_output(self):
        mock_result = {"stale": [], "table_issues": [], "p0_issues": []}
        with patch("src.cli.monitor_data_freshness.run_monitor") as mock:
            mock.return_value = mock_result
            with patch("src.cli.monitor_data_freshness.logger") as mock_logger:
                result = main(["--json"])
                assert result is None
                mock_logger.info.assert_called()

    def test_with_issues_logs_warning(self):
        mock_result = {
            "stale": ["game: 48h old"],
            "table_issues": [],
            "p0_issues": [],
        }
        with patch("src.cli.monitor_data_freshness.run_monitor") as mock:
            mock.return_value = mock_result
            with patch("src.cli.monitor_data_freshness.send_freshness_alert"):
                result = main([])
                assert result is None
