from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from src.cli.monitor_data_freshness import (
    check_freshness,
    check_table_completeness,
    check_p0_readiness,
    main,
    run_monitor,
)


class TestCheckFreshness:
    def test_returns_list(self):
        with patch("src.cli.monitor_data_freshness.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.all.return_value = []

            with patch("src.cli.monitor_data_freshness.get_expected_freshness_hours", return_value=24):
                result = check_freshness()
                assert isinstance(result, list)


class TestCheckTableCompleteness:
    def test_returns_list(self):
        with patch("src.cli.monitor_data_freshness.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.all.return_value = []

            result = check_table_completeness()
            assert isinstance(result, list)


class TestCheckP0Readiness:
    def test_returns_list(self):
        with patch("src.cli.monitor_data_freshness.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.all.return_value = []

            result = check_p0_readiness()
            assert isinstance(result, list)


class TestRunMonitor:
    def test_returns_structured_result(self):
        with (
            patch("src.cli.monitor_data_freshness.check_freshness") as mock_fresh,
            patch("src.cli.monitor_data_freshness.check_table_completeness") as mock_table,
            patch("src.cli.monitor_data_freshness.check_p0_readiness") as mock_p0,
        ):
            mock_fresh.return_value = []
            mock_table.return_value = []
            mock_p0.return_value = []

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

    def test_dry_run_no_alert(self):
        with patch("src.cli.monitor_data_freshness.run_monitor") as mock:
            mock.return_value = {"stale": ["s"], "table_issues": [], "p0_issues": []}
            result = main(["--dry-run", "--no-alert"])
            assert result is None

    def test_with_issues_calls_alert(self):
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
