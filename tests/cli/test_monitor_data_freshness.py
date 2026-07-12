from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from src.cli.monitor_data_freshness import (
    check_freshness,
    check_table_completeness,
    check_p0_readiness,
    main,
    run_monitor,
    KST,
    _table_staleness_message,
)


class TestCheckFreshness:
    def test_returns_list(self):
        with patch("src.cli.monitor_data_freshness.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.all.return_value = []

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

    def test_with_no_alert_flag(self):
        mock_result = {
            "stale": ["game: 48h old"],
            "table_issues": [],
            "p0_issues": [],
        }
        with patch("src.cli.monitor_data_freshness.run_monitor") as mock:
            mock.return_value = mock_result
            result = main(["--no-alert"])
            assert result is None


class TestCheckFreshnessEdgeCases:
    def _make_fake_source(self, source_key="game_schedule", last_success_at=None, freq="daily"):
        src = MagicMock()
        src.source_key = source_key
        src.source_type = "daily"
        src.target_domain = "naver"
        src.crawl_frequency = freq
        src.last_success_at = last_success_at
        return src

    def test_stale_never_crawled(self):
        from src.cli.monitor_data_freshness import check_freshness

        source = self._make_fake_source(last_success_at=None)
        mock_ds_repo = MagicMock()
        mock_ds_repo.get_all_active.return_value = [source]

        with patch("src.cli.monitor_data_freshness.DataSourceRepository", return_value=mock_ds_repo):
            result = check_freshness(dry_run=False)

        assert len(result) > 0
        assert "STALE" in result[0]
        assert "never crawled" in result[0]

    def test_dry_run_returns_stale_findings_without_alert_delivery(self):
        source = self._make_fake_source(last_success_at=None)
        mock_ds_repo = MagicMock()
        mock_ds_repo.get_all_active.return_value = [source]

        with patch("src.cli.monitor_data_freshness.DataSourceRepository", return_value=mock_ds_repo):
            result = check_freshness(dry_run=True)

        assert result == ["[STALE] game_schedule: never crawled (type=daily, domain=naver)"]

    def test_stale_over_threshold(self):
        from datetime import datetime

        from src.cli.monitor_data_freshness import check_freshness

        source = self._make_fake_source(last_success_at=datetime(2020, 1, 1))
        mock_ds_repo = MagicMock()
        mock_ds_repo.get_all_active.return_value = [source]

        with patch("src.cli.monitor_data_freshness.DataSourceRepository", return_value=mock_ds_repo):
            result = check_freshness(dry_run=False)

        assert len(result) > 0
        assert "STALE" in result[0]

    def test_fresh_source_not_alerted(self):
        from datetime import datetime, timedelta

        from src.cli.monitor_data_freshness import check_freshness

        source = self._make_fake_source(last_success_at=datetime.now() - timedelta(hours=2))
        mock_ds_repo = MagicMock()
        mock_ds_repo.get_all_active.return_value = [source]

        with patch("src.cli.monitor_data_freshness.DataSourceRepository", return_value=mock_ds_repo):
            result = check_freshness(dry_run=False)

        assert result == []


class TestCheckTableCompletenessEdgeCases:
    def _make_row_mock(self, scalar_val):
        mock = MagicMock()
        mock.scalar.return_value = scalar_val
        return mock

    def test_empty_table(self):
        from src.cli.monitor_data_freshness import check_table_completeness

        mock_session = MagicMock()

        def exec_side_effect(query):
            query_str = str(query)
            if "COUNT" in query_str:
                return self._make_row_mock(0)
            return self._make_row_mock(None)

        mock_session.execute.side_effect = exec_side_effect

        with patch("src.cli.monitor_data_freshness.SessionLocal") as mock_sf:
            mock_sf.return_value.__enter__.return_value = mock_session

            result = check_table_completeness(dry_run=False)

        assert len(result) > 0
        assert any("EMPTY" in r for r in result)

    def test_sqlalchemy_error(self, caplog):
        from sqlalchemy.exc import OperationalError

        from src.cli.monitor_data_freshness import check_table_completeness

        mock_session = MagicMock()
        err = OperationalError("stmt", {}, Exception("db down"))
        mock_session.execute.side_effect = err

        with patch("src.cli.monitor_data_freshness.SessionLocal") as mock_sf:
            mock_sf.return_value.__enter__.return_value = mock_session

            with caplog.at_level(logging.ERROR):
                result = check_table_completeness(dry_run=False)

        assert len(result) > 0
        assert any("ERROR" in r for r in result)

    def test_populated_table_with_old_timestamp_is_stale_in_dry_run(self):
        mock_session = MagicMock()
        old_timestamp = datetime.now(KST) - timedelta(days=9)

        def exec_side_effect(query):
            if "COUNT" in str(query):
                return self._make_row_mock(1)
            return self._make_row_mock(old_timestamp)

        mock_session.execute.side_effect = exec_side_effect
        with patch("src.cli.monitor_data_freshness.SessionLocal") as mock_sf:
            mock_sf.return_value.__enter__.return_value = mock_session
            result = check_table_completeness(dry_run=True)

        assert any("[STALE] Table team_events" in issue for issue in result)

    def test_ticket_price_season_uses_current_season_policy(self):
        now = datetime(2026, 7, 1, tzinfo=KST)

        issue = _table_staleness_message(
            domain="ticket",
            table="ticket_prices",
            date_column="season",
            latest_value=2025,
            now=now,
        )

        assert issue is not None
        assert "required>=2026" in issue


class TestRunMonitorEdgeCases:
    def test_alert_sends_slack(self):
        with (
            patch("src.cli.monitor_data_freshness.check_freshness") as mock_fresh,
            patch("src.cli.monitor_data_freshness.check_table_completeness") as mock_table,
            patch("src.cli.monitor_data_freshness.check_p0_readiness") as mock_p0,
            patch("src.cli.monitor_data_freshness.SlackWebhookClient") as mock_slack,
        ):
            mock_fresh.return_value = ["game: stale"]
            mock_table.return_value = []
            mock_p0.return_value = []

            result = run_monitor(alert=True, dry_run=False)

            mock_slack.send_alert.assert_called_once()
            assert "stale" in result

    def test_dry_run_reports_issues_without_sending_slack(self):
        with (
            patch("src.cli.monitor_data_freshness.check_freshness", return_value=["source: stale"]),
            patch("src.cli.monitor_data_freshness.check_table_completeness", return_value=[]),
            patch("src.cli.monitor_data_freshness.check_p0_readiness", return_value=[]),
            patch("src.cli.monitor_data_freshness.SlackWebhookClient") as mock_slack,
        ):
            result = run_monitor(alert=True, dry_run=True)

        assert result["stale"] == ["source: stale"]
        mock_slack.send_alert.assert_not_called()

    def test_no_issues_logger(self, caplog):
        with (
            patch("src.cli.monitor_data_freshness.check_freshness") as mock_fresh,
            patch("src.cli.monitor_data_freshness.check_table_completeness") as mock_table,
            patch("src.cli.monitor_data_freshness.check_p0_readiness") as mock_p0,
        ):
            mock_fresh.return_value = []
            mock_table.return_value = []
            mock_p0.return_value = []

            result = run_monitor(alert=True, dry_run=False)

            assert result["stale"] == []
            assert result["table_issues"] == []
