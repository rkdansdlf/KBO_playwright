from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy.exc import SQLAlchemyError
from unittest.mock import MagicMock, patch

from src.cli import morning_pbp_report
from src.cli.morning_pbp_report import (
    _append_affected_games,
    _append_detail_failures,
    _append_oci_skips,
    _append_relay_section,
    _append_validation_section,
    _build_telegram_message,
    _find_latest_summary,
    _query_pbp_validation_summary,
    _read_pbp_report_csv,
    main,
    run_morning_report,
)


class TestMorningPbpReport:
    def test_dry_run_no_summary(self):
        with patch("src.cli.morning_pbp_report._find_latest_summary", return_value=None):
            result = main(["--dry-run"])
            assert result == 0

    def test_dry_run_with_summary(self):
        with patch("src.cli.morning_pbp_report._find_latest_summary") as mock_find:
            mock_find.return_value = ("20250101", {"stability": {}})
            with patch("src.cli.morning_pbp_report._query_pbp_validation_summary", return_value={}):
                result = main(["--dry-run"])
                assert result == 0

    def test_specific_date(self):
        with patch("src.cli.morning_pbp_report._find_latest_summary") as mock_find:
            mock_find.return_value = ("20250101", {"stability": {}})
            with patch("src.cli.morning_pbp_report._query_pbp_validation_summary", return_value={}):
                result = main(["--date", "20250101", "--dry-run"])
                assert result == 0


class TestAppendRelaySection:
    def test_appends_relay_data(self):
        lines = []
        _append_relay_section(lines, {"relay": {"20250101LGSS0": {"status": "ok"}}}, [])
        assert len(lines) > 0
        assert "RELAY" in lines[0] or "relay" in lines[0].lower()

    def test_appends_failures(self):
        lines = []
        _append_relay_section(lines, {}, ["G1 failed"])
        assert any("G1" in line for line in lines)


class TestAppendDetailFailures:
    def test_appends_failures(self):
        lines = []
        _append_detail_failures(lines, ["fail1", "fail2"])
        assert len(lines) >= 2


class TestAppendValidationSection:
    def test_appends_validation(self):
        lines = []
        _append_validation_section(lines, {"verified": 5, "unverified": 2})
        assert len(lines) > 0


class TestAppendOciSkips:
    def test_appends_skips(self):
        lines = []
        _append_oci_skips(lines, {"skip_counts": {"no_data": 1}})
        assert len(lines) > 0


class TestAppendAffectedGames:
    def test_appends_games(self):
        lines = []
        _append_affected_games(lines, ["G1", "G2"])
        assert len(lines) >= 2


class TestBuildTelegramMessage:
    def test_builds_message(self):
        msg = _build_telegram_message(
            target_date="20250101",
            summary={"stability": {}},
            validation_counts={"verified": 5},
            dry_run=True,
        )
        assert isinstance(msg, str)
        assert "20250101" in msg


class TestReadPbpReportCsv:
    def test_returns_empty_for_missing_file(self):
        result = _read_pbp_report_csv("20990101")
        assert result == []


class TestFindLatestSummary:
    def test_returns_none_when_no_files(self, tmp_path):
        result = _find_latest_summary(str(tmp_path))
        assert result is None


class TestQueryPbpValidationSummary:
    def test_returns_dict(self):
        with patch("src.db.engine.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.group_by.return_value.all.return_value = []
            result = _query_pbp_validation_summary()
            assert isinstance(result, dict)

    def test_reads_status_rows(self):
        with patch("src.db.engine.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            result = mock_session.execute.return_value
            result.fetchall.side_effect = [[("verified", 3), ("weird_status", 2)], []]
            counts = _query_pbp_validation_summary()
            assert counts["verified"] == 3
            assert counts["other"] == 2

    def test_falls_back_to_metadata_query(self):
        with patch("src.db.engine.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            result = mock_session.execute.return_value
            result.fetchall.side_effect = [[], [("recovered", 4)]]
            counts = _query_pbp_validation_summary()
            assert counts["recovered"] == 4

    def test_handles_query_error(self):
        with patch("src.db.engine.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.side_effect = SQLAlchemyError("boom")
            counts = _query_pbp_validation_summary()
            assert isinstance(counts, dict)
            assert counts["verified"] == 0


class TestFindLatestSummaryDefaults:
    def test_default_date_no_file(self, tmp_path):
        monkeypatch_attr = tmp_path
        with patch.object(morning_pbp_report, "DAILY_SUMMARY_DIR", monkeypatch_attr):
            result = _find_latest_summary(None)
        assert result is None

    def test_default_date_with_file(self, tmp_path):
        seoul_tz = ZoneInfo("Asia/Seoul")
        yesterday = (datetime.now(seoul_tz) - timedelta(days=1)).strftime("%Y%m%d")
        (tmp_path / f"{yesterday}.json").write_text(json.dumps({"stability": {}}), encoding="utf-8")
        with patch.object(morning_pbp_report, "DAILY_SUMMARY_DIR", tmp_path):
            result = _find_latest_summary(None)
        assert result is not None
        assert result[0] == yesterday

    def test_parse_error_returns_none(self, tmp_path):
        (tmp_path / "20250101.json").write_text("{not valid json", encoding="utf-8")
        with patch.object(morning_pbp_report, "DAILY_SUMMARY_DIR", tmp_path):
            result = _find_latest_summary("20250101")
        assert result is None

    def test_reads_valid_summary(self, tmp_path):
        (tmp_path / "20250101.json").write_text(json.dumps({"stability": {"a": 1}}), encoding="utf-8")
        with patch.object(morning_pbp_report, "DAILY_SUMMARY_DIR", tmp_path):
            result = _find_latest_summary("20250101")
        assert result == ("20250101", {"stability": {"a": 1}})


class TestBuildTelegramMessageNonDryRun:
    def test_omits_dry_run_marker(self):
        msg = _build_telegram_message(
            target_date="20250101",
            summary={"stability": {}},
            validation_counts={"verified": 5},
            dry_run=False,
        )
        assert "Dry-run" not in msg


class TestAppendSampleLimits:
    def test_relay_failures_over_limit(self):
        lines = []
        failures = [f"G{i}" for i in range(15)]
        _append_relay_section(lines, {}, failures)
        assert any("more" in line for line in lines)

    def test_affected_games_over_limit(self):
        lines = []
        affected = [f"G{i}" for i in range(15)]
        _append_affected_games(lines, affected)
        assert any("more" in line for line in lines)


class TestReadPbpReportCsvPresent:
    def test_reads_rows(self, tmp_path):
        csv_path = tmp_path / "pbp_report_daily_20990101.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["game_id", "status"])
            writer.writeheader()
            writer.writerow({"game_id": "G1", "status": "ok"})
        with patch.object(morning_pbp_report, "DAILY_SUMMARY_DIR", tmp_path):
            rows = _read_pbp_report_csv("20990101")
        assert rows == [{"game_id": "G1", "status": "ok"}]

    def test_read_error_returns_empty(self, tmp_path):
        bad = tmp_path / "pbp_report_daily_20990101.csv"
        bad.mkdir()
        with patch.object(morning_pbp_report, "DAILY_SUMMARY_DIR", tmp_path):
            rows = _read_pbp_report_csv("20990101")
        assert rows == []


class TestRunMorningReportNonDryRun:
    def test_sends_when_summary_present(self):
        with patch.object(morning_pbp_report, "_find_latest_summary", return_value=("20250101", {"stability": {}})):
            with patch.object(morning_pbp_report, "_query_pbp_validation_summary", return_value={}):
                with patch("src.utils.alerting.SlackWebhookClient.send_alert", return_value=True) as mock_send:
                    result = run_morning_report("20250101", dry_run=False)
        assert result is True
        mock_send.assert_called_once()

    def test_sends_when_no_summary(self):
        with patch.object(morning_pbp_report, "_find_latest_summary", return_value=None):
            with patch("src.utils.alerting.SlackWebhookClient.send_alert", return_value=True) as mock_send:
                result = run_morning_report(dry_run=False)
        assert result is True
        mock_send.assert_called_once()
