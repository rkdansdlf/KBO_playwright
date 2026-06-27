"""Tests for trend_tracker — quality metric trend detection."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.constants import KST
from src.monitoring.trend_tracker import TrendTracker


def _today_str() -> str:
    return datetime.now(KST).strftime("%Y%m%d")


def _date_str(days_ago: int) -> str:
    return (datetime.now(KST) - timedelta(days=days_ago)).strftime("%Y%m%d")


def _make_report(date_str: str, metrics: dict) -> dict:
    return {
        "generated_at": f"{date_str}T12:00:00",
        "metrics": {
            "date": date_str,
            **metrics,
        },
    }


def _write_report(tmp_path: Path, report: dict) -> None:
    date_str = report["metrics"]["date"]
    path = tmp_path / f"{date_str}.json"
    path.write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")


class TestLoadReports:
    def test_empty_directory(self, tmp_path: Path) -> None:
        tracker = TrendTracker(report_dir=tmp_path)
        assert tracker.load_reports(days=30) == []

    def test_nonexistent_directory(self, tmp_path: Path) -> None:
        tracker = TrendTracker(report_dir=tmp_path / "nonexistent")
        assert tracker.load_reports(days=30) == []

    def test_loads_valid_json(self, tmp_path: Path) -> None:
        date_str = _date_str(0)
        report = _make_report(date_str, {"completed_count": 10})
        _write_report(tmp_path, report)
        tracker = TrendTracker(report_dir=tmp_path)
        result = tracker.load_reports(days=30)
        assert len(result) == 1
        assert result[0]["metrics"]["completed_count"] == 10

    def test_skips_invalid_json(self, tmp_path: Path) -> None:
        (tmp_path / "20250601.json").write_text("not json", encoding="utf-8")
        tracker = TrendTracker(report_dir=tmp_path)
        assert tracker.load_reports(days=30) == []

    def test_filters_by_cutoff(self, tmp_path: Path) -> None:
        old_date = _date_str(60)
        recent_date = _date_str(0)
        old = _make_report(old_date, {"completed_count": 5})
        recent = _make_report(recent_date, {"completed_count": 10})
        _write_report(tmp_path, old)
        _write_report(tmp_path, recent)
        tracker = TrendTracker(report_dir=tmp_path)
        result = tracker.load_reports(days=30)
        assert len(result) == 1
        assert result[0]["metrics"]["date"] == recent_date

    def test_deduplicates_by_date(self, tmp_path: Path) -> None:
        date_str = _date_str(0)
        r1 = _make_report(date_str, {"completed_count": 5})
        r2 = _make_report(date_str, {"completed_count": 10})
        r2["generated_at"] = f"{date_str}T14:00:00"
        _write_report(tmp_path, r1)
        _write_report(tmp_path, r2)
        tracker = TrendTracker(report_dir=tmp_path)
        result = tracker.load_reports(days=30)
        assert len(result) == 1
        assert result[0]["metrics"]["completed_count"] == 10

    def test_dedup_keeps_older_when_newer_missing(self, tmp_path: Path) -> None:
        date_str = _date_str(0)
        r1 = _make_report(date_str, {"completed_count": 5})
        r1["generated_at"] = f"{date_str}T14:00:00"
        r2 = _make_report(date_str, {"completed_count": 10})
        r2["generated_at"] = f"{date_str}T10:00:00"
        (tmp_path / f"{date_str}.json").write_text(json.dumps(r1, ensure_ascii=False), encoding="utf-8")
        (tmp_path / f"{date_str}_2.json").write_text(json.dumps(r2, ensure_ascii=False), encoding="utf-8")
        tracker = TrendTracker(report_dir=tmp_path)
        result = tracker.load_reports(days=30)
        assert len(result) == 1
        assert result[0]["metrics"]["completed_count"] == 5


class TestGetTrend:
    def test_stable_with_few_values(self, tmp_path: Path) -> None:
        date_str = _date_str(0)
        report = _make_report(date_str, {"completed_count": 10})
        _write_report(tmp_path, report)
        tracker = TrendTracker(report_dir=tmp_path)
        trend = tracker.get_trend("metrics.completed_count")
        assert trend["direction"] == "stable"
        assert len(trend["values"]) == 1

    def test_increasing_trend(self, tmp_path: Path) -> None:
        for i in range(5):
            date_str = _date_str(4 - i)
            report = _make_report(date_str, {"completed_count": i * 10})
            _write_report(tmp_path, report)
        tracker = TrendTracker(report_dir=tmp_path)
        trend = tracker.get_trend("metrics.completed_count", days=7)
        assert trend["direction"] == "increasing"

    def test_decreasing_trend(self, tmp_path: Path) -> None:
        for i in range(5):
            date_str = _date_str(4 - i)
            report = _make_report(date_str, {"completed_count": (4 - i) * 10})
            _write_report(tmp_path, report)
        tracker = TrendTracker(report_dir=tmp_path)
        trend = tracker.get_trend("metrics.completed_count", days=7)
        assert trend["direction"] == "decreasing"

    def test_missing_metric_key(self, tmp_path: Path) -> None:
        report = _make_report("20250601", {"completed_count": 10})
        _write_report(tmp_path, report)
        tracker = TrendTracker(report_dir=tmp_path)
        trend = tracker.get_trend("metrics.nonexistent")
        assert trend["values"] == []
        assert trend["direction"] == "stable"

    def test_skips_none_values_in_trend(self, tmp_path: Path) -> None:
        for i in range(5):
            date_str = _date_str(4 - i)
            metrics = {"completed_count": i * 10} if i % 2 == 0 else {}
            report = _make_report(date_str, metrics)
            _write_report(tmp_path, report)
        tracker = TrendTracker(report_dir=tmp_path)
        trend = tracker.get_trend("metrics.completed_count", days=7)
        assert len(trend["values"]) == 3

    def test_stable_with_non_monotonic_values(self, tmp_path: Path) -> None:
        values = [10, 20, 15]
        for i, val in enumerate(values):
            date_str = _date_str(2 - i)
            report = _make_report(date_str, {"completed_count": val})
            _write_report(tmp_path, report)
        tracker = TrendTracker(report_dir=tmp_path)
        trend = tracker.get_trend("metrics.completed_count", days=7)
        assert trend["direction"] == "stable"


class TestDetectDegradations:
    def test_no_reports(self, tmp_path: Path) -> None:
        tracker = TrendTracker(report_dir=tmp_path)
        assert tracker.detect_degradations({"metrics.x": 50.0}) == []

    def test_single_report(self, tmp_path: Path) -> None:
        report = _make_report("20250601", {"metrics.relay_integrity.recent_missing_count": 5})
        _write_report(tmp_path, report)
        tracker = TrendTracker(report_dir=tmp_path)
        assert tracker.detect_degradations({"metrics.relay_integrity.recent_missing_count": 50.0}) == []

    def test_detects_positive_degradation(self, tmp_path: Path) -> None:
        r1 = _make_report(_date_str(14), {"relay_integrity": {"recent_missing_count": 10}})
        r2 = _make_report(_date_str(0), {"relay_integrity": {"recent_missing_count": 30}})
        _write_report(tmp_path, r1)
        _write_report(tmp_path, r2)
        tracker = TrendTracker(report_dir=tmp_path)
        alerts = tracker.detect_degradations(
            {"metrics.relay_integrity.recent_missing_count": 50.0},
            days=30,
        )
        assert len(alerts) == 1
        assert alerts[0]["metric"] == "metrics.relay_integrity.recent_missing_count"
        assert alerts[0]["first"] == 10
        assert alerts[0]["last"] == 30
        assert alerts[0]["severity"] == "WARN"

    def test_no_degradation_within_threshold(self, tmp_path: Path) -> None:
        r1 = _make_report(_date_str(14), {"relay_integrity": {"recent_missing_count": 10}})
        r2 = _make_report(_date_str(0), {"relay_integrity": {"recent_missing_count": 12}})
        _write_report(tmp_path, r1)
        _write_report(tmp_path, r2)
        tracker = TrendTracker(report_dir=tmp_path)
        alerts = tracker.detect_degradations(
            {"metrics.relay_integrity.recent_missing_count": 50.0},
            days=30,
        )
        assert alerts == []

    def test_negative_threshold_detects_improvement(self, tmp_path: Path) -> None:
        r1 = _make_report(_date_str(14), {"relay_integrity": {"recent_missing_count": 30}})
        r2 = _make_report(_date_str(0), {"relay_integrity": {"recent_missing_count": 10}})
        _write_report(tmp_path, r1)
        _write_report(tmp_path, r2)
        tracker = TrendTracker(report_dir=tmp_path)
        alerts = tracker.detect_degradations(
            {"metrics.relay_integrity.recent_missing_count": -50.0},
            days=30,
        )
        assert len(alerts) == 1
        assert alerts[0]["pct_change"] < 0

    def test_skips_metrics_with_none_values(self, tmp_path: Path) -> None:
        r1 = _make_report(_date_str(14), {"relay_integrity": {"recent_missing_count": 10}})
        r2 = _make_report(_date_str(0), {})
        _write_report(tmp_path, r1)
        _write_report(tmp_path, r2)
        tracker = TrendTracker(report_dir=tmp_path)
        alerts = tracker.detect_degradations(
            {"metrics.relay_integrity.recent_missing_count": 50.0},
            days=30,
        )
        assert alerts == []

    def test_small_triggers_on_any_increase(self, tmp_path: Path) -> None:
        r1 = _make_report(_date_str(14), {"relay_integrity": {"recent_missing_count": 1}})
        r2 = _make_report(_date_str(0), {"relay_integrity": {"recent_missing_count": 100}})
        _write_report(tmp_path, r1)
        _write_report(tmp_path, r2)
        tracker = TrendTracker(report_dir=tmp_path)
        alerts = tracker.detect_degradations(
            {"metrics.relay_integrity.recent_missing_count": 50.0},
            days=30,
        )
        assert len(alerts) == 1


class TestResolveKey:
    def test_simple_key(self) -> None:
        tracker = TrendTracker()
        report = {"metrics": {"completed_count": 42}}
        assert tracker._resolve_key(report, "metrics.completed_count") == 42.0

    def test_missing_key(self) -> None:
        tracker = TrendTracker()
        report = {"metrics": {}}
        assert tracker._resolve_key(report, "metrics.nonexistent") is None

    def test_non_dict_intermediate(self) -> None:
        tracker = TrendTracker()
        report = {"metrics": "not_a_dict"}
        assert tracker._resolve_key(report, "metrics.completed_count") is None

    def test_non_numeric_value(self) -> None:
        tracker = TrendTracker()
        report = {"metrics": {"status": "ok"}}
        assert tracker._resolve_key(report, "metrics.status") is None


class TestExtractReportDate:
    def test_from_metrics_date(self) -> None:
        tracker = TrendTracker()
        report = {"metrics": {"date": "2025-06-01"}}
        path = Path("20250601.json")
        result = tracker._extract_report_date(report, path)
        assert result.year == 2025
        assert result.month == 6
        assert result.day == 1

    def test_fallback_to_filename(self) -> None:
        tracker = TrendTracker()
        report = {"metrics": {}}
        path = Path("20250615.json")
        result = tracker._extract_report_date(report, path)
        assert result.year == 2025
        assert result.month == 6
        assert result.day == 15

    def test_invalid_metrics_date_falls_back(self) -> None:
        tracker = TrendTracker()
        report = {"metrics": {"date": "invalid"}}
        path = Path("20250615.json")
        result = tracker._extract_report_date(report, path)
        assert result.year == 2025


class TestGeneratedAtKey:
    def test_returns_timestamp(self) -> None:
        tracker = TrendTracker()
        report = {"generated_at": "2025-06-01T14:00:00"}
        assert tracker._generated_at_key(report) == "2025-06-01T14:00:00"

    def test_returns_empty_string(self) -> None:
        tracker = TrendTracker()
        assert tracker._generated_at_key({}) == ""


class TestPrintTrendSummary:
    def test_no_reports_logs_info(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        tracker = TrendTracker(report_dir=tmp_path)
        with caplog.at_level("INFO"):
            tracker.print_trend_summary(days=14)
        assert "No quality reports found" in caplog.text

    def test_with_reports_prints_summary(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        for i in range(3):
            date_str = _date_str(2 - i)
            report = _make_report(
                date_str,
                {
                    "completed_count": i * 10,
                    "relay_integrity": {"recent_missing_count": i, "current_season_missing_count": i},
                    "standings_integrity": {"ok": True},
                    "pa_formula_integrity": {"violation_count": i},
                },
            )
            report["quality_gate"] = {"ok": True}
            _write_report(tmp_path, report)
        tracker = TrendTracker(report_dir=tmp_path)
        with caplog.at_level("INFO"):
            tracker.print_trend_summary(days=7)
        assert "Quality Metric Trends" in caplog.text

    def test_degradations_logged_when_present(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        r1 = _make_report(_date_str(10), {"relay_integrity": {"recent_missing_count": 10}})
        r2 = _make_report(_date_str(0), {"relay_integrity": {"recent_missing_count": 100}})
        _write_report(tmp_path, r1)
        _write_report(tmp_path, r2)
        tracker = TrendTracker(report_dir=tmp_path)
        with caplog.at_level("INFO"):
            tracker.print_trend_summary(days=30)
        assert "Degradations detected" in caplog.text


class TestSendDegradationAlert:
    def test_no_degradations_no_alert(self, tmp_path: Path) -> None:
        tracker = TrendTracker(report_dir=tmp_path)
        with patch("src.monitoring.trend_tracker.SlackWebhookClient") as mock_client:
            tracker.send_degradation_alert(days=14)
            mock_client.send_alert.assert_not_called()

    def test_sends_alert_when_degraded(self, tmp_path: Path) -> None:
        r1 = _make_report(_date_str(10), {"relay_integrity": {"recent_missing_count": 10}})
        r2 = _make_report(_date_str(0), {"relay_integrity": {"recent_missing_count": 100}})
        _write_report(tmp_path, r1)
        _write_report(tmp_path, r2)
        tracker = TrendTracker(report_dir=tmp_path)
        with patch("src.monitoring.trend_tracker.SlackWebhookClient") as mock_client:
            mock_client.send_alert = MagicMock(return_value=True)
            tracker.send_degradation_alert(days=30)
            mock_client.send_alert.assert_called_once()
            call_args = mock_client.send_alert.call_args[0]
            assert "열화" in call_args[0] or "degradation" in call_args[0].lower()

    def test_pa_violation_triggers_alert(self, tmp_path: Path) -> None:
        r1 = _make_report(_date_str(10), {"relay_integrity": {"recent_missing_count": 10}})
        r2 = _make_report(_date_str(0), {"relay_integrity": {"recent_missing_count": 100}})
        _write_report(tmp_path, r1)
        _write_report(tmp_path, r2)
        tracker = TrendTracker(report_dir=tmp_path)
        with patch("src.monitoring.trend_tracker.SlackWebhookClient") as mock_client:
            mock_client.send_alert = MagicMock(return_value=True)
            tracker.send_degradation_alert(days=30)
            mock_client.send_alert.assert_called_once()
