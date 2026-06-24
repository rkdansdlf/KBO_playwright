"""Tests for trend_tracker — quality metric trend detection."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

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
