from __future__ import annotations

from pathlib import Path

from src.cli.quality_dashboard import (
    _is_yyyymmdd,
    _quality_failures,
    _record_from_report,
    _section_ok,
)


class TestSectionOk:
    def test_empty_report(self):
        assert _section_ok({}, "quality_gate", "ok")

    def test_non_dict_current(self):
        assert _section_ok({"quality_gate": 42}, "quality_gate", "ok")

    def test_ok_true(self):
        report = {"quality_gate": {"ok": True}}
        assert _section_ok(report, "quality_gate", "ok")

    def test_ok_false(self):
        report = {"quality_gate": {"ok": False}}
        assert not _section_ok(report, "quality_gate", "ok")

    def test_ok_missing_key_default_true(self):
        report = {"quality_gate": {}}
        assert _section_ok(report, "quality_gate", "ok")

    def test_ok_non_bool_value_fallback(self):
        report = {"quality_gate": {"ok": "yes"}}
        assert _section_ok(report, "quality_gate", "ok")

    def test_single_key_path(self):
        report = {"ok": True}
        assert _section_ok(report, "ok")

    def test_deep_nested_path(self):
        report = {"a": {"b": {"c": {"ok": False}}}}
        assert not _section_ok(report, "a", "b", "c", "ok")

    def test_deep_nested_path_missing_ok(self):
        report = {"a": {"b": {"c": {}}}}
        assert _section_ok(report, "a", "b", "c", "ok")

    def test_deep_nested_path_non_dict_intermediate(self):
        report = {"a": {"b": None}}
        assert _section_ok(report, "a", "b", "c")


class TestQualityFailures:
    def test_all_ok(self):
        report = {
            "quality_gate": {"ok": True},
            "metrics": {
                "relay_integrity": {"ok": True},
                "standings_integrity": {"ok": True},
                "parity": {"ok": True},
                "pa_formula_integrity": {"ok": True},
            },
        }
        assert _quality_failures(report) == []

    def test_quality_gate_fails(self):
        report = {
            "quality_gate": {"ok": False},
            "metrics": {
                "relay_integrity": {"ok": True},
                "standings_integrity": {"ok": True},
                "parity": {"ok": True},
                "pa_formula_integrity": {"ok": True},
            },
        }
        assert _quality_failures(report) == ["quality_gate"]

    def test_all_fail(self):
        report = {
            "quality_gate": {"ok": False},
            "metrics": {
                "relay_integrity": {"ok": False},
                "standings_integrity": {"ok": False},
                "parity": {"ok": False},
                "pa_formula_integrity": {"ok": False},
            },
        }
        failures = _quality_failures(report)
        assert failures == ["quality_gate", "relay_integrity", "standings_integrity", "parity", "pa_formula"]

    def test_missing_metrics_default_ok(self):
        report = {"quality_gate": {"ok": True}}
        assert _quality_failures(report) == []

    def test_partial_failures(self):
        report = {
            "quality_gate": {"ok": True},
            "metrics": {
                "relay_integrity": {"ok": False},
                "standings_integrity": {"ok": True},
                "parity": {"ok": True},
                "pa_formula_integrity": {"ok": False},
            },
        }
        failures = _quality_failures(report)
        assert failures == ["relay_integrity", "pa_formula"]


class TestIsYyyymmdd:
    def test_valid(self):
        assert _is_yyyymmdd("20250630")

    def test_short(self):
        assert not _is_yyyymmdd("2025063")

    def test_long(self):
        assert not _is_yyyymmdd("202506301")

    def test_non_digit(self):
        assert not _is_yyyymmdd("2025-06-30")

    def test_invalid_date(self):
        assert not _is_yyyymmdd("20251301")

    def test_empty(self):
        assert not _is_yyyymmdd("")

    def test_leap_year(self):
        assert _is_yyyymmdd("20240229")

    def test_non_leap_year(self):
        assert not _is_yyyymmdd("20230229")


class TestRecordFromReport:
    def test_basic_record(self):
        report = {
            "generated_at": "2025-06-30T12:00:00",
            "quality_gate": {"ok": True},
            "metrics": {
                "date": "20250629",
                "total_games": 5,
                "completed_count": 3,
                "status_counts": {"FINAL": 3, "CANCELED": 2},
                "relay_integrity": {"ok": True},
                "standings_integrity": {"ok": True},
                "parity": {"ok": True},
                "pa_formula_integrity": {"ok": True},
            },
        }
        record = _record_from_report(Path("report.json"), report)
        assert record["file"] == "report.json"
        assert record["date"] == "20250629"
        assert record["generated_at"] == "2025-06-30T12:00:00"
        assert record["total_games"] == 5
        assert record["completed_count"] == 3
        assert record["status_counts"] == {"FINAL": 3, "CANCELED": 2}
        assert record["quality_gate_ok"] is True
        assert record["relay_ok"] is True
        assert record["standings_ok"] is True
        assert record["parity_ok"] is True
        assert record["pa_formula_ok"] is True
        assert record["failures"] == []

    def test_record_with_failures(self):
        report = {
            "quality_gate": {"ok": False},
            "metrics": {
                "relay_integrity": {"ok": False},
                "pa_formula_integrity": {"ok": False},
            },
        }
        record = _record_from_report(Path("fail.json"), report)
        assert record["quality_gate_ok"] is False
        assert record["relay_ok"] is False
        assert record["pa_formula_ok"] is False
        assert record["failures"] == ["quality_gate", "relay_integrity", "pa_formula"]

    def test_date_fallback_from_filename(self):
        report = {"quality_gate": {"ok": True}, "metrics": {}}
        record = _record_from_report(Path("20250630.json"), report)
        assert record["date"] == "20250630"

    def test_missing_metrics_defaults(self):
        report = {"quality_gate": {"ok": True}}
        record = _record_from_report(Path("report.json"), report)
        assert record["total_games"] == 0
        assert record["completed_count"] == 0
        assert record["status_counts"] == {}
