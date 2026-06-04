"""Tests for the unified gap report (src/cli/gap_report.py)."""

from __future__ import annotations

from datetime import datetime

from src.cli.gap_report import (
    _gap_severity,
    build_gap_report,
    check_id_resolution_gaps,
    check_profile_gaps,
    check_relay_gaps,
    format_report_summary,
)
from src.utils.alerting import GAP_EMOJI_MAP


# ── _gap_severity ────────────────────────────────────────────────────────────────────


def test_gap_severity_ok():
    assert _gap_severity({"ok": True}) == "ok"


def test_gap_severity_warning():
    assert _gap_severity({"ok": False}) == "warning"


def test_gap_severity_error():
    assert _gap_severity({"ok": False, "error": "DB down"}) == "error"


# ── format_report_summary ────────────────────────────────────────────────────────────


def test_format_report_summary_all_ok():
    report = {
        "gaps": {
            cat: {"ok": True}
            for cat in ["FRESHNESS", "RELAY", "STALENESS", "STANDINGS", "PROFILE", "ID_RESOLUTION"]
        }
    }
    result = format_report_summary(report)
    # Should contain green checkmark for each category
    assert "✅" in result
    assert "FRESHNESS" in result
    assert "RELAY" in result


def test_format_report_summary_mixed():
    report = {
        "gaps": {
            "FRESHNESS": {"ok": False},
            "RELAY": {"ok": True},
            "STALENESS": {"ok": True, "error": "timeout"},
        }
    }
    result = format_report_summary(report)
    assert "⚠️" in result  # warning for FRESHNESS
    assert "❌" in result  # error for STALENESS
    assert "✅" in result  # ok for RELAY


def test_format_report_summary_unknown_category():
    report = {"gaps": {"MYSTERY": {"ok": False, "missing_count": 3}}}
    result = format_report_summary(report)
    assert "MYSTERY" in result


# ── build_gap_report structure ───────────────────────────────────────────────────────


def test_build_gap_report_structure():
    report = build_gap_report()
    assert "generated_at" in report
    assert "gaps" in report
    for cat in ["FRESHNESS", "RELAY", "STALENESS", "STANDINGS", "PROFILE", "ID_RESOLUTION"]:
        assert cat in report["gaps"], f"Missing gap category: {cat}"
        assert "ok" in report["gaps"][cat], f"Missing 'ok' key in {cat}"


# ── check_relay_gaps (with monkeypatch) ──────────────────────────────────────────────


def _make_fake_session(execute_return=None, query_return=None):
    """Build a fake session class that returns the given values."""
    class FakeScalars:
        def __init__(self, values=None):
            self._values = values or []
        def all(self):
            return self._values

    class FakeResult:
        def __init__(self, scalar_val=0):
            self._scalar_val = scalar_val
        def scalars(self):
            return FakeScalars()
        def scalar(self):
            return self._scalar_val

    class FakeQuery:
        def __init__(self):
            self._rows = []
        def filter(self, *a, **kw):
            return self
        def all(self):
            return self._rows

    class FakeSession:
        def __init__(self, *a, **kw):
            self.execute_return = execute_return or FakeResult()
            self.query_return = query_return
        def execute(self, stmt, params=None):
            return self.execute_return
        def query(self, model):
            return self.query_return or FakeQuery()
        def close(self):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *a):
            pass

    return FakeSession


def test_check_relay_gaps_happy_path(monkeypatch):
    """Simulate empty DB result — no relay gaps."""
    import src.cli.gap_report as gap_report_module
    monkeypatch.setattr(
        gap_report_module, "SessionLocal",
        _make_fake_session(),
    )
    result = check_relay_gaps()
    assert result["ok"] is True
    assert result["missing_count"] == 0


# ── check_profile_gaps (with monkeypatch) ────────────────────────────────────────────


def test_check_profile_gaps_no_gaps(monkeypatch):
    import src.cli.gap_report as gap_report_module
    monkeypatch.setattr(
        gap_report_module, "SessionLocal",
        _make_fake_session(),
    )
    result = check_profile_gaps()
    assert result["ok"] is True
    assert result["missing_count"] == 0


# ── check_id_resolution_gaps (with monkeypatch) ──────────────────────────────────────


def test_check_id_resolution_gaps_no_gaps(monkeypatch):
    import src.cli.gap_report as gap_report_module
    monkeypatch.setattr(
        gap_report_module, "SessionLocal",
        _make_fake_session(),
    )
    result = check_id_resolution_gaps()
    assert result["ok"] is True
    assert result["total"] == 0
