"""Tests for gap_report and dashboard_report pure functions."""

from __future__ import annotations

import pytest

from src.cli.gap_report import _gap_severity


class TestGapReportLogic:
    def test_gap_severity_returns_string(self):
        result = _gap_severity({"ok": True, "total": 0})
        assert isinstance(result, str)

    def test_gap_severity_with_errors(self):
        result = _gap_severity({"ok": False, "error": "test"})
        assert isinstance(result, str)

    def test_gap_severity_with_gaps(self):
        result = _gap_severity({"ok": False, "total": 5, "gaps": []})
        assert isinstance(result, str)
