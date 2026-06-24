from __future__ import annotations

from unittest.mock import MagicMock, patch
from src.cli.dashboard_report import _date_or_today, AVAILABLE_SECTIONS


class TestDateOrToday:
    def test_with_date(self):
        assert _date_or_today("20260624") == "20260624"

    def test_none_returns_today(self):
        result = _date_or_today(None)
        assert len(result) == 8
        assert result.isdigit()


class TestAvailableSections:
    def test_sections_defined(self):
        assert "standings" in AVAILABLE_SECTIONS
        assert "park_factor" in AVAILABLE_SECTIONS
        assert "quality" in AVAILABLE_SECTIONS
        assert "all" in AVAILABLE_SECTIONS
