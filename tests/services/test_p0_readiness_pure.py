from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

from src.services.p0_readiness import (
    _safe_rows,
    normalize_yyyymmdd,
)


class TestNormalizeYyyymmdd:
    def test_none_returns_today(self):
        result = normalize_yyyymmdd(None)
        assert len(result) == 8
        assert result.isdigit()

    def test_date_input(self):
        result = normalize_yyyymmdd(date(2026, 6, 25))
        assert result == "20260625"

    def test_datetime_input(self):
        result = normalize_yyyymmdd(datetime(2026, 6, 25, 14, 30))
        assert result == "20260625"

    def test_string_input_passes_through(self):
        result = normalize_yyyymmdd("20260625")
        assert result == "20260625"


class TestSafeRows:
    def test_exception_returns_empty(self):
        class BadQuery:
            def all(self):
                raise RuntimeError("DB error")

        result = _safe_rows(BadQuery())
        assert result == []
