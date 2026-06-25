from __future__ import annotations

import os
from datetime import date, datetime
from zoneinfo import ZoneInfo

from src.services.p0_readiness import (
    _env_enabled,
    _safe_rows,
    normalize_yyyymmdd,
)


class TestEnvEnabled:
    def test_default_enabled(self, monkeypatch):
        monkeypatch.delenv("TEST_VAR", raising=False)
        assert _env_enabled("TEST_VAR") is True

    def test_explicit_enabled(self, monkeypatch):
        monkeypatch.setenv("TEST_VAR", "1")
        assert _env_enabled("TEST_VAR") is True

    def test_disabled_zero(self, monkeypatch):
        monkeypatch.setenv("TEST_VAR", "0")
        assert _env_enabled("TEST_VAR") is False

    def test_disabled_false(self, monkeypatch):
        monkeypatch.setenv("TEST_VAR", "false")
        assert _env_enabled("TEST_VAR") is False

    def test_disabled_no(self, monkeypatch):
        monkeypatch.setenv("TEST_VAR", "no")
        assert _env_enabled("TEST_VAR") is False

    def test_disabled_off(self, monkeypatch):
        monkeypatch.setenv("TEST_VAR", "off")
        assert _env_enabled("TEST_VAR") is False

    def test_default_param(self, monkeypatch):
        monkeypatch.delenv("TEST_VAR", raising=False)
        assert _env_enabled("TEST_VAR", default="0") is False


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
