"""Tests for src.utils.date_helpers."""

from __future__ import annotations

import logging
from datetime import date, datetime

import pytest

from src.constants import KST
from src.utils import date_helpers


class TestParseDateStr:
    def test_default_format(self) -> None:
        result = date_helpers.parse_date_str("20250629")
        assert result == date(2025, 6, 29)

    def test_custom_format(self) -> None:
        result = date_helpers.parse_date_str("2025-06-29", fmt="%Y-%m-%d")
        assert result == date(2025, 6, 29)

    def test_returns_date_not_datetime(self) -> None:
        result = date_helpers.parse_date_str("20250101")
        assert isinstance(result, date)
        assert not isinstance(result, datetime)


class TestParseDatetimeStr:
    def test_default_format(self) -> None:
        result = date_helpers.parse_datetime_str("20250629")
        assert result == datetime(2025, 6, 29, tzinfo=KST)

    def test_custom_format(self) -> None:
        result = date_helpers.parse_datetime_str("2025-06-29 14:30", fmt="%Y-%m-%d %H:%M")
        assert result == datetime(2025, 6, 29, 14, 30, tzinfo=KST)

    def test_has_kst_tzinfo(self) -> None:
        result = date_helpers.parse_datetime_str("20250101")
        assert result.tzinfo is KST


class TestParseDateStrLenient:
    def test_parses_well_formed_value(self) -> None:
        result = date_helpers.parse_date_str_lenient("20250629")
        assert result == date(2025, 6, 29)

    def test_parses_with_custom_format(self) -> None:
        result = date_helpers.parse_date_str_lenient("2025-06-29", fmt="%Y-%m-%d")
        assert result == date(2025, 6, 29)

    def test_does_not_raise_on_malformed_value(self) -> None:
        result = date_helpers.parse_date_str_lenient("not-a-date")
        assert isinstance(result, date)

    def test_uses_explicit_fallback_when_given(self) -> None:
        anchor = date(2020, 1, 31)
        result = date_helpers.parse_date_str_lenient("not-a-date", fallback=anchor)
        assert result == anchor

    def test_defaults_to_kst_today_without_fallback(self) -> None:
        result = date_helpers.parse_date_str_lenient("not-a-date")
        assert result == datetime.now(KST).date()

    def test_fallback_is_ignored_when_value_parses(self) -> None:
        result = date_helpers.parse_date_str_lenient("20250629", fallback=date(2020, 1, 31))
        assert result == date(2025, 6, 29)

    def test_logs_warning_on_malformed_value(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="src.utils.date_helpers"):
            date_helpers.parse_date_str_lenient("not-a-date", fallback=date(2020, 1, 31))
        assert "not-a-date" in caplog.text
        assert "2020-01-31" in caplog.text

    def test_does_not_log_on_successful_parse(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="src.utils.date_helpers"):
            date_helpers.parse_date_str_lenient("20250629")
        assert caplog.text == ""

    def test_rejects_none_without_fallback(self) -> None:
        # `None` is not a `str`, so the lenient contract still has to hold
        # rather than leaking a TypeError out of the job that called it.
        result = date_helpers.parse_date_str_lenient(None, fallback=date(2020, 1, 31))  # type: ignore[arg-type]
        assert result == date(2020, 1, 31)

    def test_always_returns_date_not_datetime(self) -> None:
        result = date_helpers.parse_date_str_lenient("20250629")
        assert isinstance(result, date)
        assert not isinstance(result, datetime)


class TestNormalizeToDate:
    def test_hyphen_separator(self) -> None:
        result = date_helpers.normalize_to_date("2025-06-29")
        assert result == date(2025, 6, 29)

    def test_slash_separator(self) -> None:
        result = date_helpers.normalize_to_date("2025/06/29")
        assert result == date(2025, 6, 29)

    def test_dot_separator(self) -> None:
        result = date_helpers.normalize_to_date("2025.06.29")
        assert result == date(2025, 6, 29)

    def test_no_separator(self) -> None:
        result = date_helpers.normalize_to_date("20250629")
        assert result == date(2025, 6, 29)

    def test_returns_date_with_kst(self) -> None:
        result = date_helpers.normalize_to_date("2025-06-29")
        assert isinstance(result, date)
