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

    def test_accepts_iso_form(self) -> None:
        assert date_helpers.parse_date_str_lenient("2025-06-29") == date(2025, 6, 29)

    @pytest.mark.parametrize("raw", ["2025/06/29", "2025.06.29", "  20250629  "])
    def test_accepts_alternate_separators(self, raw: str) -> None:
        assert date_helpers.parse_date_str_lenient(raw) == date(2025, 6, 29)

    def test_raises_on_malformed_with_raise_sentinel(self) -> None:
        """Callers that must reject bad input opt in via the sentinel."""
        with pytest.raises(ValueError, match="cannot parse"):
            date_helpers.parse_date_str_lenient("not-a-date", fallback=date_helpers.RAISE_ON_UNPARSABLE)

    def test_sentinel_is_not_silently_treated_as_a_date(self) -> None:
        """A guard against the sentinel ever leaking into a returned value."""
        with pytest.raises(ValueError, match="cannot parse"):
            date_helpers.parse_date_str_lenient(None, fallback=date_helpers.RAISE_ON_UNPARSABLE)  # type: ignore[arg-type]

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
        # `None` is not a `str`, so the fallback contract still has to hold
        # rather than leaking a TypeError out of the job that called it.
        result = date_helpers.parse_date_str_lenient(None, fallback=date(2020, 1, 31))  # type: ignore[arg-type]
        assert result == date(2020, 1, 31)

    def test_none_with_raise_sentinel_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="cannot parse"):
            date_helpers.parse_date_str_lenient(None, fallback=date_helpers.RAISE_ON_UNPARSABLE)  # type: ignore[arg-type]

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
