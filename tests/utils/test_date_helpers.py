"""Tests for src.utils.date_helpers."""

from __future__ import annotations

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
