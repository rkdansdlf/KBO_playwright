"""Unit tests for backfill_starting_pitchers_from_stats pure functions."""

from __future__ import annotations

import pytest

from src.cli.backfill_starting_pitchers_from_stats import (
    _is_blank,
    _normalize_date,
)


class TestNormalizeDate:
    def test_valid_date(self) -> None:
        assert _normalize_date("2025-06-15") == "2025-06-15"

    def test_none(self) -> None:
        assert _normalize_date(None) is None

    def test_empty_string(self) -> None:
        assert _normalize_date("") is None

    def test_whitespace(self) -> None:
        assert _normalize_date("   ") == ""


class TestIsBlank:
    def test_none(self) -> None:
        assert _is_blank(None) is True

    def test_empty_string(self) -> None:
        assert _is_blank("") is True

    def test_whitespace(self) -> None:
        assert _is_blank("   ") is True

    def test_non_blank(self) -> None:
        assert _is_blank("hello") is False

    def test_number(self) -> None:
        assert _is_blank(42) is False
