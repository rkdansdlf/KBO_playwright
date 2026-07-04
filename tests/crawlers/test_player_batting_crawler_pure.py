"""Unit tests for player_batting_all_series_crawler pure functions."""

from __future__ import annotations

import pytest

from src.crawlers.player_batting_all_series_crawler import (
    _extract_player_id_from_href,
    _is_basic2_headers,
    safe_parse_number,
)


class TestSafeParseNumber:
    def test_int(self) -> None:
        assert safe_parse_number("42", int) == 42

    def test_float(self) -> None:
        result = safe_parse_number("3.14", float)
        assert result is not None
        assert abs(result - 3.14) < 0.01

    def test_none(self) -> None:
        assert safe_parse_number(None, int) is None

    def test_empty(self) -> None:
        assert safe_parse_number("", int) is None

    def test_dash(self) -> None:
        assert safe_parse_number("-", int) is None

    def test_na(self) -> None:
        assert safe_parse_number("N/A", int) is None

    def test_invalid(self) -> None:
        assert safe_parse_number("abc", int) is None

    def test_whitespace(self) -> None:
        assert safe_parse_number("  42  ", int) == 42


class TestExtractPlayerIdFromHref:
    def test_valid(self) -> None:
        assert _extract_player_id_from_href("/player.do?playerId=12345") == 12345

    def test_none(self) -> None:
        assert _extract_player_id_from_href(None) is None

    def test_empty(self) -> None:
        assert _extract_player_id_from_href("") is None

    def test_no_match(self) -> None:
        assert _extract_player_id_from_href("/player.do") is None


class TestIsBasic2Headers:
    def test_true_with_BB(self) -> None:
        assert _is_basic2_headers(["이름", "BB", "안타"]) is True

    def test_true_with_Korean(self) -> None:
        assert _is_basic2_headers(["이름", "볼넷", "안타"]) is True

    def test_false(self) -> None:
        assert _is_basic2_headers(["name", "avg", "hits"]) is False

    def test_empty(self) -> None:
        assert _is_basic2_headers([]) is False


class TestFuturesNormalizeHeader:
    def test_valid(self) -> None:
        from src.crawlers.player_pitching_all_series_crawler import normalize_header

        result = normalize_header("ERA")
        assert isinstance(result, str)

    def test_empty(self) -> None:
        from src.crawlers.player_pitching_all_series_crawler import normalize_header

        result = normalize_header("")
        assert result == ""
