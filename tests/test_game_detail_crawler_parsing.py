"""Unit tests for GameDetailCrawler parsing methods."""
from __future__ import annotations

import pytest
from src.crawlers.game_detail_crawler import GameDetailCrawler


@pytest.fixture
def crawler():
    return GameDetailCrawler()


class TestParseInningsToOuts:
    @pytest.mark.parametrize(
        ("inp", "expected"),
        [
            (None, None),
            ("-", 0),
            ("0", 0),
            ("5", 15),
            ("5 1/3", 16),
            ("5 2/3", 17),
            ("1/3", 1),
            ("2/3", 2),
            ("5.1", 16),
            ("5.2", 17),
            ("0.2", 2),
            ("1.0", 3),
            ("9", 27),
            ("9 0/3", 27),
        ],
    )
    def test_standard_formats(self, crawler, inp, expected):
        assert crawler._parse_innings_to_outs(inp) == expected

    @pytest.mark.parametrize(
        ("inp", "expected"),
        [
            ("5⅓", 16),
            ("5⅔", 17),
            ("⅓", 1),
            ("⅔", 2),
            ("5 ⅓", 16),
            ("5 ⅔", 17),
            (" 5⅓ ", 16),
        ],
    )
    def test_unicode_fractions(self, crawler, inp, expected):
        assert crawler._parse_innings_to_outs(inp) == expected

    def test_invalid_returns_none(self, crawler):
        assert crawler._parse_innings_to_outs("abc") is None
        assert crawler._parse_innings_to_outs("삼진") is None
        assert crawler._parse_innings_to_outs("X") is None

    def test_white_space_handling(self, crawler):
        assert crawler._parse_innings_to_outs("  5  ") == 15
        assert crawler._parse_innings_to_outs("  5 1/3  ") == 16
        assert crawler._parse_innings_to_outs("  5⅓  ") == 16


class TestParseDecision:
    @pytest.mark.parametrize(
        ("inp", "expected"),
        [
            (None, None),
            ("", None),
            ("  ", None),
            ("승", "W"),
            ("패", "L"),
            ("세", "S"),
            ("홀드", "H"),
            ("H", "H"),
            ("  승  ", "W"),
        ],
    )
    def test_valid_decisions(self, crawler, inp, expected):
        assert crawler._parse_decision(inp) == expected

    @pytest.mark.parametrize(
        "inp",
        [
            "기타",
            "W",
            "L",
            "S",
        ],
    )
    def test_invalid_returns_none(self, crawler, inp):
        assert crawler._parse_decision(inp) is None


class TestParseScoreboardRow:
    def test_empty_row(self, crawler):
        result = crawler._parse_scoreboard_row(["TEAM", "1", "2", "3", "R", "H", "E"], [])
        assert result["name"] is None
        assert result["line_score"] == []
        assert result["score"] is None

    def test_standard_row(self, crawler):
        headers = ["TEAM", "1", "2", "3", "4", "5", "6", "7", "8", "9", "R", "H", "E"]
        row = ["LG", "0", "1", "0", "2", "0", "0", "0", "0", "0", "3", "8", "1"]
        result = crawler._parse_scoreboard_row(headers, row, 2025)
        assert result["name"] == "LG"
        assert result["line_score"] == [0, 1, 0, 2, 0, 0, 0, 0, 0]
        assert result["score"] == 3
        assert result["hits"] == 8
        assert result["errors"] == 1

    def test_extra_inning_row(self, crawler):
        headers = ["TEAM", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "R", "H", "E"]
        row = ["SSG", "0", "0", "0", "1", "0", "0", "0", "0", "0", "1", "0", "2", "10", "0"]
        result = crawler._parse_scoreboard_row(headers, row, 2025)
        assert result["name"] == "SSG"
        assert result["line_score"] == [0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0]
        assert result["score"] == 2
        assert result["hits"] == 10
        assert result["errors"] == 0

    def test_name_cleanup(self, crawler):
        headers = ["TEAM", "1", "2", "R", "H", "E"]
        row = ["LG승", "1", "0", "1", "5", "0"]
        result = crawler._parse_scoreboard_row(headers, row, 2025)
        assert result["name"] == "LG"

    def test_short_row(self, crawler):
        headers = ["TEAM", "R", "H", "E"]
        row = ["LG", "3", "8", "1"]
        result = crawler._parse_scoreboard_row(headers, row, 2025)
        assert result["name"] == "LG"
        assert result["line_score"] == []
        assert result["score"] == 3
        assert result["hits"] == 8
        assert result["errors"] == 1


class TestSafeInt:
    @pytest.mark.parametrize(
        ("inp", "expected"),
        [
            (None, None),
            ("", None),
            ("-", None),
            ("null", None),
            ("0", 0),
            ("5", 5),
            ("10", 10),
            ("1,000", 1000),
            (5, 5),
            (0, 0),
            ("abc", None),
        ],
    )
    def test_safe_int(self, crawler, inp, expected):
        assert crawler._safe_int(inp) == expected
