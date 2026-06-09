import pytest

from src.crawlers.player_pitching_all_series_crawler import (
    extract_player_id,
    normalize_header,
    parse_innings,
    safe_float,
    safe_int,
)


class TestNormalizeHeader:
    def test_simple_text(self):
        assert normalize_header("ERA") == "ERA"

    def test_strips_non_breaking_space(self):
        assert normalize_header("WHIP\xa0") == "WHIP"

    def test_takes_first_part(self):
        assert normalize_header("G        (경기)") == "G"

    def test_none_returns_empty(self):
        assert normalize_header(None) == ""


class TestSafeInt:
    def test_normal_int(self):
        assert safe_int("42") == 42

    def test_dash_returns_none(self):
        assert safe_int("-") is None

    def test_en_dash_returns_none(self):
        assert safe_int("–") is None

    def test_none_returns_none(self):
        assert safe_int(None) is None

    def test_float_string(self):
        assert safe_int("3.14") == 3


class TestSafeFloat:
    def test_normal_float(self):
        assert safe_float("3.14") == 3.14

    def test_dash_returns_none(self):
        assert safe_float("-") is None

    def test_none_returns_none(self):
        assert safe_float(None) is None


class TestParseInnings:
    def test_whole_innings(self):
        result, outs = parse_innings("9")
        assert result == 9.0
        assert outs == 27

    def test_innings_with_fraction(self):
        result, outs = parse_innings("6 2/3")
        assert result == pytest.approx(6.67, rel=0.01)
        assert outs == 20

    def test_innings_with_decimal(self):
        result, outs = parse_innings("7.1")
        assert result == 7.1
        assert outs == 22

    def test_empty_returns_none(self):
        assert parse_innings("") == (None, None)
        assert parse_innings(None) == (None, None)

    def test_dash_returns_none(self):
        assert parse_innings("-") == (None, None)


class TestExtractPlayerId:
    def test_extracts_id(self):
        assert extract_player_id("playerId=54321") == 54321

    def test_none_returns_none(self):
        assert extract_player_id(None) is None

    def test_no_match_returns_none(self):
        assert extract_player_id("no-match") is None
