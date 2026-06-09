import pytest

from src.crawlers.team_pitching_stats_crawler import _build_column_map, _parse_innings


class TestBuildColumnMap:
    def test_korean_headers(self):
        headers = ["팀명", "경기", "승", "패", "방어율"]
        result = _build_column_map(headers)
        assert result["team_name"] == 0
        assert "games" in result
        assert "era" in result

    def test_english_headers(self):
        headers = ["팀", "g", "w", "l", "era"]
        result = _build_column_map(headers)
        assert result["team_name"] == 0

    def test_empty_headers_fallback(self):
        result = _build_column_map([])
        assert result["team_name"] == 0

    def test_missing_team_name_fallback(self):
        headers = ["a", "b", "c"]
        result = _build_column_map(headers)
        assert result["team_name"] == 1


class TestParseInnings:
    def test_whole_number(self):
        assert _parse_innings("9") == 9.0

    def test_decimal_innings(self):
        result = _parse_innings("6.1")
        assert result == pytest.approx(6.333, rel=0.01)

    def test_float_string(self):
        result = _parse_innings("0.2")
        assert result == pytest.approx(0.666, rel=0.01)

    def test_empty_returns_none(self):
        assert _parse_innings("") is None
        assert _parse_innings("-") is None
