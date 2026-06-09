
from src.crawlers.team_batting_stats_crawler import _build_column_map


class TestBuildColumnMap:
    def test_korean_headers(self):
        headers = ["팀명", "경기", "승", "패", "타율"]
        result = _build_column_map(headers)
        assert result["team_name"] == 0
        assert result["games"] == 1

    def test_english_headers(self):
        headers = ["팀", "g", "w", "l", "avg"]
        result = _build_column_map(headers)
        assert result["team_name"] == 0
        assert "games" in result

    def test_empty_headers_fallback(self):
        result = _build_column_map([])
        assert result["team_name"] == 0

    def test_fallback_team_name_position(self):
        headers = ["순위", "unknown1", "unknown2"]
        result = _build_column_map(headers)
        assert result["team_name"] == 1
