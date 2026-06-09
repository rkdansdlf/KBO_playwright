
from src.parsers.futures_stats_parser import (
    _classify_tables,
    parse_futures_tables,
)


class TestClassifyTables:
    def test_classify_by_keyword_hitter(self):
        tables = [
            {"headers": ["연도", "팀명", "타수", "안타", "AVG"]},
            {"headers": ["연도", "팀명", "ERA", "승", "패"]},
        ]
        hitter, pitcher = _classify_tables(tables)
        assert len(hitter) == 1
        assert len(pitcher) == 1

    def test_classify_by_table_type_marker(self):
        tables = [
            {"_table_type": "HITTER", "headers": ["연도", "팀명"]},
            {"_table_type": "PITCHER", "headers": ["연도", "팀명"]},
        ]
        hitter, pitcher = _classify_tables(tables)
        assert len(hitter) == 1
        assert len(pitcher) == 1

    def test_classify_by_caption_hitter_hint(self):
        tables = [
            {"headers": ["연도"], "caption": "타격 기록"},
            {"headers": ["연도"], "caption": "투수 기록"},
        ]
        hitter, pitcher = _classify_tables(tables)
        assert len(hitter) == 1
        assert len(pitcher) == 1

    def test_classify_empty_tables(self):
        hitter, pitcher = _classify_tables([])
        assert hitter == []
        assert pitcher == []

    def test_classify_unknown_tables_go_nowhere(self):
        tables = [
            {"headers": ["기타", "정보"]},
        ]
        hitter, pitcher = _classify_tables(tables)
        assert hitter == []
        assert pitcher == []

    def test_classify_by_summary_hitter_hint(self):
        tables = [
            {"headers": ["연도"], "summary": "batting stats"},
        ]
        hitter, pitcher = _classify_tables(tables)
        assert len(hitter) == 1

    def test_classify_by_summary_pitcher_hint(self):
        tables = [
            {"headers": ["연도"], "summary": "pitching stats"},
        ]
        hitter, pitcher = _classify_tables(tables)
        assert len(pitcher) == 1

    def test_classify_handles_mixed_keywords(self):
        tables = [
            {"headers": ["연도", "타수", "ERA"]},
        ]
        hitter, pitcher = _classify_tables(tables)
        assert len(hitter) == 1
        assert len(pitcher) == 0


class TestParseFuturesTables:
    def test_parse_futures_batting_only(self):
        tables = [
            {
                "headers": ["연도", "팀명", "경기", "타수", "안타", "타율"],
                "rows": [
                    ["2024", "LG", "50", "150", "45", "0.300"],
                ],
            },
        ]
        result = parse_futures_tables(tables)
        assert "batting" in result
        assert "pitching" in result
        assert len(result["batting"]) == 1
        assert result["batting"][0]["season"] == 2024

    def test_parse_futures_pitching_only(self):
        tables = [
            {
                "headers": ["연도", "팀명", "경기", "승", "패", "ERA", "이닝"],
                "rows": [
                    ["2024", "LG", "20", "5", "3", "3.50", "40.0"],
                ],
            },
        ]
        result = parse_futures_tables(tables)
        assert len(result["pitching"]) == 1
        assert result["pitching"][0]["season"] == 2024

    def test_parse_futures_both(self):
        tables = [
            {
                "headers": ["연도", "팀명", "경기", "타수", "안타", "타율"],
                "rows": [["2024", "LG", "50", "150", "45", "0.300"]],
            },
            {
                "headers": ["연도", "팀명", "경기", "승", "패", "ERA", "이닝"],
                "rows": [["2024", "LG", "20", "5", "3", "3.50", "40.0"]],
            },
        ]
        result = parse_futures_tables(tables)
        assert len(result["batting"]) == 1
        assert len(result["pitching"]) == 1

    def test_parse_futures_empty_tables(self):
        result = parse_futures_tables([])
        assert result == {"batting": [], "pitching": []}

    def test_parse_futures_league_and_level(self):
        tables = [
            {
                "headers": ["연도", "팀명", "경기", "타수", "안타", "타율"],
                "rows": [["2024", "LG", "50", "150", "45", "0.300"]],
            },
        ]
        result = parse_futures_tables(tables)
        assert result["batting"][0]["league"] == "FUTURES"
        assert result["batting"][0]["level"] == "KBO2"
