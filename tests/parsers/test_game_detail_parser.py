"""Tests for game_detail_parser.py — pure functions and HTML parsing helpers.

Uses synthetic HTML tables (pandas-parseable) for scoreboard / hitter / pitcher data.
"""

import pandas as pd
from bs4 import BeautifulSoup

from src.parsers.game_detail_parser import (
    _build_hitter_payload,
    _build_pitcher_payload,
    _build_team_info,
    _extract_hitter_tables,
    _extract_pitcher_tables,
    _extract_scoreboard,
    _parse_decision,
    _parse_duration_minutes,
    _parse_metadata,
    _safe_player_id,
    _season_year_from_game,
    parse_game_detail_html,
)


class TestSeasonYearFromGame:
    def test_normal(self):
        assert _season_year_from_game("20251015") == 2025
        assert _season_year_from_game("2024-10-15") == 2024

    def test_empty(self):
        assert _season_year_from_game("") is None
        assert _season_year_from_game(None) is None

    def test_short_string(self):
        assert _season_year_from_game("123") is None



class TestParseDecision:
    def test_win(self):
        assert _parse_decision("승") == "W"

    def test_loss(self):
        assert _parse_decision("패") == "L"

    def test_save(self):
        assert _parse_decision("세") == "S"

    def test_hold(self):
        assert _parse_decision("홀드") == "H"
        assert _parse_decision("H") == "H"

    def test_none(self):
        assert _parse_decision(None) is None
        assert _parse_decision("") is None
        assert _parse_decision("기타") is None


class TestSafePlayerId:
    def test_valid(self):
        assert _safe_player_id(12345) == 12345
        assert _safe_player_id("12345") == 12345

    def test_none(self):
        assert _safe_player_id(None) is None

    def test_non_digit(self):
        assert _safe_player_id("abc") is None
        assert _safe_player_id("12a34") is None


class TestParseDurationMinutes:
    def test_valid(self):
        assert _parse_duration_minutes("2:30") == 150
        assert _parse_duration_minutes("0:45") == 45

    def test_invalid(self):
        assert _parse_duration_minutes(None) is None
        assert _parse_duration_minutes("") is None
        assert _parse_duration_minutes("abc") is None
        assert _parse_duration_minutes("2:30:00") is None


class TestExtractScoreboard:
    def test_valid_scoreboard(self):
        df = pd.DataFrame({"팀": ["LG", "SS"], "R": [5, 3], "H": [10, 8], "E": [0, 1], "1": [1, 0], "2": [2, 1]})
        result = _extract_scoreboard([pd.DataFrame(), df])
        assert result is not None

    def test_no_scoreboard(self):
        df = pd.DataFrame({"A": [1], "B": [2]})
        assert _extract_scoreboard([df]) is None

    def test_empty_list(self):
        assert _extract_scoreboard([]) is None


class TestExtractHitterTables:
    def test_valid_hitter_table(self):
        df = pd.DataFrame({"선수": ["A"], "타수": [3], "안타": [1]})
        assert len(_extract_hitter_tables([df])) == 1

    def test_no_hitter_table(self):
        df = pd.DataFrame({"선수": ["A"], "이닝": [3]})
        assert _extract_hitter_tables([df]) == []


class TestExtractPitcherTables:
    def test_valid_pitcher_table(self):
        df = pd.DataFrame({"선수": ["A"], "이닝": [3], "삼진": [5]})
        assert len(_extract_pitcher_tables([df])) == 1

    def test_no_pitcher_table(self):
        df = pd.DataFrame({"선수": ["A"], "타수": [3]})
        assert _extract_pitcher_tables([df]) == []


class TestBuildTeamInfo:
    def test_from_scoreboard(self):
        df = pd.DataFrame({"팀": ["LG", "SS"], "R": [5, 3], "H": [10, 8], "E": [0, 1]})
        teams = _build_team_info(df, "20250325LGSS0", 2025)
        assert teams["away"]["code"] is not None
        assert teams["home"]["code"] is not None

    def test_no_scoreboard(self):
        teams = _build_team_info(None, "20250325LGSS0", 2025)
        assert teams["away"]["code"] is not None
        assert teams["home"]["code"] is not None

    def test_line_score_extraction(self):
        df = pd.DataFrame({"팀": ["LG", "SS"], "R": [5, 3], "H": [10, 8], "E": [0, 1], "1": [1, 0], "2": [2, 1]})
        teams = _build_team_info(df, "20250325LGSS0", 2025)
        assert len(teams["away"]["line_score"]) >= 2


class TestBuildHitterPayload:
    def test_basic_hitters(self):
        df = pd.DataFrame({
            "선수": ["홍길동", "김철수"],
            "타석": [4, 3],
            "타수": [3, 2],
            "안타": [2, 1],
            "득점": [1, 0],
            "타율": [0.667, 0.500],
        })
        teams = {"away": {"code": "LG"}, "home": {"code": "SS"}}
        result = _build_hitter_payload([df], teams)
        assert len(result["away"]) == 2

    def test_team_summary_skipped(self):
        df = pd.DataFrame({
            "선수": ["팀합계", "홍길동"],
            "타수": [10, 3],
            "안타": [5, 2],
        })
        teams = {"away": {"code": "LG"}, "home": {"code": "SS"}}
        result = _build_hitter_payload([df], teams)
        assert len(result["away"]) == 1

    def test_empty_tables(self):
        teams = {"away": {"code": "LG"}, "home": {"code": "SS"}}
        assert _build_hitter_payload([], teams) == {"away": [], "home": []}


class TestBuildPitcherPayload:
    def test_basic_pitchers(self):
        df = pd.DataFrame({
            "선수": ["투수1"],
            "이닝": ["5.0"],
            "삼진": [5],
            "실점": [2],
            "자책": [2],
            "ERA": [3.60],
        })
        teams = {"away": {"code": "LG"}, "home": {"code": "SS"}}
        result = _build_pitcher_payload([df], teams)
        assert len(result["away"]) == 1
        assert result["away"][0]["stats"]["innings_outs"] == 15

    def test_first_pitcher_is_starter(self):
        df = pd.DataFrame({
            "선수": ["선발투수", "계투투수"],
            "이닝": ["5.0", "3.0"],
        })
        teams = {"away": {"code": "LG"}, "home": {"code": "SS"}}
        result = _build_pitcher_payload([df], teams)
        assert result["away"][0]["is_starting"] is True
        assert result["away"][1]["is_starting"] is False

    def test_decision_parsing(self):
        df = pd.DataFrame({
            "선수": ["투수1"],
            "이닝": ["3.0"],
            "결과": ["승"],
        })
        teams = {"away": {"code": "LG"}, "home": {"code": "SS"}}
        result = _build_pitcher_payload([df], teams)
        assert result["away"][0]["stats"]["decision"] == "W"

    def test_alternate_columns(self):
        df = pd.DataFrame({
            "선수명": ["P1"],
            "IP": ["5.0"],
            "삼진": [5],
        })
        teams = {"away": {"code": "LG"}, "home": {"code": "SS"}}
        result = _build_pitcher_payload([df], teams)
        assert len(result["away"]) == 1


class TestParseMetadata:
    def test_full_metadata(self):
        html = """
        <html><body>
        <div class="box-score-area">
        구장 : JAMSIL
        관중 : 25,000
        개시 : 14:00
        종료 : 16:30
        경기시간 : 2:30
        </div>
        </body></html>
        """
        meta = _parse_metadata(BeautifulSoup(html, "html.parser"))
        assert meta["stadium"] == "JAMSIL"
        assert meta["attendance"] == 25000
        assert meta["start_time"] == "14:00"
        assert meta["end_time"] == "16:30"
        assert meta["game_time"] == "2:30"
        assert meta["duration_minutes"] == 150

    def test_empty_html(self):
        meta = _parse_metadata(BeautifulSoup("", "html.parser"))
        assert meta["stadium"] is None
        assert meta["attendance"] is None


class TestParseGameDetailHtml:
    def test_integration_minimal_html(self):
        """Minimal end-to-end smoke test of parse_game_detail_html."""
        html = """
        <html><body>
        <div class="box-score-area">구장: JAMSIL 관중: 15,000 개시: 18:00 경기시간: 3:00</div>
        <table>
          <thead><tr><th>팀</th><th>1</th><th>2</th><th>3</th><th>R</th><th>H</th><th>E</th></tr></thead>
          <tbody>
            <tr><td>LG</td><td>0</td><td>1</td><td>2</td><td>3</td><td>8</td><td>0</td></tr>
            <tr><td>SS</td><td>1</td><td>0</td><td>0</td><td>1</td><td>5</td><td>1</td></tr>
          </tbody>
        </table>
        </body></html>
        """
        result = parse_game_detail_html(html, "20250325LGSS0", "20250325")
        assert result["game_id"] == "20250325LGSS0"
        assert result["game_date"] == "20250325"
        assert "teams" in result
        assert "hitters" in result
        assert "pitchers" in result
        assert "metadata" in result
        assert result["home_team_code"] is not None
        assert result["away_team_code"] is not None
        assert result["metadata"]["stadium"] == "JAMSIL"
