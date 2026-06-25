from __future__ import annotations

from src.crawlers.game_detail_crawler import (
    GameDetailCrawler,
)


class TestEmptyMetadata:
    def test_returns_all_none(self):
        result = GameDetailCrawler._empty_metadata()
        assert result == {
            "stadium": None,
            "attendance": None,
            "start_time": None,
            "end_time": None,
            "game_time": None,
            "duration_minutes": None,
        }


class TestParseNameAndUniform:
    def test_no_parentheses(self):
        name, uniform = GameDetailCrawler._parse_name_and_uniform("홍길동", {"등번호": "27"})
        assert name == "홍길동"
        assert uniform == "27"

    def test_parentheses_with_number(self):
        name, uniform = GameDetailCrawler._parse_name_and_uniform("홍길동(27)", {"등번호": None})
        assert name == "홍길동"
        assert uniform == "27"

    def test_parentheses_with_text(self):
        name, uniform = GameDetailCrawler._parse_name_and_uniform("홍길동(주장)", {"등번호": "10"})
        assert name == "홍길동"
        assert uniform == "10"

    def test_empty_cells(self):
        name, uniform = GameDetailCrawler._parse_name_and_uniform("김철수", {})
        assert name == "김철수"
        assert uniform is None


class TestResolveFromRosterMap:
    def test_direct_match(self):
        roster_map = {"김철수": [{"id": 12345, "uniform": "10"}]}
        pid, uniform = GameDetailCrawler._resolve_from_roster_map(roster_map, "김철수", None)
        assert pid == 12345
        assert uniform == "10"

    def test_no_match(self):
        roster_map = {"이영호": [{"id": 99999, "uniform": "5"}]}
        pid, uniform = GameDetailCrawler._resolve_from_roster_map(roster_map, "김철수", None)
        assert pid is None
        assert uniform is None

    def test_none_roster_map(self):
        pid, uniform = GameDetailCrawler._resolve_from_roster_map(None, "김철수", "10")
        assert pid is None
        assert uniform == "10"

    def test_multiple_candidates_with_uniform(self):
        roster_map = {"김철수": [{"id": 111, "uniform": "7"}, {"id": 222, "uniform": "10"}]}
        pid, uniform = GameDetailCrawler._resolve_from_roster_map(roster_map, "김철수", "7")
        assert pid == 111
        assert uniform == "7"

    def test_multiple_candidates_without_uniform(self):
        roster_map = {"김철수": [{"id": 111, "uniform": "7"}, {"id": 222, "uniform": "10"}]}
        pid, uniform = GameDetailCrawler._resolve_from_roster_map(roster_map, "김철수", None)
        assert pid is None
        assert uniform is None


class TestStatsComplete:
    def test_both_sides_populated(self):
        hitters = {"away": [{"player_id": 1}], "home": [{"player_id": 2}]}
        pitchers = {"away": [{"player_id": 3}], "home": [{"player_id": 4}]}
        assert GameDetailCrawler._stats_complete(hitters, pitchers) is True

    def test_empty_away_hitters(self):
        hitters = {"away": [], "home": [{"player_id": 2}]}
        pitchers = {"away": [{}], "home": [{}]}
        assert GameDetailCrawler._stats_complete(hitters, pitchers) is False


class TestHasPartialRecoveryAnchor:
    def test_has_line_score(self):
        team_info = {"away": {"line_score": [1, 0, 0]}}
        metadata = {"stadium": None, "attendance": None}
        assert GameDetailCrawler._has_partial_recovery_anchor(team_info, metadata) is True

    def test_has_stadium(self):
        team_info = {"away": {}}
        metadata = {"stadium": "잠실", "attendance": None}
        assert GameDetailCrawler._has_partial_recovery_anchor(team_info, metadata) is True

    def test_nothing(self):
        team_info = {"away": {}}
        metadata = {"stadium": None, "attendance": None}
        assert GameDetailCrawler._has_partial_recovery_anchor(team_info, metadata) is False


class TestParseMetadataInfoText:
    def test_full_metadata(self):
        metadata = {}
        text = "구장 : 잠실 관중 : 23,750 개시 : 18:30 종료 : 21:45 경기시간 : 3:15"
        GameDetailCrawler._parse_metadata_info_text(metadata, text)
        assert metadata["stadium"] == "잠실"
        assert metadata["attendance"] == 23750
        assert metadata["start_time"] == "18:30"
        assert metadata["end_time"] == "21:45"
        assert metadata["game_time"] == "3:15"

    def test_partial_metadata(self):
        metadata = {}
        text = "구장 : 문학"
        GameDetailCrawler._parse_metadata_info_text(metadata, text)
        assert metadata["stadium"] == "문학"

    def test_empty_text(self):
        metadata = {}
        GameDetailCrawler._parse_metadata_info_text(metadata, "")
        assert metadata == {}


class TestBackfillHitterPlateAppearances:
    def test_pas_none(self):
        stats = {
            "plate_appearances": None,
            "at_bats": 3,
            "walks": 1,
            "hbp": 0,
            "sacrifice_hits": 1,
            "sacrifice_flies": 0,
        }
        GameDetailCrawler._backfill_hitter_plate_appearances(stats)
        assert stats["plate_appearances"] == 5

    def test_pas_zero(self):
        stats = {"plate_appearances": 0, "at_bats": 4, "walks": 2, "hbp": 1, "sacrifice_hits": 0, "sacrifice_flies": 1}
        GameDetailCrawler._backfill_hitter_plate_appearances(stats)
        assert stats["plate_appearances"] == 8

    def test_pas_already_set(self):
        stats = {"plate_appearances": 10, "at_bats": 3, "walks": 1, "hbp": 0, "sacrifice_hits": 0, "sacrifice_flies": 0}
        GameDetailCrawler._backfill_hitter_plate_appearances(stats)
        assert stats["plate_appearances"] == 10


class TestParseDurationMinutes:
    def test_3h15m(self):
        assert GameDetailCrawler._parse_duration_minutes("3:15") == 195

    def test_none(self):
        assert GameDetailCrawler._parse_duration_minutes(None) is None

    def test_invalid(self):
        assert GameDetailCrawler._parse_duration_minutes("invalid") is None


class TestParseSeasonYear:
    def test_compact_date(self):
        assert GameDetailCrawler._parse_season_year("20251013") == 2025

    def test_hyphenated_date(self):
        assert GameDetailCrawler._parse_season_year("2026-06-26") == 2026

    def test_invalid(self):
        assert GameDetailCrawler._parse_season_year("abc") is None


class TestDeriveHitterStatsFromInningCells:
    def test_strikeout(self):
        cells = {"1": "삼진"}
        result = GameDetailCrawler._derive_hitter_stats_from_inning_cells(cells)
        assert result["strikeouts"] == 1
        assert result["walks"] == 0

    def test_walk(self):
        cells = {"1": "4구"}
        result = GameDetailCrawler._derive_hitter_stats_from_inning_cells(cells)
        assert result["walks"] == 1

    def test_hbp(self):
        cells = {"1": "사구"}
        result = GameDetailCrawler._derive_hitter_stats_from_inning_cells(cells)
        assert result["hbp"] == 1

    def test_sacrifice_hit(self):
        cells = {"1": "희생번트"}
        result = GameDetailCrawler._derive_hitter_stats_from_inning_cells(cells)
        assert result["sacrifice_hits"] == 1

    def test_sacrifice_fly(self):
        cells = {"1": "희생플라이"}
        result = GameDetailCrawler._derive_hitter_stats_from_inning_cells(cells)
        assert result["sacrifice_flies"] == 1

    def test_mixed(self):
        cells = {"1": "삼진", "2": "4구", "3": "희생번트"}
        result = GameDetailCrawler._derive_hitter_stats_from_inning_cells(cells)
        assert result["strikeouts"] == 1
        assert result["walks"] == 1
        assert result["sacrifice_hits"] == 1

    def test_empty_values(self):
        cells = {"1": "", "2": "&nbsp;"}
        result = GameDetailCrawler._derive_hitter_stats_from_inning_cells(cells)
        assert result == {"strikeouts": 0, "walks": 0, "hbp": 0, "sacrifice_hits": 0, "sacrifice_flies": 0}


class TestPopulateHitterStats:
    def test_basic_stats(self):
        stats = {}
        extras = {}
        cells = {"타석": "4", "타수": "3", "안타": "2", "타점": "1"}
        GameDetailCrawler._populate_hitter_stats(stats, extras, cells)
        assert stats["plate_appearances"] == 4
        assert stats["at_bats"] == 3
        assert stats["hits"] == 2
        assert stats["rbi"] == 1

    def test_float_stats(self):
        stats = {}
        extras = {}
        cells = {"타율": "0.333", "출루율": "0.400", "장타율": "0.500"}
        GameDetailCrawler._populate_hitter_stats(stats, extras, cells)
        assert stats["avg"] == 0.333
        assert stats["obp"] == 0.4
        assert stats["slg"] == 0.5

    def test_unknown_goes_to_extras(self):
        stats = {}
        extras = {}
        cells = {"Unknown Header": "value"}
        GameDetailCrawler._populate_hitter_stats(stats, extras, cells)
        assert "Unknown Header" not in stats
        assert extras["Unknown Header"] == "value"

    def test_empty_value_skipped(self):
        stats = {}
        extras = {}
        cells = {"타석": "", "타수": "-"}
        GameDetailCrawler._populate_hitter_stats(stats, extras, cells)
        assert "plate_appearances" not in stats


class TestPopulatePitcherStats:
    def test_basic_stats(self):
        stats = {}
        extras = {}
        cells = {"이닝": "5.2", "삼진": "7", "투구수": "92"}
        GameDetailCrawler._populate_pitcher_stats(stats, extras, cells)
        assert stats["innings_outs"] == 17
        assert stats["strikeouts"] == 7
        assert stats["pitches"] == 92

    def test_float_stats(self):
        stats = {}
        extras = {}
        cells = {"ERA": "3.18", "WHIP": "1.20"}
        GameDetailCrawler._populate_pitcher_stats(stats, extras, cells)
        assert stats["era"] == 3.18
        assert stats["whip"] == 1.2

    def test_innings_parsing(self):
        stats = {}
        extras = {}
        cells = {"이닝": "3.1"}
        GameDetailCrawler._populate_pitcher_stats(stats, extras, cells)
        assert stats["innings_outs"] == 10


class TestParseBattingOrder:
    def test_타순(self):
        assert GameDetailCrawler._parse_batting_order({"타순": "4"}) == 4

    def test_NO(self):
        assert GameDetailCrawler._parse_batting_order({"NO": "1"}) == 1

    def test_none(self):
        assert GameDetailCrawler._parse_batting_order({}) is None


class TestParsePosition:
    def test_POS(self):
        assert GameDetailCrawler._parse_position({"POS": "SS"}) == "SS"

    def test_포지션(self):
        assert GameDetailCrawler._parse_position({"포지션": "투수"}) == "투수"

    def test_none(self):
        assert GameDetailCrawler._parse_position({}) is None


class TestParseDecision:
    def test_win(self):
        assert GameDetailCrawler._parse_decision("승") == "W"

    def test_loss(self):
        assert GameDetailCrawler._parse_decision("패") == "L"

    def test_save(self):
        assert GameDetailCrawler._parse_decision("세") == "S"

    def test_hold(self):
        assert GameDetailCrawler._parse_decision("홀드") == "H"

    def test_none(self):
        assert GameDetailCrawler._parse_decision("") is None


class TestParseScoreboardRow:
    def setup_method(self):
        self.crawler = GameDetailCrawler.__new__(GameDetailCrawler)

    def test_full_row(self):
        headers = ["TEAM", "1", "2", "3", "4", "5", "R", "H", "E"]
        row = ["LG", "0", "1", "0", "2", "0", "3", "8", "1"]
        result = self.crawler._parse_scoreboard_row(headers, row, 2025)
        assert result["name"] == "LG"
        assert result["line_score"] == [0, 1, 0, 2, 0]
        assert result["score"] == 3
        assert result["hits"] == 8
        assert result["errors"] == 1

    def test_empty_row(self):
        result = self.crawler._parse_scoreboard_row([], [], 2025)
        assert result["name"] is None
        assert result["score"] is None

    def test_name_with_suffix(self):
        headers = ["TEAM", "R", "H", "E"]
        row = ["삼성승", "5", "10", "2"]
        result = self.crawler._parse_scoreboard_row(headers, row, 2025)
        assert result["name"] == "삼성"

    def test_name_with_draw(self):
        headers = ["TEAM", "R", "H", "E"]
        row = ["LG무", "3", "7", "1"]
        result = self.crawler._parse_scoreboard_row(headers, row, 2025)
        assert result["name"] == "LG"
