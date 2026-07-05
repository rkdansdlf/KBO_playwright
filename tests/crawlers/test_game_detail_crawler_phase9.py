from __future__ import annotations

from src.crawlers.game_detail_crawler import (
    HitterPayloadContext,
    GameDetailCrawler,
    PitcherPayloadContext,
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


class TestBuildLineScore:
    def test_normal_values(self):
        result = GameDetailCrawler._build_line_score(
            ["TEAM", "0", "1", "2", "3", "0", "1", "2", "3", "0", "1", "2", "3"]
        )
        assert result == [0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3]

    def test_with_empty_and_dash(self):
        result = GameDetailCrawler._build_line_score(["TEAM", "0", "-", "", "2"])
        assert result == [0, None, None, 2]

    def test_invalid_int(self):
        result = GameDetailCrawler._build_line_score(["TEAM", "X", "1"])
        assert result[0] is None
        assert result[1] == 1

    def test_short_list(self):
        result = GameDetailCrawler._build_line_score(["TEAM", "1", "2"])
        assert result == [1, 2]


class TestExtractRHE:
    def test_normal(self):
        r, h, e = GameDetailCrawler._extract_rhe(["LG", "0", "1", "0", "2", "0", "3", "8", "1"])
        assert r == 3
        assert h == 8
        assert e == 1

    def test_only_r_and_h(self):
        r, h, e = GameDetailCrawler._extract_rhe(["LG", "0", "1", "0", "2", "0", "3", "8"])
        assert r == 0
        assert h == 3
        assert e == 8

    def test_no_stats(self):
        r, h, e = GameDetailCrawler._extract_rhe(["LG"])
        assert r is None
        assert h is None
        assert e is None

    def test_with_dashes(self):
        r, h, e = GameDetailCrawler._extract_rhe(["LG", "0", "-", "", "-", "3", "-", "1"])
        assert r == 0
        assert h == 3
        assert e == 1

    def test_non_numeric_skipped(self):
        r, h, e = GameDetailCrawler._extract_rhe(["LG", "0", "X", "1", "3", "7"])
        assert r == 1
        assert h == 3
        assert e == 7


class TestBoxscoreTimeoutDebugPath:
    def test_normal(self):
        path = GameDetailCrawler._boxscore_timeout_debug_path("20250412SKLG0", lightweight=False)
        assert path.startswith("data/timeout_20250412SKLG0_")
        assert path.endswith(".png")

    def test_lightweight(self):
        path = GameDetailCrawler._boxscore_timeout_debug_path("20250412SKLG0", lightweight=True)
        assert path.startswith("data/lightweight_timeout_20250412SKLG0_")
        assert path.endswith(".png")


class TestApplyHitterInningDerivatives:
    def test_backfills_stats(self):
        stats = {"strikeouts": 0, "walks": None, "hbp": 0}
        inning_rows = [{"cells": {"1": "삼진", "2": "4구"}}]
        GameDetailCrawler._apply_hitter_inning_derivatives(stats, inning_rows, 1)
        assert stats["strikeouts"] == 1
        assert stats["walks"] == 1

    def test_skips_existing_nonzero(self):
        stats = {"strikeouts": 3, "walks": 0}
        inning_rows = [{"cells": {"1": "삼진"}}]
        GameDetailCrawler._apply_hitter_inning_derivatives(stats, inning_rows, 1)
        assert stats["strikeouts"] == 3
        assert stats["walks"] == 0

    def test_empty_inning_rows(self):
        stats = {"strikeouts": None}
        GameDetailCrawler._apply_hitter_inning_derivatives(stats, [], 1)
        assert stats["strikeouts"] is None

    def test_idx_out_of_range(self):
        stats = {"strikeouts": None}
        inning_rows = [{"cells": {"1": "삼진"}}]
        GameDetailCrawler._apply_hitter_inning_derivatives(stats, inning_rows, 5)
        assert stats["strikeouts"] is None


class TestBuildHitterPayload:
    def test_starter(self):
        ctx = HitterPayloadContext(
            row={"cells": {"타순": "1", "POS": "SS"}},
            idx=1,
            player_name="홍길동",
            p_id=123,
            uniform_no="7",
            team_code="LG",
            team_side="home",
            stats={"hits": 2, "at_bats": 4},
            extras={"note": "test"},
        )
        result = GameDetailCrawler._build_hitter_payload(ctx)
        assert result["player_id"] == 123
        assert result["player_name"] == "홍길동"
        assert result["uniform_no"] == "7"
        assert result["team_code"] == "LG"
        assert result["team_side"] == "home"
        assert result["batting_order"] == 1
        assert result["position"] == "SS"
        assert result["is_starter"] is True
        assert result["appearance_seq"] == 1
        assert result["stats"] == {"hits": 2, "at_bats": 4}
        assert result["extras"] == {"note": "test"}

    def test_substitute(self):
        ctx = HitterPayloadContext(
            row={"cells": {"타순": "10"}},
            idx=2,
            player_name="김철수",
            p_id=None,
            uniform_no=None,
            team_code="LG",
            team_side="away",
            stats={},
            extras={},
        )
        result = GameDetailCrawler._build_hitter_payload(ctx)
        assert result["player_id"] is None
        assert result["batting_order"] == 10
        assert result["is_starter"] is False
        assert result["extras"] is None


class TestBuildPitcherPayload:
    def test_full(self):
        ctx = PitcherPayloadContext(
            row={"cells": {"이닝": "5.0", "삼진": "7", "승": "1"}},
            idx=1,
            player_name="최영",
            p_id=456,
            uniform_no="18",
            team_code="SSG",
            team_side="away",
        )
        result = GameDetailCrawler._build_pitcher_payload(ctx)
        assert result["player_id"] == 456
        assert result["player_name"] == "최영"
        assert result["uniform_no"] == "18"
        assert result["team_code"] == "SSG"
        assert result["team_side"] == "away"
        assert result["is_starting"] is True
        assert result["appearance_seq"] == 1
        assert result["stats"]["innings_outs"] == 15
        assert result["stats"]["strikeouts"] == 7
        assert result["extras"] is None

    def test_substitute_with_extras(self):
        ctx = PitcherPayloadContext(
            row={"cells": {"ERA": "4.50", "Unknown": "val"}},
            idx=3,
            player_name="박철",
            p_id=None,
            uniform_no=None,
            team_code="LG",
            team_side="home",
        )
        result = GameDetailCrawler._build_pitcher_payload(ctx)
        assert result["player_id"] is None
        assert result["is_starting"] is False
        assert result["stats"]["era"] == 4.5
        assert result["stats"]["innings_outs"] is None
        assert result["extras"] == {"Unknown": "val"}

    def test_decision_hold(self):
        ctx = PitcherPayloadContext(
            row={"cells": {"이닝": "1.0", "결": "홀드"}},
            idx=2,
            player_name="이동",
            p_id=789,
            uniform_no="99",
            team_code="KT",
            team_side="away",
        )
        result = GameDetailCrawler._build_pitcher_payload(ctx)
        assert result["stats"]["decision"] == "H"
        assert result["stats"]["innings_outs"] == 3


class TestSelectHitterExtraRow:
    def test_extra_has_names_match(self):
        extra_map = {"홍길동": {"extra_hits": 1}}
        extra_rows = [{"extra_hits": 99}]
        result = GameDetailCrawler._select_hitter_extra_row(
            extra_has_names=True,
            extra_map=extra_map,
            extra_rows=extra_rows,
            player_name="홍길동",
            idx=1,
        )
        assert result == {"extra_hits": 1}

    def test_extra_has_names_no_match(self):
        extra_map = {"홍길동": {"extra_hits": 1}}
        result = GameDetailCrawler._select_hitter_extra_row(
            extra_has_names=True,
            extra_map=extra_map,
            extra_rows=[],
            player_name="김철수",
            idx=1,
        )
        assert result is None

    def test_by_index(self):
        extra_rows = [{"extra_hits": 10}, {"extra_hits": 20}]
        result = GameDetailCrawler._select_hitter_extra_row(
            extra_has_names=False,
            extra_map={},
            extra_rows=extra_rows,
            player_name="any",
            idx=2,
        )
        assert result == {"extra_hits": 20}

    def test_by_index_out_of_range(self):
        result = GameDetailCrawler._select_hitter_extra_row(
            extra_has_names=False,
            extra_map={},
            extra_rows=[{"extra_hits": 1}],
            player_name="any",
            idx=5,
        )
        assert result is None


class TestResolveHanwhaParkJunyoung:
    def test_single_candidate_low_era(self):
        row = {"cells": {"평균자책점": "2.50"}}
        result = GameDetailCrawler._resolve_hanwha_park_junyoung(row, [row], 1)
        assert result == 56709

    def test_single_candidate_high_era(self):
        row = {"cells": {"ERA": "5.00"}}
        result = GameDetailCrawler._resolve_hanwha_park_junyoung(row, [row], 1)
        assert result == 52731

    def test_single_candidate_invalid_era(self):
        row = {"cells": {}}
        result = GameDetailCrawler._resolve_hanwha_park_junyoung(row, [row], 1)
        assert result == 52731

    def test_multiple_candidates_first(self):
        rows = [
            {"playerName": "박준영", "cells": {}},
            {"playerName": "박준영", "cells": {}},
            {"playerName": "김철수", "cells": {}},
        ]
        result = GameDetailCrawler._resolve_hanwha_park_junyoung(rows[0], rows, 1)
        assert result == 52731

    def test_multiple_candidates_second(self):
        rows = [
            {"playerName": "박준영", "cells": {}},
            {"playerName": "박준영", "cells": {}},
            {"playerName": "김철수", "cells": {}},
        ]
        result = GameDetailCrawler._resolve_hanwha_park_junyoung(rows[1], rows, 2)
        assert result == 56709

    def test_multiple_candidates_fallback_low_idx(self):
        rows = [
            {"playerName": "박준영", "cells": {}},
            {"playerName": "박준영", "cells": {}},
        ]
        result = GameDetailCrawler._resolve_hanwha_park_junyoung(rows[0], rows, 0)
        assert result == 56709

    def test_multiple_candidates_fallback_high_idx(self):
        rows = [
            {"playerName": "박준영", "cells": {}},
            {"playerName": "박준영", "cells": {}},
        ]
        result = GameDetailCrawler._resolve_hanwha_park_junyoung(rows[1], rows, 5)
        assert result == 56709


class TestLogUnresolvedPlayerIds:
    def test_no_unresolved(self, caplog):
        hitters = {"away": [{"player_name": "홍길동", "player_id": 1}]}
        pitchers = {"home": [{"player_name": "최영", "player_id": 2}]}
        GameDetailCrawler._log_unresolved_player_ids("game1", hitters, pitchers)
        assert len(caplog.records) == 0

    def test_unresolved_hitters(self, caplog):
        caplog.set_level("WARNING")
        hitters = {
            "away": [{"player_name": "홍길동", "player_id": None, "team_code": "LG", "uniform_no": "7"}],
            "home": [],
        }
        pitchers = {"away": [], "home": []}
        GameDetailCrawler._log_unresolved_player_ids("game1", hitters, pitchers)
        assert any("game1" in r.message for r in caplog.records)

    def test_unresolved_pitchers(self, caplog):
        caplog.set_level("INFO")
        hitters = {"away": [], "home": []}
        pitchers = {
            "home": [{"player_name": "최영", "player_id": None, "team_code": "SSG", "uniform_no": "18"}],
        }
        GameDetailCrawler._log_unresolved_player_ids("game1", hitters, pitchers)
        info_msgs = [r.message for r in caplog.records if r.levelname == "INFO"]
        assert any("최영" in m for m in info_msgs)
