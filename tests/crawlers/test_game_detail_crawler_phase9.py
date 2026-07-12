from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.crawlers.game_detail_crawler import (
    BoxscoreCrawlContext,
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


@pytest.mark.asyncio
class TestBoxscoreExtractionFlows:
    async def test_extract_hitters_merges_extra_stats_derives_innings_and_uses_roster(self):
        crawler = GameDetailCrawler()
        base_rows = [
            {
                "playerName": "홍길동(7)",
                "playerId": None,
                "cells": {"타순": "1", "POS": "SS", "타석": "0", "타수": "3", "안타": "1", "볼넷": "0"},
            },
            {"playerName": "합계", "playerId": None, "cells": {"타수": "4", "안타": "1"}},
        ]
        extra_rows = [{"playerName": "홍길동", "cells": {"볼넷": "1", "OPS": "0.900"}}]
        inning_rows = [{"cells": {"1": "삼진"}}]
        crawler._extract_table_rows = AsyncMock(side_effect=[base_rows, extra_rows, inning_rows])
        ctx = BoxscoreCrawlContext(
            page=MagicMock(),
            season_year=2025,
            roster_map={"홍길동": [{"id": 123, "uniform": "7"}]},
        )

        hitters, team_total = await crawler._extract_hitters(ctx, team_side="away", team_code="LG")

        assert len(hitters) == 1
        hitter = hitters[0]
        assert hitter["player_id"] == 123
        assert hitter["uniform_no"] == "7"
        assert hitter["batting_order"] == 1
        assert hitter["position"] == "SS"
        assert hitter["stats"]["walks"] == 1
        assert hitter["stats"]["strikeouts"] == 1
        assert hitter["stats"]["plate_appearances"] == 4
        assert hitter["stats"]["ops"] == 0.9
        assert team_total == {"at_bats": 4, "hits": 1}

    async def test_extract_pitchers_uses_roster_and_parses_decision(self):
        crawler = GameDetailCrawler()
        rows = [
            {
                "playerName": "최영(18)",
                "playerId": None,
                "cells": {"이닝": "5.2", "결과": "승", "삼진": "7"},
            },
            {"playerName": "합계", "playerId": None, "cells": {}},
        ]
        crawler._extract_table_rows = AsyncMock(return_value=rows)
        ctx = BoxscoreCrawlContext(
            page=MagicMock(),
            season_year=2025,
            roster_map={"최영": [{"id": 456, "uniform": "18"}]},
        )

        pitchers = await crawler._extract_pitchers(ctx, team_side="home", team_code="LG")

        assert pitchers == [
            {
                "player_id": 456,
                "player_name": "최영",
                "uniform_no": "18",
                "team_code": "LG",
                "team_side": "home",
                "is_starting": True,
                "appearance_seq": 1,
                "stats": {"strikeouts": 7, "decision": "W", "innings_outs": 17},
                "extras": {"결과": "승"},
            },
        ]

    async def test_extract_team_info_parses_scoreboard_rows(self):
        page = AsyncMock()
        page.evaluate.return_value = {
            "headers": ["TEAM", "1", "2", "3", "R", "H", "E"],
            "rows": [
                ["LG", "1", "0", "2", "3", "7", "0"],
                ["두산", "0", "1", "0", "1", "5", "1"],
            ],
        }
        crawler = GameDetailCrawler()

        teams = await crawler._extract_team_info(page, "20250501LGOB0", 2025)

        assert teams["away"] == {
            "name": "LG",
            "code": "LG",
            "score": 3,
            "hits": 7,
            "errors": 0,
            "line_score": [1, 0, 2],
        }
        assert teams["home"] == {
            "name": "두산",
            "code": "DB",
            "score": 1,
            "hits": 5,
            "errors": 1,
            "line_score": [0, 1, 0],
        }

    async def test_extract_team_info_falls_back_to_live_scores_and_game_id_codes(self):
        page = AsyncMock()
        page.evaluate.return_value = None
        crawler = GameDetailCrawler()
        crawler._extract_live_scores = AsyncMock(
            return_value={
                "away": {"name": "LG", "code": None, "score": 3, "hits": 7, "errors": 0, "line_score": []},
                "home": {"name": "두산", "code": None, "score": 1, "hits": 5, "errors": 1, "line_score": []},
            },
        )
        crawler._fetch_scoreboard_inning_scores = AsyncMock(return_value=None)

        teams = await crawler._extract_team_info(page, "20250501LGOB0", 2025)

        assert teams["away"]["code"] == "LG"
        assert teams["home"]["code"] == "DB"
        crawler._fetch_scoreboard_inning_scores.assert_awaited_once_with(page, "20250501LGOB0", "LG", "OB")

    async def test_crawl_games_uses_injected_pool_and_preserves_input_order(self):
        pool = MagicMock(max_pages=2)
        pool.start = AsyncMock()
        pool.acquire = AsyncMock(side_effect=[MagicMock(), MagicMock()])
        pool.release = AsyncMock()
        pool.close = AsyncMock()
        crawler = GameDetailCrawler(resolver=MagicMock(), pool=pool)

        async def _crawl_single(_page, game_id, _game_date, *, lightweight):
            return {"game_id": game_id, "lightweight": lightweight}

        crawler._crawl_single = AsyncMock(side_effect=_crawl_single)
        games = [
            {"game_id": "20250501LGOB0", "game_date": "20250501"},
            {"game_id": "20250502KTSS0", "game_date": "20250502"},
        ]

        payloads = await crawler.crawl_games(games, concurrency=2, lightweight=True)

        assert [payload["game_id"] for payload in payloads] == ["20250501LGOB0", "20250502KTSS0"]
        assert all(payload["lightweight"] for payload in payloads)
        pool.start.assert_awaited_once()
        assert pool.release.await_count == 2
        pool.close.assert_not_awaited()

    async def test_navigate_section_respects_compliance_block(self):
        crawler = GameDetailCrawler()
        ctx = BoxscoreCrawlContext(page=AsyncMock(), game_id="20250501LGOB0", game_date="20250501")

        with patch("src.crawlers.game_detail_crawler.compliance.is_allowed", new=AsyncMock(return_value=False)):
            success, reason, url = await crawler._navigate_section(ctx, "BOX_SCORE")

        assert success is False
        assert reason == "blocked"
        assert "gameId=20250501LGOB0" in url

    async def test_crawl_single_lightweight_builds_partial_payload(self):
        crawler = GameDetailCrawler()
        crawler._navigate_section = AsyncMock(return_value=(True, "ok", "https://example.test/review"))
        crawler._wait_for_boxscore = AsyncMock(return_value=(True, "ok"))
        crawler._load_roster_map_from_lineup = AsyncMock(return_value={})
        crawler._extract_team_info = AsyncMock(
            return_value={
                "away": {"code": "LG", "score": 3},
                "home": {"code": "DB", "score": 1},
            },
        )
        crawler._extract_metadata = AsyncMock(return_value={"stadium": "잠실"})
        crawler._extract_game_summary = AsyncMock(return_value={"status": "running"})
        crawler._extract_live_scores = AsyncMock(
            return_value={"away": {"score": 3}, "home": {"score": 1}},
        )

        result = await crawler._crawl_single(AsyncMock(), "20250501LGOB0", "20250501", lightweight=True)

        assert result == {
            "game_id": "20250501LGOB0",
            "game_date": "20250501",
            "metadata": {"stadium": "잠실"},
            "summary": {"status": "running"},
            "teams": {"away": {"code": "LG", "score": 3}, "home": {"code": "DB", "score": 1}},
            "home_team_code": "DB",
            "away_team_code": "LG",
            "hitters": {"away": [], "home": []},
            "pitchers": {"away": [], "home": []},
            "lifecycle_state": "running",
        }

    async def test_extract_metadata_combines_explicit_and_info_area_fields(self):
        stadium = MagicMock()
        stadium.text_content = AsyncMock(return_value="구장 : 잠실")
        crowd = MagicMock()
        crowd.text_content = AsyncMock(return_value="관중 : 12,345")
        info = MagicMock()
        info.text_content = AsyncMock(return_value="개시 : 18:30 종료 : 21:45 경기시간 : 3:15")
        page = AsyncMock()
        page.query_selector.side_effect = [stadium, crowd, info]

        metadata = await GameDetailCrawler()._extract_metadata(page)

        assert metadata == {
            "stadium": "잠실",
            "attendance": 12345,
            "start_time": "18:30",
            "end_time": "21:45",
            "game_time": "3:15",
            "duration_minutes": 195,
        }

    async def test_extract_live_scores_returns_normalized_partial_team_info(self):
        page = AsyncMock()
        page.evaluate.return_value = {
            "away": {"code": "lg", "name": "LG", "score": 3},
            "home": {"code": "db", "name": "두산", "score": 1},
        }

        scores = await GameDetailCrawler()._extract_live_scores(page)

        assert scores == {
            "away": {
                "code": "lg",
                "name": "LG",
                "score": 3,
                "hits": None,
                "errors": None,
                "line_score": [],
            },
            "home": {
                "code": "db",
                "name": "두산",
                "score": 1,
                "hits": None,
                "errors": None,
                "line_score": [],
            },
        }

    async def test_wait_for_boxscore_records_timeout_after_second_cancel_check(self):
        crawler = GameDetailCrawler()
        crawler._is_cancelled_boxscore_page = AsyncMock(side_effect=[False, False])
        crawler._save_boxscore_timeout_screenshot = AsyncMock()
        page = AsyncMock()
        page.url = "https://example.test/review"
        page.wait_for_selector.side_effect = RuntimeError("timeout")

        with patch("src.crawlers.game_detail_crawler.PlaywrightError", RuntimeError):
            ready, reason = await crawler._wait_for_boxscore(page, game_id="20250501LGOB0")

        assert (ready, reason) == (False, "timeout")
        crawler._save_boxscore_timeout_screenshot.assert_awaited_once()

    async def test_extract_detailed_stats_returns_hitter_and_pitcher_pairs(self):
        crawler = GameDetailCrawler()
        ctx = BoxscoreCrawlContext(
            page=AsyncMock(),
            game_id="20250501LGOB0",
            team_info={"away": {"code": "LG"}, "home": {"code": "DB"}},
            metadata={"stadium": "잠실"},
        )
        hitters = {"away": [{"stats": {"hits": 1, "at_bats": 3}}], "home": [{"stats": {"hits": 1, "at_bats": 3}}]}
        pitchers = {"away": [{"player_id": 1}], "home": [{"player_id": 2}]}
        crawler._click_review_tab_if_present = AsyncMock()
        crawler._extract_hitter_pair = AsyncMock(return_value=(hitters, {"away": {"hits": 1, "at_bats": 3}}))
        crawler._extract_pitcher_pair = AsyncMock(return_value=pitchers)
        crawler._retry_missing_boxscore_sections = AsyncMock(
            return_value=(hitters, {"away": {"hits": 1, "at_bats": 3}}, pitchers)
        )
        crawler._validate_hitter_totals = AsyncMock()

        result = await crawler._extract_detailed_stats(ctx)

        assert result == (hitters, pitchers)
        crawler._validate_hitter_totals.assert_awaited_once()

    async def test_recover_missing_hitter_and_pitcher_sections(self):
        crawler = GameDetailCrawler()
        ctx = BoxscoreCrawlContext(
            page=AsyncMock(),
            game_id="20250501LGOB0",
            team_info={"away": {"code": "LG"}, "home": {"code": "DB"}},
        )
        crawler._navigate_section = AsyncMock(return_value=(True, "ok", "https://example.test"))
        recovered_hitters = {"away": [{"player_id": 1}], "home": [{"player_id": 2}]}
        recovered_totals = {"away": {}, "home": {}}
        recovered_pitchers = {"away": [{"player_id": 3}], "home": [{"player_id": 4}]}
        crawler._extract_hitter_pair = AsyncMock(return_value=(recovered_hitters, recovered_totals))
        crawler._extract_pitcher_pair = AsyncMock(return_value=recovered_pitchers)

        hitters, totals = await crawler._recover_hitter_section_if_missing(
            ctx, {"away": [], "home": []}, {"away": {}, "home": {}}
        )
        pitchers = await crawler._recover_pitcher_section_if_missing(ctx, {"away": [], "home": []})

        assert (hitters, totals) == (recovered_hitters, recovered_totals)
        assert pitchers == recovered_pitchers

    async def test_validate_hitter_totals_saves_debug_screenshot_on_mismatch(self):
        page = AsyncMock()
        hitters = {"away": [{"stats": {"hits": 1, "at_bats": 3}}], "home": []}
        totals = {"away": {"hits": 2, "at_bats": 4}}

        await GameDetailCrawler()._validate_hitter_totals(page, "20250501LGOB0", hitters, totals)

        page.screenshot.assert_awaited_once_with(path="data/integrity_warning_20250501LGOB0_away.png")

    async def test_cancelled_boxscore_status_is_detected_before_waiting(self):
        status = MagicMock()
        status.text_content = AsyncMock(return_value="우천취소")
        page = AsyncMock()
        page.url = "https://example.test/review"
        page.query_selector.return_value = status

        cancelled = await GameDetailCrawler()._is_cancelled_boxscore_page(page)

        assert cancelled is True
