from __future__ import annotations

import pytest

from src.crawlers.relay_crawler import (
    RelayCrawler,
    _events_to_legacy_innings,
    _pbp_rows_to_legacy_innings,
)


class TestEventsToLegacyInnings:
    def test_empty_events(self) -> None:
        assert _events_to_legacy_innings([]) == []

    def test_single_inning(self) -> None:
        events = [
            {"inning": 1, "inning_half": "top", "description": "Strikeout", "event_type": "K", "outs": 1},
            {"inning": 1, "inning_half": "top", "description": "Single", "event_type": "H", "outs": 1},
        ]
        result = _events_to_legacy_innings(events)
        assert len(result) == 1
        assert result[0]["inning"] == 1
        assert result[0]["half"] == "top"
        assert len(result[0]["plays"]) == 2

    def test_multiple_innings(self) -> None:
        events = [
            {"inning": 1, "inning_half": "top", "description": "out", "event_type": "K"},
            {"inning": 1, "inning_half": "bottom", "description": "hit", "event_type": "H"},
            {"inning": 2, "inning_half": "top", "description": "walk", "event_type": "BB"},
        ]
        result = _events_to_legacy_innings(events)
        assert len(result) == 3
        assert result[0]["inning"] == 1
        assert result[0]["half"] == "top"
        assert result[1]["half"] == "bottom"
        assert result[2]["inning"] == 2

    def test_key_field_mapping(self) -> None:
        events = [
            {
                "inning": 1,
                "inning_half": "top",
                "description": "Foul ball",
                "event_type": "F",
                "batter_name": "Kim",
                "pitcher": "Park",
                "result_code": "Foul",
                "outs": 0,
            },
        ]
        result = _events_to_legacy_innings(events)
        play = result[0]["plays"][0]
        assert play["description"] == "Foul ball"
        assert play["batter"] == "Kim"
        assert play["pitcher"] == "Park"
        assert play["result"] == "Foul"

    def test_fallback_batter_pitcher(self) -> None:
        events = [
            {
                "inning": 1,
                "inning_half": "top",
                "description": "Hit",
                "event_type": "H",
                "batter": "b_foo",
                "pitcher": "p_bar",
                "result": "single",
            },
        ]
        result = _events_to_legacy_innings(events)
        play = result[0]["plays"][0]
        assert play["batter"] == "b_foo"
        assert play["pitcher"] == "p_bar"
        assert play["result"] == "single"


class TestPbpRowsToLegacyInnings:
    def test_empty(self) -> None:
        assert _pbp_rows_to_legacy_innings([]) == []

    def test_single_inning(self) -> None:
        rows = [
            {"inning": 1, "inning_half": "top", "play_description": "Strikeout", "event_type": "K", "outs": 2},
            {"inning": 1, "inning_half": "top", "play_description": "Walk", "event_type": "BB", "outs": 2},
        ]
        result = _pbp_rows_to_legacy_innings(rows)
        assert len(result) == 1
        assert len(result[0]["plays"]) == 2
        assert result[0]["plays"][0]["description"] == "Strikeout"

    def test_multiple_innings(self) -> None:
        rows = [
            {"inning": 1, "inning_half": "top", "play_description": "A", "event_type": "X"},
            {"inning": 2, "inning_half": "bottom", "play_description": "B", "event_type": "Y"},
        ]
        result = _pbp_rows_to_legacy_innings(rows)
        assert len(result) == 2

    def test_fallback_description(self) -> None:
        rows = [
            {"inning": 1, "inning_half": "top", "description": "fallback_desc", "event_type": "Z"},
        ]
        result = _pbp_rows_to_legacy_innings(rows)
        assert result[0]["plays"][0]["description"] == "fallback_desc"

    def test_key_field_mapping(self) -> None:
        rows = [
            {
                "inning": 1,
                "inning_half": "top",
                "play_description": "Single",
                "event_type": "H",
                "batter_name": "Lee",
                "pitcher": "Choi",
                "result": "single",
                "outs": 1,
            },
        ]
        result = _pbp_rows_to_legacy_innings(rows)
        play = result[0]["plays"][0]
        assert play["description"] == "Single"
        assert play["batter"] == "Lee"
        assert play["pitcher"] == "Choi"
        assert play["result"] == "single"
        assert play["outs"] == 1


class TestMapToNaverId:
    def test_direct(self) -> None:
        assert RelayCrawler._map_to_naver_id("20260412SKLG0") == "20260412SKLG02026"

    def test_double_header(self) -> None:
        assert RelayCrawler._map_to_naver_id("20260412SKLG1") == "20260412SKLG12026"

    def test_short_id(self) -> None:
        assert RelayCrawler._map_to_naver_id("9999") == "99999999"


class TestScoreStadiumMatch:
    def test_exact_match(self) -> None:
        game = {"stadium": "잠실"}
        assert RelayCrawler._score_stadium_match(game, "잠실") == 30

    def test_case_insensitive(self) -> None:
        game = {"stadium": "Jamsil"}
        assert RelayCrawler._score_stadium_match(game, "jamsil") == 30

    def test_place_fallback(self) -> None:
        game = {"place": "문학"}
        assert RelayCrawler._score_stadium_match(game, "문학") == 30

    def test_no_match(self) -> None:
        game = {"stadium": "잠실"}
        assert RelayCrawler._score_stadium_match(game, "문학") == 0

    def test_none_stadium(self) -> None:
        game = {"stadium": "잠실"}
        assert RelayCrawler._score_stadium_match(game, None) == 0


class TestGameTimeMins:
    def test_normal_time(self) -> None:
        game = {"gameStartTime": "14:00"}
        assert RelayCrawler._game_time_mins(game) == 840

    def test_start_time_fallback(self) -> None:
        game = {"startTime": "18:30"}
        assert RelayCrawler._game_time_mins(game) == 1110

    def test_empty_time(self) -> None:
        game = {"gameStartTime": ""}
        assert RelayCrawler._game_time_mins(game) == 0

    def test_invalid_format(self) -> None:
        game = {"gameStartTime": "invalid"}
        assert RelayCrawler._game_time_mins(game) == 0

    def test_missing_key(self) -> None:
        game = {}
        assert RelayCrawler._game_time_mins(game) == 0


class TestIsTeamInId:
    def test_direct_match(self) -> None:
        assert RelayCrawler._is_team_in_id("SK", "20260412SKLG0") is True

    def test_legacy_match(self) -> None:
        assert RelayCrawler._is_team_in_id("SSG", "20260412SKLG0") is True

    def test_no_match(self) -> None:
        assert RelayCrawler._is_team_in_id("KT", "20260412SKLG0") is False

    def test_kia_legacy(self) -> None:
        assert RelayCrawler._is_team_in_id("KIA", "20260412HTLG0") is True

    def test_wo_legacy(self) -> None:
        assert RelayCrawler._is_team_in_id("WO", "20260412KHLG0") is True
