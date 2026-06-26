from __future__ import annotations

import pytest

from src.crawlers.relay_crawler import RelayCrawler, _time_diff_score


class TestScoreSuffixMatch:
    def setup_method(self):
        self.crawler = RelayCrawler.__new__(RelayCrawler)

    def test_exact_match(self):
        suffixes = {"exact": "LGSS0", "legacy": "0625LGSS0", "team": "ZZZZ", "id_has_teams": False, "dh_no": "0"}
        assert self.crawler._score_suffix_match("20250625LGSS0", suffixes) == 100

    def test_legacy_match(self):
        suffixes = {"exact": "XXXX", "legacy": "LGSS0", "team": "ZZZZ", "id_has_teams": False, "dh_no": "0"}
        assert self.crawler._score_suffix_match("20250625LGSS0", suffixes) == 90

    def test_team_match(self):
        suffixes = {"exact": "XXXX", "legacy": "YYYY", "team": "LGSS0", "id_has_teams": False, "dh_no": "0"}
        assert self.crawler._score_suffix_match("20250625LGSS0", suffixes) == 80

    def test_dh_no_match(self):
        suffixes = {"exact": "XXXX", "legacy": "YYYY", "team": "ZZZZ", "id_has_teams": True, "dh_no": "0"}
        assert self.crawler._score_suffix_match("20250625LGSS0", suffixes) == 20

    def test_no_match(self):
        suffixes = {"exact": "XXXX", "legacy": "YYYY", "team": "ZZZZ", "id_has_teams": False, "dh_no": "0"}
        assert self.crawler._score_suffix_match("20250625LGSS0", suffixes) == 0


class TestScoreTeamMatch:
    def setup_method(self):
        self.crawler = RelayCrawler.__new__(RelayCrawler)

    def test_exact_teams_match(self):
        score, matched = self.crawler._score_team_match("LG", "SS", "LG", "SS")
        assert score == 50
        assert matched is True

    def test_partial_away_match(self):
        score, matched = self.crawler._score_team_match("LG", "KT", "LG", "SS")
        assert score == -140
        assert matched is False

    def test_partial_home_match(self):
        score, matched = self.crawler._score_team_match("KT", "SS", "LG", "SS")
        assert score == -140
        assert matched is False

    def test_one_side_matching_no_penalty(self):
        score, matched = self.crawler._score_team_match("LG", "", "LG", "SS")
        assert score == 10
        assert matched is False

    def test_mismatch_penalty(self):
        score, matched = self.crawler._score_team_match("KT", "NC", "LG", "SS")
        assert score == -150
        assert matched is False

    def test_empty_teams_no_penalty(self):
        score, matched = self.crawler._score_team_match("", "", "LG", "SS")
        assert score == 0
        assert matched is False

    def test_one_empty_no_match(self):
        score, matched = self.crawler._score_team_match("LG", "", "LG", "SS")
        assert score == 10
        assert matched is False


class TestScoreDateMatch:
    def setup_method(self):
        self.crawler = RelayCrawler.__new__(RelayCrawler)

    def test_exact_date_match(self):
        game = {"gameDate": "2025-06-25"}
        assert self.crawler._score_date_match(game, "20250625LGSS0", "20250625") == 30

    def test_partial_date_match(self):
        game = {"gameDate": "20250620"}
        assert self.crawler._score_date_match(game, "20250625LGSS0", "20250625") == 0

    def test_same_month_day_different(self):
        game = {"gameDate": "2025-06-25"}
        assert self.crawler._score_date_match(game, "20250625LGSS0", "20250625") == 30

    def test_no_match(self):
        game = {"gameDate": "2025-07-25"}
        assert self.crawler._score_date_match(game, "20250625LGSS0", "20250625") == 0

    def test_empty_game_date_falls_back_to_game_id(self):
        game = {"gameDate": ""}
        assert self.crawler._score_date_match(game, "20250625LGSS0", "20250625") == 30

    def test_no_date_at_all(self):
        game = {}
        assert self.crawler._score_date_match(game, "20250625LGSS0", "20250625") == 30


class TestScoreTimeMatch:
    def setup_method(self):
        self.crawler = RelayCrawler.__new__(RelayCrawler)

    def test_exact_time_match(self):
        game = {"gameStartTime": "18:30"}
        assert self.crawler._score_time_match(game, "18:30") > 0

    def test_close_time_match(self):
        game = {"gameStartTime": "18:35"}
        assert self.crawler._score_time_match(game, "18:30") > 0

    def test_no_game_time(self):
        game = {}
        assert self.crawler._score_time_match(game, "18:30") == 0

    def test_no_input_time(self):
        game = {"gameStartTime": "18:30"}
        assert self.crawler._score_time_match(game, None) == 0

    def test_startTime_field(self):
        game = {"startTime": "19:00"}
        assert self.crawler._score_time_match(game, "19:00") > 0


class TestParseGameState:
    def setup_method(self):
        self.crawler = RelayCrawler.__new__(RelayCrawler)

    def test_full_state(self):
        log = {"currentGameState": {"homeScore": 5, "awayScore": 3, "out": 2, "base1": 1, "base2": 0, "base3": 1}}
        result = self.crawler._parse_game_state(log)
        assert result["home_score"] == 5
        assert result["away_score"] == 3
        assert result["outs"] == 2
        assert result["base_state"] == 5

    def test_empty_state(self):
        log = {"currentGameState": {}}
        result = self.crawler._parse_game_state(log)
        assert result["home_score"] == 0
        assert result["away_score"] == 0
        assert result["outs"] == 0
        assert result["base_state"] == 0

    def test_no_state_key(self):
        log = {}
        result = self.crawler._parse_game_state(log)
        assert result["home_score"] == 0
        assert result["away_score"] == 0

    def test_full_bases(self):
        log = {"currentGameState": {"base1": 1, "base2": 1, "base3": 1}}
        result = self.crawler._parse_game_state(log)
        assert result["base_state"] == 7

    def test_no_bases(self):
        log = {"currentGameState": {"base1": 0, "base2": 0, "base3": 0}}
        result = self.crawler._parse_game_state(log)
        assert result["base_state"] == 0

    def test_null_scores(self):
        log = {"currentGameState": {"homeScore": None, "awayScore": None}}
        result = self.crawler._parse_game_state(log)
        assert result["home_score"] == 0
        assert result["away_score"] == 0


class TestParseSegmentInningHalf:
    def setup_method(self):
        self.crawler = RelayCrawler.__new__(RelayCrawler)

    def test_top_inning(self):
        segment = {"title": "3회초 공격"}
        inning, half = self.crawler._parse_segment_inning_half(segment)
        assert inning == 3
        assert half == "top"

    def test_bottom_inning(self):
        segment = {"title": "5회말 공격"}
        inning, half = self.crawler._parse_segment_inning_half(segment)
        assert inning == 5
        assert half == "bottom"

    def test_inn_field_top(self):
        segment = {"inn": 7, "homeOrAway": "AWAY"}
        inning, half = self.crawler._parse_segment_inning_half(segment)
        assert inning == 7
        assert half == "top"

    def test_inn_field_bottom(self):
        segment = {"inn": 4, "homeOrAway": "HOME"}
        inning, half = self.crawler._parse_segment_inning_half(segment)
        assert inning == 4
        assert half == "bottom"

    def test_numeric_side_top(self):
        segment = {"inn": 1, "homeOrAway": "0"}
        inning, half = self.crawler._parse_segment_inning_half(segment)
        assert inning == 1
        assert half == "top"

    def test_numeric_side_bottom(self):
        segment = {"inn": 2, "homeOrAway": "1"}
        inning, half = self.crawler._parse_segment_inning_half(segment)
        assert inning == 2
        assert half == "bottom"

    def test_no_title_no_inn(self):
        segment = {}
        inning, half = self.crawler._parse_segment_inning_half(segment)
        assert inning == 0
        assert half is None

    def test_unknown_side(self):
        segment = {"inn": 2, "homeOrAway": "UNKNOWN"}
        inning, half = self.crawler._parse_segment_inning_half(segment)
        assert inning == 2
        assert half is None


class TestScheduleGameHasTeamMatch:
    def test_both_match(self):
        game = {"awayTeamCode": "LG", "homeTeamCode": "SS"}
        assert RelayCrawler._schedule_game_has_team_match(game, "LG", "SS") is True

    def test_away_only(self):
        game = {"awayTeamCode": "LG", "homeTeamCode": "KT"}
        assert RelayCrawler._schedule_game_has_team_match(game, "LG", "SS") is False

    def test_empty_codes(self):
        game = {"awayTeamCode": "", "homeTeamCode": ""}
        assert RelayCrawler._schedule_game_has_team_match(game, "LG", "SS") is True

    def test_missing_fields(self):
        game = {}
        assert RelayCrawler._schedule_game_has_team_match(game, "LG", "SS") is True


class TestComputePayloadHash:
    def setup_method(self):
        self.crawler = RelayCrawler.__new__(RelayCrawler)

    def test_deterministic(self):
        data = [{"a": 1, "b": 2}]
        h1 = self.crawler._compute_payload_hash(data)
        h2 = self.crawler._compute_payload_hash(data)
        assert h1 == h2

    def test_12_chars(self):
        data = [{"key": "value"}]
        result = self.crawler._compute_payload_hash(data)
        assert len(result) == 12

    def test_different_data_different_hash(self):
        h1 = self.crawler._compute_payload_hash([{"a": 1}])
        h2 = self.crawler._compute_payload_hash([{"a": 2}])
        assert h1 != h2


class TestTimeDiffScore:
    def test_zero_diff(self):
        assert _time_diff_score(0) == 25

    def test_small_diff(self):
        assert _time_diff_score(10) == 15

    def test_medium_diff(self):
        assert _time_diff_score(60) == -10

    def test_large_diff(self):
        assert _time_diff_score(121) == -30
