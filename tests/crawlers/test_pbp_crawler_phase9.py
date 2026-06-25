from __future__ import annotations

from unittest.mock import MagicMock

from src.crawlers.pbp_crawler import PBPCrawler


class TestFormatBaseString:
    def test_empty(self):
        assert PBPCrawler._format_base_string(0) == "---"

    def test_first_base(self):
        assert PBPCrawler._format_base_string(1) == "1--"

    def test_second_base(self):
        assert PBPCrawler._format_base_string(2) == "-2-"

    def test_third_base(self):
        assert PBPCrawler._format_base_string(4) == "--3"

    def test_first_and_second(self):
        assert PBPCrawler._format_base_string(3) == "12-"

    def test_first_and_third(self):
        assert PBPCrawler._format_base_string(5) == "1-3"

    def test_second_and_third(self):
        assert PBPCrawler._format_base_string(6) == "-23"

    def test_full_bases(self):
        assert PBPCrawler._format_base_string(7) == "123"


class TestParseInningHeader:
    def test_top_inning(self):
        result = PBPCrawler._parse_inning_header("3회초 공격", 1)
        assert result == {"inning": 3, "half": "top"}

    def test_bottom_inning(self):
        result = PBPCrawler._parse_inning_header("5회말 공격", 2)
        assert result == {"inning": 5, "half": "bottom"}

    def test_no_match(self):
        result = PBPCrawler._parse_inning_header("공격", 1)
        assert result == {"inning": 2, "half": "unknown"}

    def test_empty_string(self):
        result = PBPCrawler._parse_inning_header("", 0)
        assert result == {"inning": 1, "half": "unknown"}

    def test_double_digit_inning(self):
        result = PBPCrawler._parse_inning_header("10회초", 1)
        assert result == {"inning": 10, "half": "top"}


class TestInitialLegacyState:
    def test_returns_expected_dict(self):
        result = PBPCrawler._initial_legacy_state()
        assert "current_outs" in result
        assert "current_runners" in result
        assert "current_inning" in result
        assert "current_half" in result
        assert "home_score" in result
        assert "away_score" in result


class TestApplyInningHeader:
    def test_top_inning_top(self):
        state = {"current_inning": 0, "current_half": None}
        result = PBPCrawler._apply_inning_header(state, "3회초", "blue")
        assert result is True
        assert state["current_inning"] == 3
        assert state["current_half"] == "top"

    def test_bottom_inning_bottom(self):
        state = {"current_inning": 0, "current_half": None}
        result = PBPCrawler._apply_inning_header(state, "5회말", "blue")
        assert result is True
        assert state["current_inning"] == 5
        assert state["current_half"] == "bottom"

    def test_mismatched_half(self):
        state = {"current_inning": 3, "current_half": "top"}
        result = PBPCrawler._apply_inning_header(state, "3회말", "blue")
        assert result is True
        assert state["current_inning"] == 3
        assert state["current_half"] == "bottom"

    def test_matching_inning_different_half(self):
        state = {"current_inning": 3, "current_half": "top"}
        result = PBPCrawler._apply_inning_header(state, "4회초", "blue")
        assert result is True
        assert state["current_inning"] == 4

    def test_no_blue_in_cls(self):
        state = {}
        result = PBPCrawler._apply_inning_header(state, "3회초", "red")
        assert result is False

    def test_no_회_in_text(self):
        state = {}
        result = PBPCrawler._apply_inning_header(state, "공격", "blue")
        assert result is False


class TestIsLegacyEventText:
    def test_valid_event_red_class(self):
        assert PBPCrawler._is_legacy_event_text("안타", "red") is True

    def test_valid_event_normaifl_class(self):
        assert PBPCrawler._is_legacy_event_text("삼진", "normaiflTxt") is True

    def test_invalid_class(self):
        assert PBPCrawler._is_legacy_event_text("안타", "other") is False

    def test_game_prep_text(self):
        assert PBPCrawler._is_legacy_event_text("경기 준비중", "red") is False

    def test_game_start_text(self):
        assert PBPCrawler._is_legacy_event_text("경기 시작", "red") is False


class TestUpdateOutBaseState:
    def test_strikeout_returns_before(self):
        state = {"current_outs": 0, "current_runners": 0}
        outs, runners = PBPCrawler._update_out_base_state(state, "삼진")
        assert outs == 0
        assert runners == 0
        assert state["current_outs"] == 1

    def test_double_play(self):
        state = {"current_outs": 0, "current_runners": 0}
        outs, runners = PBPCrawler._update_out_base_state(state, "병살")
        assert outs == 0
        assert runners == 0
        assert state["current_outs"] == 0

    def test_out_at_first(self):
        state = {"current_outs": 1, "current_runners": 0}
        outs, runners = PBPCrawler._update_out_base_state(state, "아웃")
        assert outs == 1
        assert runners == 0
        assert state["current_outs"] == 2

    def test_no_change_for_hit(self):
        state = {"current_outs": 0, "current_runners": 0}
        outs, runners = PBPCrawler._update_out_base_state(state, "안타")
        assert outs == 0
        assert runners == 0
        assert state["current_outs"] == 0

    def test_fly_out(self):
        state = {"current_outs": 0, "current_runners": 0}
        outs, runners = PBPCrawler._update_out_base_state(state, "플라이 아웃")
        assert outs == 0
        assert runners == 0
        assert state["current_outs"] == 1

    def test_walk_plus_runner(self):
        state = {"current_outs": 0, "current_runners": 0}
        outs, runners = PBPCrawler._update_out_base_state(state, "1사 1루")
        assert outs == 1
        assert runners == 1
        assert state["current_outs"] == 1
        assert state["current_runners"] == 1


class TestIsAuthRedirect:
    def test_login_url(self):
        page = MagicMock()
        page.url = "https://www.koreabaseball.com/Login.aspx"
        assert PBPCrawler._is_auth_redirect(page) is True

    def test_error_url(self):
        page = MagicMock()
        page.url = "https://www.koreabaseball.com/Error.html"
        assert PBPCrawler._is_auth_redirect(page) is True

    def test_normal_url(self):
        page = MagicMock()
        page.url = "https://www.koreabaseball.com/GameCenter"
        assert PBPCrawler._is_auth_redirect(page) is False
