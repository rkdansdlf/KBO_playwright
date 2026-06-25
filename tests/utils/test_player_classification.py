from __future__ import annotations

import pytest

from src.utils.player_classification import (
    STAFF_KEYWORDS,
    PlayerCategory,
    classify_player,
)


class TestClassifyPlayer:
    def test_register_manager(self):
        entry = {"status_source": "register", "staff_role": "manager"}
        assert classify_player(entry) == PlayerCategory.MANAGER

    def test_register_coach(self):
        entry = {"status_source": "register", "staff_role": "coach"}
        assert classify_player(entry) == PlayerCategory.COACH

    def test_register_staff(self):
        entry = {"status_source": "register", "staff_role": "trainer"}
        assert classify_player(entry) == PlayerCategory.STAFF

    def test_register_no_role(self):
        entry = {"status_source": "register"}
        assert classify_player(entry) == PlayerCategory.STAFF

    def test_active_player(self):
        entry = {"team": "LG", "position": "유격수"}
        assert classify_player(entry) == PlayerCategory.ACTIVE

    def test_retired_empty_team(self):
        entry = {"team": "", "position": "투수"}
        assert classify_player(entry) == PlayerCategory.RETIRED

    def test_retired_dash_team(self):
        entry = {"team": "-", "position": "투수"}
        assert classify_player(entry) == PlayerCategory.RETIRED

    def test_retired_keyword(self):
        entry = {"team": "은퇴", "position": "투수"}
        assert classify_player(entry) == PlayerCategory.RETIRED

    def test_retired_english_keyword(self):
        entry = {"team": "retired", "position": "투수"}
        assert classify_player(entry) == PlayerCategory.RETIRED

    def test_manager_in_position(self):
        entry = {"team": "LG", "position": "감독"}
        assert classify_player(entry) == PlayerCategory.MANAGER

    def test_coach_in_position(self):
        entry = {"team": "LG", "position": "코치"}
        assert classify_player(entry) == PlayerCategory.COACH

    def test_coach_keyword_in_position(self):
        for keyword in STAFF_KEYWORDS:
            if keyword == "감독대행":
                continue  # This contains "감독" which triggers MANAGER first
            entry = {"team": "LG", "position": keyword}
            assert classify_player(entry) == PlayerCategory.COACH

    def test_staff_in_team(self):
        entry = {"team": "코치", "position": "투수"}
        assert classify_player(entry) == PlayerCategory.STAFF

    def test_manager_in_team(self):
        entry = {"team": "감독", "position": "투수"}
        assert classify_player(entry) == PlayerCategory.STAFF

    def test_none_team(self):
        entry = {"team": None, "position": "투수"}
        assert classify_player(entry) == PlayerCategory.RETIRED

    def test_whitespace_team(self):
        entry = {"team": "   ", "position": "투수"}
        assert classify_player(entry) == PlayerCategory.RETIRED


class TestStaffKeywords:
    def test_all_keywords_defined(self):
        expected = (
            "코치",
            "감독대행",
            "매니저",
            "트레이너",
            "재활",
            "전력분석",
            "불펜포수",
            "불펜",
            "컨디셔닝",
            "수비코디",
            "인스트럭터",
        )
        assert expected == STAFF_KEYWORDS
