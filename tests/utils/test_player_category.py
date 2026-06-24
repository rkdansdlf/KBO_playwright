from __future__ import annotations

from src.utils.player_classification import (
    STAFF_KEYWORDS,
    PlayerCategory,
    classify_player,
)


class TestPlayerCategory:
    def test_active_value(self):
        assert PlayerCategory.ACTIVE == "ACTIVE"

    def test_retired_value(self):
        assert PlayerCategory.RETIRED == "RETIRED"

    def test_manager_value(self):
        assert PlayerCategory.MANAGER == "MANAGER"

    def test_coach_value(self):
        assert PlayerCategory.COACH == "COACH"

    def test_staff_value(self):
        assert PlayerCategory.STAFF == "STAFF"


class TestClassifyPlayer:
    def test_registered_manager(self):
        entry = {"status_source": "register", "staff_role": "manager"}
        assert classify_player(entry) == PlayerCategory.MANAGER

    def test_registered_coach(self):
        entry = {"status_source": "register", "staff_role": "coach"}
        assert classify_player(entry) == PlayerCategory.COACH

    def test_registered_staff(self):
        entry = {"status_source": "register", "staff_role": "trainer"}
        assert classify_player(entry) == PlayerCategory.STAFF

    def test_registered_no_role(self):
        entry = {"status_source": "register"}
        assert classify_player(entry) == PlayerCategory.STAFF

    def test_manager_position(self):
        entry = {"team": "LG", "position": "감독"}
        assert classify_player(entry) == PlayerCategory.MANAGER

    def test_coach_position(self):
        entry = {"team": "LG", "position": "코치"}
        assert classify_player(entry) == PlayerCategory.COACH

    def test_trainer_position(self):
        entry = {"team": "LG", "position": "트레이너"}
        assert classify_player(entry) == PlayerCategory.COACH

    def test_empty_team_retired(self):
        entry = {"team": "", "position": "투수"}
        assert classify_player(entry) == PlayerCategory.RETIRED

    def test_dash_team_retired(self):
        entry = {"team": "-", "position": "투수"}
        assert classify_player(entry) == PlayerCategory.RETIRED

    def test_none_team_retired(self):
        entry = {"team": None, "position": "투수"}
        assert classify_player(entry) == PlayerCategory.RETIRED

    def test_retired_keyword_in_team(self):
        entry = {"team": "은퇴", "position": "투수"}
        assert classify_player(entry) == PlayerCategory.RETIRED

    def test_retired_english(self):
        entry = {"team": "retired", "position": "투수"}
        assert classify_player(entry) == PlayerCategory.RETIRED

    def test_coach_in_team(self):
        entry = {"team": "코치", "position": "투수"}
        assert classify_player(entry) == PlayerCategory.STAFF

    def test_active_player(self):
        entry = {"team": "LG", "position": "투수"}
        assert classify_player(entry) == PlayerCategory.ACTIVE

    def test_active_hitter(self):
        entry = {"team": "삼성", "position": "타자"}
        assert classify_player(entry) == PlayerCategory.ACTIVE


class TestStaffKeywords:
    def test_coach_keyword(self):
        assert "코치" in STAFF_KEYWORDS

    def test_trainer_keyword(self):
        assert "트레이너" in STAFF_KEYWORDS

    def test_manager_keyword(self):
        assert "매니저" in STAFF_KEYWORDS

    def test_rehab_keyword(self):
        assert "재활" in STAFF_KEYWORDS
