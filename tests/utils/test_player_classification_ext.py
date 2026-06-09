from src.utils.player_classification import STAFF_KEYWORDS, PlayerCategory, classify_player


class TestClassifyPlayer:
    def test_active_defaults(self):
        assert classify_player({"team": "LG", "position": "내야수"}) == PlayerCategory.ACTIVE

    def test_pitcher_still_active(self):
        assert classify_player({"team": "두산", "position": "투수"}) == PlayerCategory.ACTIVE

    def test_retired_by_team_keyword(self):
        assert classify_player({"team": "은퇴", "position": ""}) == PlayerCategory.RETIRED

    def test_manager_by_position(self):
        assert classify_player({"team": "한화", "position": "감독"}) == PlayerCategory.MANAGER

    def test_coach_by_keyword(self):
        assert classify_player({"team": "LG", "position": "수비코치"}) == PlayerCategory.COACH

    def test_staff_if_team_mentions_coach(self):
        assert classify_player({"team": "두산 코치", "position": " "}) == PlayerCategory.STAFF

    def test_register_staff_manager(self):
        assert classify_player({"status_source": "register", "staff_role": "manager"}) == PlayerCategory.MANAGER

    def test_register_staff_coach(self):
        assert classify_player({"status_source": "register", "staff_role": "coach"}) == PlayerCategory.COACH

    def test_register_staff_other(self):
        assert classify_player({"status_source": "register", "staff_role": "trainer"}) == PlayerCategory.STAFF

    def test_empty_team_is_retired(self):
        assert classify_player({"team": "", "position": ""}) == PlayerCategory.RETIRED

    def test_retired_in_team(self):
        assert classify_player({"team": "retired", "position": "투수"}) == PlayerCategory.RETIRED

    def test_staff_keywords_defined(self):
        assert "코치" in STAFF_KEYWORDS
        assert "감독대행" in STAFF_KEYWORDS
