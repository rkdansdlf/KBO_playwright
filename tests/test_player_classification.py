from src.utils.player_classification import classify_player, PlayerCategory


def test_classify_active_defaults():
    entry = {"team": "LG", "position": "내야수"}
    assert classify_player(entry) == PlayerCategory.ACTIVE


def test_classify_pitcher_still_active():
    entry = {"team": "두산", "position": "투수"}
    assert classify_player(entry) == PlayerCategory.ACTIVE


def test_classify_retired_by_team_keyword():
    entry = {"team": "은퇴", "position": "포수"}
    assert classify_player(entry) == PlayerCategory.RETIRED


def test_classify_manager():
    entry = {"team": "한화", "position": "감독"}
    assert classify_player(entry) == PlayerCategory.MANAGER


def test_classify_coach_keyword():
    entry = {"team": "LG", "position": "수비코치"}
    assert classify_player(entry) == PlayerCategory.COACH


def test_classify_staff_if_team_mentions_coach():
    entry = {"team": "두산 코치", "position": " "}
    assert classify_player(entry) == PlayerCategory.STAFF
