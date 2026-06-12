from datetime import date

from src.utils.schedule_validation import (
    is_detail_candidate_game,
    parse_schedule_date,
    split_schedule_game_id,
    validate_schedule_game_payload,
)


class TestParseScheduleDate:
    def test_valid_date(self):
        assert parse_schedule_date("20250415") == date(2025, 4, 15)

    def test_valid_date_with_dashes(self):
        assert parse_schedule_date("2025-04-15") == date(2025, 4, 15)

    def test_invalid_length(self):
        assert parse_schedule_date("2025") is None

    def test_none_input(self):
        assert parse_schedule_date(None) is None

    def test_non_digit(self):
        assert parse_schedule_date("2025abcd") is None

    def test_invalid_date(self):
        assert parse_schedule_date("20250230") is None


class TestSplitScheduleGameId:
    def test_valid_game_id(self):
        result = split_schedule_game_id("20250415LGSS0")
        assert result is not None
        date_part, away, home, dh = result
        assert date_part == "20250415"
        assert away == "LG"
        assert home == "SS"
        assert dh == "0"

    def test_valid_with_legacy_code(self):
        result = split_schedule_game_id("20250415SKLG0")
        assert result is not None
        assert result[1] == "SK"
        assert result[2] == "LG"

    def test_none_input(self):
        assert split_schedule_game_id(None) is None

    def test_empty_string(self):
        assert split_schedule_game_id("") is None

    def test_invalid_format(self):
        assert split_schedule_game_id("abc") is None


class TestValidateScheduleGamePayload:
    def test_valid_game(self):
        game = {
            "game_id": "20250415LGSS0",
            "game_date": "20250415",
            "away_team_code": "LG",
            "home_team_code": "SS",
            "game_status": "SCHEDULED",
            "stadium": "Jamsil",
        }
        assert validate_schedule_game_payload(game) == (True, None)

    def test_missing_game_id(self):
        assert validate_schedule_game_payload({"game_date": "20250415"}) == (False, "missing_game_id")

    def test_invalid_game_date(self):
        assert validate_schedule_game_payload({"game_id": "20250415LGSS0", "game_date": "bad"}) == (
            False,
            "invalid_game_date",
        )

    def test_expected_year_mismatch(self):
        game = {
            "game_id": "20250415LGSS0",
            "game_date": "20250415",
            "away_team_code": "LG",
            "home_team_code": "SS",
            "game_status": "SCHEDULED",
            "stadium": "Jamsil",
        }
        assert validate_schedule_game_payload(game, expected_year=2026) == (False, "schedule_date_mismatch")

    def test_missing_team_code(self):
        game = {
            "game_id": "20250415LGSS0",
            "game_date": "20250415",
            "away_team_code": "",
            "home_team_code": "SS",
            "game_status": "SCHEDULED",
            "stadium": "Jamsil",
        }
        assert validate_schedule_game_payload(game) == (False, "missing_away_team_code")

    def test_invalid_game_status(self):
        game = {
            "game_id": "20250415LGSS0",
            "game_date": "20250415",
            "away_team_code": "LG",
            "home_team_code": "SS",
            "game_status": "INVALID",
            "stadium": "Jamsil",
        }
        assert validate_schedule_game_payload(game) == (False, "invalid_game_status")

    def test_missing_stadium(self):
        game = {
            "game_id": "20250415LGSS0",
            "game_date": "20250415",
            "away_team_code": "LG",
            "home_team_code": "SS",
            "game_status": "SCHEDULED",
        }
        assert validate_schedule_game_payload(game) == (False, "missing_stadium")


class TestIsDetailCandidateGame:
    def test_future_game_not_candidate(self):
        today = date(2025, 4, 10)
        assert is_detail_candidate_game({"game_date": "20250415"}, today=today) is False

    def test_cancelled_game_not_candidate(self):
        today = date(2025, 4, 15)
        assert is_detail_candidate_game({"game_date": "20250415", "game_status": "CANCELLED"}, today=today) is False

    def test_completed_game_is_candidate(self):
        today = date(2025, 4, 15)
        assert is_detail_candidate_game({"game_date": "20250415", "game_status": "COMPLETED"}, today=today) is True

    def test_live_game_is_candidate(self):
        today = date(2025, 4, 15)
        assert is_detail_candidate_game({"game_date": "20250415", "game_status": "LIVE"}, today=today) is True

    def test_past_scheduled_game_is_candidate(self):
        today = date(2025, 4, 16)
        assert is_detail_candidate_game({"game_date": "20250415", "game_status": "SCHEDULED"}, today=today) is True

    def test_invalid_game_date_not_candidate(self):
        today = date(2025, 4, 15)
        assert is_detail_candidate_game({"game_date": "invalid"}, today=today) is False
