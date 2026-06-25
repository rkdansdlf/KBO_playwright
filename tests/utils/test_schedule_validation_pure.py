from __future__ import annotations

from src.utils.schedule_validation import (
    is_detail_candidate_game,
    parse_schedule_date,
    split_schedule_game_id,
    validate_schedule_game_payload,
)


class TestParseScheduleDate:
    def test_valid_date(self):
        result = parse_schedule_date("2026-06-25")
        assert result is not None

    def test_none(self):
        assert parse_schedule_date(None) is None

    def test_invalid(self):
        assert parse_schedule_date("not-a-date") is None


class TestSplitScheduleGameId:
    def test_valid_id(self):
        result = split_schedule_game_id("20260625LGSS0")
        assert result is not None
        assert len(result) == 4

    def test_none(self):
        assert split_schedule_game_id(None) is None

    def test_too_short(self):
        assert split_schedule_game_id("2026") is None


class TestValidateScheduleGamePayload:
    def test_empty_dict(self):
        is_valid, reason = validate_schedule_game_payload({})
        assert is_valid is False


class TestIsDetailCandidateGame:
    def test_empty_dict(self):
        result = is_detail_candidate_game({})
        assert result is False
