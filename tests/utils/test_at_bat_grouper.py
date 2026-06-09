"""Tests for at_bat_grouper — PBP event grouping into at-bats."""

from src.utils.at_bat_grouper import (
    ROLE_AT_BAT_PITCH,
    ROLE_AT_BAT_RESULT,
    ROLE_RUNNER_ADVANCE,
    ROLE_RUNNER_OUT,
    ROLE_STOLEN_BASE,
    ROLE_UNKNOWN,
    compute_at_bat_pitch_count,
    group_events_into_at_bats,
)


class TestGroupEventsIntoAtBats:
    def test_empty_events(self):
        assert group_events_into_at_bats([]) == []

    def test_single_event(self):
        events = [{"batter_name": "홍길동", "inning": 1, "inning_half": "top", "event_type": "batting", "description": "안타"}]
        result = group_events_into_at_bats(events)
        assert result[0]["at_bat_seq"] == 1
        assert result[0]["at_bat_event_role"] == ROLE_AT_BAT_RESULT
        assert result[0]["at_bat_confidence"] == "high"

    def test_two_at_bats_same_inning_different_batters(self):
        events = [
            {"batter_name": "홍길동", "inning": 1, "inning_half": "top", "event_type": "batting", "description": "안타"},
            {"batter_name": "김철수", "inning": 1, "inning_half": "top", "event_type": "batting", "description": "삼진"},
        ]
        result = group_events_into_at_bats(events)
        assert result[0]["at_bat_seq"] == 1
        assert result[1]["at_bat_seq"] == 2

    def test_steal_event_not_terminal(self):
        events = [
            {"batter_name": "홍길동", "inning": 1, "inning_half": "top", "event_type": "steal", "description": "도루"},
            {"batter_name": "홍길동", "inning": 1, "inning_half": "top", "event_type": "batting", "description": "안타"},
        ]
        result = group_events_into_at_bats(events)
        assert result[0]["at_bat_seq"] == 1
        assert result[0]["at_bat_event_role"] == ROLE_STOLEN_BASE
        # Both are same at-bat; steal does not terminate
        assert result[1]["at_bat_seq"] == 1
        assert result[1]["at_bat_event_role"] == ROLE_AT_BAT_RESULT

    def test_runner_advance_role(self):
        events = [{"batter_name": "홍길동", "inning": 1, "inning_half": "top", "event_type": "runner_advance", "description": "진루"}]
        result = group_events_into_at_bats(events)
        assert result[0]["at_bat_event_role"] == ROLE_RUNNER_ADVANCE

    def test_runner_out_role(self):
        events = [{"batter_name": "", "inning": 1, "inning_half": "top", "event_type": "runner_out", "description": "주루사"}]
        result = group_events_into_at_bats(events)
        assert result[0]["at_bat_event_role"] == ROLE_RUNNER_OUT
        assert result[0]["at_bat_confidence"] == "medium"

    def test_unknown_event_type(self):
        events = [{"batter_name": "홍길동", "inning": 1, "inning_half": "top", "event_type": "unknown", "description": "???"}]
        result = group_events_into_at_bats(events)
        assert result[0]["at_bat_event_role"] == ROLE_UNKNOWN

    def test_inning_boundary_creates_new_at_bat(self):
        events = [
            {"batter_name": "홍길동", "inning": 1, "inning_half": "top", "event_type": "batting", "description": "안타"},
            {"batter_name": "홍길동", "inning": 1, "inning_half": "bottom", "event_type": "batting", "description": "삼진"},
        ]
        result = group_events_into_at_bats(events)
        assert result[0]["at_bat_seq"] == 1
        assert result[1]["at_bat_seq"] == 2

    def test_pitch_count_event_role(self):
        events = [
            {"batter_name": "홍길동", "inning": 1, "inning_half": "top", "event_type": "batting", "description": "1구 볼"},
            {"batter_name": "홍길동", "inning": 1, "inning_half": "top", "event_type": "batting", "description": "2구 파울"},
            {"batter_name": "홍길동", "inning": 1, "inning_half": "top", "event_type": "batting", "description": "3구 스트라이크: 헛스윙 삼진"},
        ]
        result = group_events_into_at_bats(events)
        # First two are pitch-count events (only "n구" text, no result keywords)
        assert result[0]["at_bat_event_role"] == ROLE_AT_BAT_PITCH
        assert result[1]["at_bat_event_role"] == ROLE_AT_BAT_PITCH
        # Third has result keyword
        assert result[2]["at_bat_event_role"] == ROLE_AT_BAT_RESULT

    def test_batter_name_from_batter_key(self):
        events = [{"batter": "홍길동", "inning": 1, "inning_half": "top", "event_type": "batting", "description": "안타"}]
        result = group_events_into_at_bats(events)
        assert result[0]["at_bat_seq"] == 1


class TestComputeAtBatPitchCount:
    def test_single_pitch(self):
        events = [
            {"batter_name": "홍길동", "inning": 1, "inning_half": "top", "description": "1구 볼", "at_bat_seq": 1},
        ]
        result = compute_at_bat_pitch_count(events)
        assert result[0]["balls"] == 1
        assert result[0]["strikes"] == 0

    def test_accumulates_across_at_bat(self):
        events = [
            {"batter_name": "홍길동", "inning": 1, "inning_half": "top", "description": "1구 볼", "at_bat_seq": 1},
            {"batter_name": "홍길동", "inning": 1, "inning_half": "top", "description": "2구 스트라이크", "at_bat_seq": 1},
            {"batter_name": "홍길동", "inning": 1, "inning_half": "top", "description": "3구 파울", "at_bat_seq": 1},
        ]
        result = compute_at_bat_pitch_count(events)
        assert result[0]["balls"] == 1
        assert result[0]["strikes"] == 0
        assert result[1]["balls"] == 1
        assert result[1]["strikes"] == 1
        assert result[2]["balls"] == 1
        assert result[2]["strikes"] == 2

    def test_resets_between_at_bats(self):
        events = [
            {"batter_name": "홍길동", "inning": 1, "inning_half": "top", "description": "1구 볼", "at_bat_seq": 1},
            {"batter_name": "김철수", "inning": 1, "inning_half": "top", "description": "1구 스트라이크", "at_bat_seq": 2},
        ]
        result = compute_at_bat_pitch_count(events)
        assert result[0]["balls"] == 1
        assert result[0]["strikes"] == 0
        assert result[1]["balls"] == 0
        assert result[1]["strikes"] == 1

    def test_skips_missing_at_bat_seq(self):
        events = [{"batter_name": "홍길동", "description": "안타"}]
        result = compute_at_bat_pitch_count(events)
        assert "balls" not in result[0]

    def test_preset_balls_strikes(self):
        events = [
            {"batter_name": "홍길동", "inning": 1, "inning_half": "top", "description": "안타", "at_bat_seq": 1, "balls": 2, "strikes": 1},
        ]
        result = compute_at_bat_pitch_count(events)
        assert result[0]["balls"] == 2
        assert result[0]["strikes"] == 1

    def test_non_pitch_text_does_not_change_count(self):
        events = [
            {"batter_name": "홍길동", "inning": 1, "inning_half": "top", "description": "안타", "at_bat_seq": 1},
        ]
        result = compute_at_bat_pitch_count(events)
        assert result[0]["balls"] == 0
        assert result[0]["strikes"] == 0
