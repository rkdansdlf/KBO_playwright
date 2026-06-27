from __future__ import annotations

from src.utils.at_bat_grouper import (
    AT_BAT_TERMINAL_EVENTS,
    ROLE_AT_BAT_PITCH,
    ROLE_AT_BAT_RESULT,
    ROLE_AT_BAT_START,
    ROLE_RUNNER_ADVANCE,
    ROLE_RUNNER_OUT,
    ROLE_STOLEN_BASE,
    ROLE_UNKNOWN,
    AtBatContext,
    _event_role,
    _needs_new_at_bat,
    compute_at_bat_pitch_count,
    group_events_into_at_bats,
)


class TestGroupEventsIntoAtBats:
    def test_empty_list(self):
        assert group_events_into_at_bats([]) == []

    def test_single_event(self):
        events = [
            {
                "batter_name": "홍길동",
                "inning": 1,
                "inning_half": "top",
                "event_type": "batting",
                "description": "홍길동: 안타",
            }
        ]
        result = group_events_into_at_bats(events)
        assert len(result) == 1
        assert result[0]["at_bat_seq"] == 1
        assert result[0]["at_bat_confidence"] == "high"

    def test_different_batter_new_at_bat(self):
        events = [
            {
                "batter_name": "홍길동",
                "inning": 1,
                "inning_half": "top",
                "event_type": "batting",
                "description": "홍길동: 안타",
            },
            {
                "batter_name": "김철수",
                "inning": 1,
                "inning_half": "top",
                "event_type": "batting",
                "description": "김철수: 삼진",
            },
        ]
        result = group_events_into_at_bats(events)
        assert result[0]["at_bat_seq"] == 1
        assert result[1]["at_bat_seq"] == 2

    def test_inning_change_new_at_bat(self):
        events = [
            {
                "batter_name": "홍길동",
                "inning": 1,
                "inning_half": "top",
                "event_type": "batting",
                "description": "홍길동: 안타",
            },
            {
                "batter_name": "홍길동",
                "inning": 2,
                "inning_half": "top",
                "event_type": "batting",
                "description": "홍길동: 안타",
            },
        ]
        result = group_events_into_at_bats(events)
        assert result[0]["at_bat_seq"] == 1
        assert result[1]["at_bat_seq"] == 2

    def test_result_causes_new_at_bat(self):
        events = [
            {
                "batter_name": "홍길동",
                "inning": 1,
                "inning_half": "top",
                "event_type": "batting",
                "description": "홍길동: 안타",
            },
            {
                "batter_name": "홍길동",
                "inning": 1,
                "inning_half": "top",
                "event_type": "batting",
                "description": "홍길동: 2루타",
            },
        ]
        result = group_events_into_at_bats(events)
        assert result[0]["at_bat_seq"] == 1
        assert result[1]["at_bat_seq"] == 2

    def test_no_batter_name_medium_confidence(self):
        events = [
            {
                "batter_name": "",
                "inning": 1,
                "inning_half": "top",
                "event_type": "unknown",
                "description": "타자 등판",
            }
        ]
        result = group_events_into_at_bats(events)
        assert result[0]["at_bat_confidence"] == "medium"

    def test_fallback_to_batter_key(self):
        events = [
            {
                "batter": "김철수",
                "inning": 1,
                "inning_half": "top",
                "event_type": "batting",
                "description": "안타",
            }
        ]
        result = group_events_into_at_bats(events)
        assert result[0]["at_bat_confidence"] == "high"

    def test_groups_across_innings(self):
        events = [
            {
                "batter_name": "A",
                "inning": 1,
                "inning_half": "top",
                "event_type": "batting",
                "description": "A: 안타",
            },
            {
                "batter_name": "B",
                "inning": 1,
                "inning_half": "top",
                "event_type": "batting",
                "description": "B: 홈런",
            },
            {
                "batter_name": "A",
                "inning": 1,
                "inning_half": "bottom",
                "event_type": "batting",
                "description": "A: 도루",
            },
            {
                "batter_name": "A",
                "inning": 2,
                "inning_half": "top",
                "event_type": "batting",
                "description": "A: 안타",
            },
        ]
        result = group_events_into_at_bats(events)
        assert result[0]["at_bat_seq"] == 1
        assert result[1]["at_bat_seq"] == 2
        assert result[2]["at_bat_seq"] == 3
        assert result[3]["at_bat_seq"] == 4


class TestNeedsNewAtBat:
    def test_first_event_no_current(self):
        ctx = AtBatContext(
            current_batter_key=None,
            inning=1,
            half="top",
            batter_name="홍길동",
            current_batter=None,
            has_seen_result_this_at_bat=False,
            event_type="batting",
        )
        assert _needs_new_at_bat(ctx=ctx) is True

    def test_half_change(self):
        ctx = AtBatContext(
            current_batter_key=(1, "top", "홍길동"),
            inning=1,
            half="bottom",
            batter_name="홍길동",
            current_batter="홍길동",
            has_seen_result_this_at_bat=False,
            event_type="batting",
        )
        assert _needs_new_at_bat(ctx=ctx) is True

    def test_same_batter_same_half(self):
        ctx = AtBatContext(
            current_batter_key=(1, "top", "홍길동"),
            inning=1,
            half="top",
            batter_name="홍길동",
            current_batter="홍길동",
            has_seen_result_this_at_bat=False,
            event_type="batting",
        )
        assert _needs_new_at_bat(ctx=ctx) is False

    def test_result_seen_terminal_event(self):
        ctx = AtBatContext(
            current_batter_key=(1, "top", "홍길동"),
            inning=1,
            half="top",
            batter_name="홍길동",
            current_batter="홍길동",
            has_seen_result_this_at_bat=True,
            event_type="batting",
        )
        assert _needs_new_at_bat(ctx=ctx) is True

    def test_different_batter_same_half(self):
        ctx = AtBatContext(
            current_batter_key=(1, "top", "홍길동"),
            inning=1,
            half="top",
            batter_name="김철수",
            current_batter="홍길동",
            has_seen_result_this_at_bat=False,
            event_type="batting",
        )
        assert _needs_new_at_bat(ctx=ctx) is True


class TestEventRole:
    def test_batting_with_pitch_keyword(self):
        assert _event_role("batting", "3구 볼") == ROLE_AT_BAT_PITCH

    def test_batting_result(self):
        assert _event_role("batting", "5구 스트라이크 안타") == ROLE_AT_BAT_RESULT

    def test_steal_event(self):
        assert _event_role("steal", "도루 성공") == ROLE_STOLEN_BASE

    def test_runner_advance_event(self):
        assert _event_role("runner_advance", "1루 진루") == ROLE_RUNNER_ADVANCE

    def test_runner_out_event(self):
        assert _event_role("runner_out", "주루사 아웃") == ROLE_RUNNER_OUT

    def test_unknown_event(self):
        assert _event_role("unknown", "파울") == ROLE_UNKNOWN

    def test_batting_with_special_characters(self):
        assert _event_role("batting", "7구 스트라이 안타") == ROLE_AT_BAT_RESULT

    def test_batting_not_a_pitch(self):
        assert _event_role("batting", "안타") == ROLE_AT_BAT_RESULT


class TestComputeAtBatPitchCount:
    def test_basic_counting(self):
        events = [
            {
                "at_bat_seq": 1,
                "description": "1구 볼",
                "balls": None,
                "strikes": None,
            },
            {
                "at_bat_seq": 1,
                "description": "2구 스트라이크",
                "balls": None,
                "strikes": None,
            },
        ]
        compute_at_bat_pitch_count(events)
        assert events[0]["balls"] == 1
        assert events[0]["strikes"] == 0
        assert events[1]["balls"] == 1
        assert events[1]["strikes"] == 1

    def test_resets_on_new_at_bat(self):
        events = [
            {
                "at_bat_seq": 1,
                "description": "1구 스트라이크",
                "balls": None,
                "strikes": None,
            },
            {
                "at_bat_seq": 2,
                "description": "1구 볼",
                "balls": None,
                "strikes": None,
            },
        ]
        compute_at_bat_pitch_count(events)
        assert events[1]["balls"] == 1
        assert events[1]["strikes"] == 0

    def test_preset_values(self):
        events = [
            {
                "at_bat_seq": 1,
                "description": "",
                "balls": 2,
                "strikes": 1,
            },
        ]
        compute_at_bat_pitch_count(events)
        assert events[0]["balls"] == 2
        assert events[0]["strikes"] == 1

    def test_invalid_preset_values(self):
        events = [
            {
                "at_bat_seq": 1,
                "description": "",
                "balls": "bad",
                "strikes": "bad",
            },
        ]
        compute_at_bat_pitch_count(events)

    def test_no_at_bat_skipped(self):
        events = [
            {
                "description": "1구 볼",
            },
        ]
        compute_at_bat_pitch_count(events)
        assert "balls" not in events[0]
