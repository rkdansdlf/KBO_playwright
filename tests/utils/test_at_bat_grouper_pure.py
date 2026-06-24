from __future__ import annotations

from src.utils.at_bat_grouper import (
    AT_BAT_TERMINAL_EVENTS,
    ROLE_AT_BAT_PITCH,
    ROLE_AT_BAT_RESULT,
    ROLE_RUNNER_ADVANCE,
    ROLE_RUNNER_OUT,
    ROLE_STOLEN_BASE,
    ROLE_UNKNOWN,
    _event_role,
    _needs_new_at_bat,
)


class TestNeedsNewAtBat:
    def test_same_inning_half_same_batter(self):
        result = _needs_new_at_bat(
            current_batter_key=(1, "top", "A"),
            inning=1,
            half="top",
            batter_name="김하성",
            current_batter="김하성",
            has_seen_result_this_at_bat=False,
            event_type="batting",
        )
        assert result is False

    def test_different_inning(self):
        result = _needs_new_at_bat(
            current_batter_key=(1, "top", "A"),
            inning=2,
            half="top",
            batter_name="김하성",
            current_batter="김하성",
            has_seen_result_this_at_bat=False,
            event_type="batting",
        )
        assert result is True

    def test_different_batter(self):
        result = _needs_new_at_bat(
            current_batter_key=(1, "top", "A"),
            inning=1,
            half="top",
            batter_name="박병호",
            current_batter="김하성",
            has_seen_result_this_at_bat=False,
            event_type="batting",
        )
        assert result is True

    def test_terminal_event_with_result(self):
        result = _needs_new_at_bat(
            current_batter_key=(1, "top", "A"),
            inning=1,
            half="top",
            batter_name="김하성",
            current_batter="김하성",
            has_seen_result_this_at_bat=True,
            event_type="batting",
        )
        assert result is True

    def test_no_current_batter_with_name(self):
        result = _needs_new_at_bat(
            current_batter_key=None,
            inning=1,
            half="top",
            batter_name="김하성",
            current_batter=None,
            has_seen_result_this_at_bat=False,
            event_type="batting",
        )
        assert result is True

    def test_none_batter_name(self):
        result = _needs_new_at_bat(
            current_batter_key=(1, "top", "A"),
            inning=1,
            half="top",
            batter_name="",
            current_batter="김하성",
            has_seen_result_this_at_bat=False,
            event_type="runner_advance",
        )
        assert result is False


class TestEventRole:
    def test_batting_result(self):
        assert _event_role("batting", "안타") == ROLE_AT_BAT_RESULT

    def test_batting_pitch(self):
        assert _event_role("batting", "1구 스트라이크") == ROLE_AT_BAT_PITCH

    def test_batting_hr(self):
        assert _event_role("batting", "홈런") == ROLE_AT_BAT_RESULT

    def test_steal(self):
        assert _event_role("steal", "도루") == ROLE_STOLEN_BASE

    def test_runner_advance(self):
        assert _event_role("runner_advance", "진루") == ROLE_RUNNER_ADVANCE

    def test_runner_out(self):
        assert _event_role("runner_out", "주루사") == ROLE_RUNNER_OUT

    def test_unknown_type(self):
        assert _event_role("unknown", "text") == ROLE_UNKNOWN


class TestAtBatTerminalEvents:
    def test_batting_in_terminal(self):
        assert "batting" in AT_BAT_TERMINAL_EVENTS

    def test_steal_not_in_terminal(self):
        assert "steal" not in AT_BAT_TERMINAL_EVENTS
