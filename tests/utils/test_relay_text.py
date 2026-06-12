"""Tests for relay_text — relay text parsing utilities."""

from src.utils.relay_text import (
    advance_pitch_count,
    compact_relay_text,
    detect_relay_event_type,
    is_relay_noise_text,
    is_relay_result_event_text,
    parse_pitch_count,
)


class TestCompactRelayText:
    def test_normal_text(self):
        assert compact_relay_text("  안타  ") == "안타"

    def test_multiple_spaces(self):
        assert compact_relay_text("좌월  1점  홈런") == "좌월 1점 홈런"

    def test_none_becomes_empty(self):
        assert compact_relay_text(None) == ""

    def test_non_string_becomes_string(self):
        assert compact_relay_text(123) == "123"


class TestParsePitchCount:
    def test_ball(self):
        result = parse_pitch_count("1구 볼")
        assert result["balls"] == 1
        assert result["strikes"] == 0

    def test_strike(self):
        result = parse_pitch_count("2구 스트라이크")
        assert result["balls"] == 0
        assert result["strikes"] == 1

    def test_foul(self):
        result = parse_pitch_count("3구 파울")
        assert result["balls"] == 0
        assert result["strikes"] == 1

    def test_swing_miss(self):
        result = parse_pitch_count("4구 헛스윙")
        assert result["balls"] == 0
        assert result["strikes"] == 1

    def test_wild_pitch(self):
        result = parse_pitch_count("5구 폭투")
        assert result["balls"] == 1
        assert result["strikes"] == 0

    def test_passed_ball(self):
        result = parse_pitch_count("6구 포일")
        # 포일 is matched by regex but not handled as ball currently
        assert result["balls"] is None
        assert result["strikes"] is None

    def test_not_a_pitch(self):
        result = parse_pitch_count("안타")
        assert result["balls"] is None
        assert result["strikes"] is None

    def test_empty_text(self):
        result = parse_pitch_count("")
        assert result["balls"] is None
        assert result["strikes"] is None


class TestAdvancePitchCount:
    def test_ball_advances(self):
        balls, strikes, matched = advance_pitch_count("1구 볼", 0, 0)
        assert balls == 1
        assert strikes == 0
        assert matched

    def test_strike_advances(self):
        balls, strikes, matched = advance_pitch_count("1구 스트라이크", 1, 1)
        assert balls == 1
        assert strikes == 2
        assert matched

    def test_foul_caps_at_two_strikes(self):
        balls, strikes, matched = advance_pitch_count("1구 파울", 0, 2)
        assert balls == 0
        assert strikes == 2  # doesn't go past 2
        assert matched

    def test_foul_from_zero(self):
        balls, strikes, matched = advance_pitch_count("1구 파울", 0, 0)
        assert balls == 0
        assert strikes == 1
        assert matched

    def test_whiff_caps_at_three_strikes(self):
        balls, strikes, matched = advance_pitch_count("1구 헛스윙", 0, 2)
        assert balls == 0
        assert strikes == 3
        assert matched

    def test_ball_caps_at_four(self):
        balls, strikes, matched = advance_pitch_count("1구 볼", 3, 0)
        assert balls == 4
        assert strikes == 0
        assert matched

    def test_non_pitch_text_no_match(self):
        balls, strikes, matched = advance_pitch_count("안타", 0, 0)
        assert balls == 0
        assert strikes == 0
        assert not matched

    def test_empty_text_no_match(self):
        balls, strikes, matched = advance_pitch_count("", 0, 0)
        assert not matched


class TestIsRelayNoiseText:
    def test_game_start_noise(self):
        assert is_relay_noise_text("경기 시작")

    def test_game_end_noise(self):
        assert is_relay_noise_text("경기 종료")

    def test_pitcher_change_noise(self):
        assert is_relay_noise_text("투수 교체")

    def test_rain_delay(self):
        assert is_relay_noise_text("우천")

    def test_pitch_description_not_noise(self):
        assert not is_relay_noise_text("안타")

    def test_empty_text_is_noise(self):
        assert is_relay_noise_text("")

    def test_equals_line_noise(self):
        assert is_relay_noise_text("========")

    def test_inning_header_noise(self):
        assert is_relay_noise_text("1회초 공격")

    def test_batter_number_noise(self):
        assert is_relay_noise_text("1번타자 홍길동")


class TestIsRelayResultEventText:
    def test_hit_event(self):
        assert is_relay_result_event_text("result: 안타")

    def test_homerun_event(self):
        assert is_relay_result_event_text("result: 좌월 홈런")

    def test_out_event(self):
        assert is_relay_result_event_text("result: 삼진")

    def test_walk_event(self):
        assert is_relay_result_event_text("result: 볼넷")

    def test_noise_text_not_result(self):
        assert not is_relay_result_event_text("경기 시작")

    def test_no_colon_not_result(self):
        assert not is_relay_result_event_text("안타")

    def test_substitution_not_result(self):
        assert not is_relay_result_event_text("1구 볼: 대타 교체")


class TestDetectRelayEventType:
    def test_batting_result(self):
        assert detect_relay_event_type("result: 안타") == "batting"

    def test_steal(self):
        assert detect_relay_event_type("result: 도루") == "steal"

    def test_runner_advance(self):
        assert detect_relay_event_type("result: 진루") == "runner_advance"

    def test_runner_out(self):
        assert detect_relay_event_type("result: 주루사") == "runner_out"

    def test_unknown_noise(self):
        assert detect_relay_event_type("경기 시작") == "unknown"
