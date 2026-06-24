from __future__ import annotations

from src.utils.relay_text import (
    compact_relay_text,
    parse_pitch_count,
    advance_pitch_count,
    is_relay_noise_text,
    is_relay_result_event_text,
    detect_relay_event_type,
)


class TestCompactRelayText:
    def test_basic(self):
        assert compact_relay_text("안타") == "안타"

    def test_whitespace(self):
        assert compact_relay_text("  안타  홈런  ") == "안타 홈런"

    def test_none(self):
        assert compact_relay_text(None) == ""

    def test_empty(self):
        assert compact_relay_text("") == ""


class TestParsePitchCount:
    def test_ball(self):
        result = parse_pitch_count("5구 볼")
        assert result == {"balls": 1, "strikes": 0}

    def test_strike(self):
        result = parse_pitch_count("3구 스트라이크")
        assert result == {"balls": 0, "strikes": 1}

    def test_foul(self):
        result = parse_pitch_count("2구 파울")
        assert result == {"balls": 0, "strikes": 1}

    def test_heotswing(self):
        result = parse_pitch_count("4구 헛스윙")
        assert result == {"balls": 0, "strikes": 1}

    def test_poktu(self):
        result = parse_pitch_count("1구 폭투")
        assert result == {"balls": 1, "strikes": 0}

    def test_no_match(self):
        result = parse_pitch_count("안타")
        assert result == {"balls": None, "strikes": None}

    def test_empty(self):
        result = parse_pitch_count("")
        assert result == {"balls": None, "strikes": None}


class TestAdvancePitchCount:
    def test_ball(self):
        b, s, matched = advance_pitch_count("5구 볼", 2, 1)
        assert (b, s, matched) == (3, 1, True)

    def test_strike(self):
        b, s, matched = advance_pitch_count("3구 스트라이크", 1, 1)
        assert (b, s, matched) == (1, 2, True)

    def test_foul_at_two_strikes(self):
        b, s, matched = advance_pitch_count("5구 파울", 0, 2)
        assert (b, s, matched) == (0, 2, True)

    def test_foul_at_one_strike(self):
        b, s, matched = advance_pitch_count("4구 파울", 1, 1)
        assert (b, s, matched) == (1, 2, True)

    def test_no_match(self):
        b, s, matched = advance_pitch_count("안타", 2, 1)
        assert (b, s, matched) == (2, 1, False)


class TestIsRelayNoiseText:
    def test_empty(self):
        assert is_relay_noise_text("") is True

    def test_inning_header(self):
        assert is_relay_noise_text("1회 초 공격") is True

    def test_batter_header(self):
        assert is_relay_noise_text("1번타자 홍길동") is True

    def test_game_start(self):
        assert is_relay_noise_text("경기 시작") is True

    def test_game_end(self):
        assert is_relay_noise_text("경기 종료") is True

    def test_substitution(self):
        assert is_relay_noise_text("교체") is True

    def test_noise_pattern_equals(self):
        assert is_relay_noise_text("===") is True

    def test_noise_pitch_clock(self):
        assert is_relay_noise_text("피치클락 위반") is True


class TestIsRelayResultEventText:
    def test_noise(self):
        assert is_relay_result_event_text("경기 종료") is False

    def test_no_colon(self):
        assert is_relay_result_event_text("안타") is False

    def test_valid_hit(self):
        assert is_relay_result_event_text("1번타자: 안타") is True

    def test_valid_home_run(self):
        assert is_relay_result_event_text("홍길동: 홈런") is True

    def test_substitution(self):
        assert is_relay_result_event_text("홍길동: 교체") is False


class TestDetectRelayEventType:
    def test_unknown(self):
        assert detect_relay_event_type("경기 종료") == "unknown"

    def test_steal(self):
        assert detect_relay_event_type("홍길동: 도루") == "steal"

    def test_runner_advance(self):
        assert detect_relay_event_type("홍길동: 홈인") == "runner_advance"

    def test_runner_out(self):
        assert detect_relay_event_type("홍길동: 주루사") == "runner_out"

    def test_batting(self):
        assert detect_relay_event_type("홍길동: 안타") == "batting"

    def test_advance_on_error(self):
        assert detect_relay_event_type("홍길동: 실책") == "batting"
