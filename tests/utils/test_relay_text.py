from __future__ import annotations

import pytest

from src.utils.relay_text import (
    advance_pitch_count,
    classify_relay_result,
    compact_relay_text,
    detect_relay_event_type,
    is_relay_noise_text,
    is_relay_result_event_text,
    parse_pitch_count,
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


class TestClassifyRelayResult:
    """classify_relay_result() — 세분화 result_code 분류 테스트."""

    def test_home_run(self) -> None:
        assert classify_relay_result("이대호: 홈런") == "HR"

    def test_single(self) -> None:
        assert classify_relay_result("박민우: 안타") == "H1"

    def test_infield_single(self) -> None:
        assert classify_relay_result("타자: 내야안타") == "IH"

    def test_double(self) -> None:
        assert classify_relay_result("나성범: 2루타") == "H2"

    def test_triple(self) -> None:
        assert classify_relay_result("타자: 3루타") == "H3"

    def test_strikeout(self) -> None:
        assert classify_relay_result("타자: 삼진") == "K"

    def test_walk(self) -> None:
        assert classify_relay_result("타자: 볼넷") == "BB"

    def test_intentional_walk(self) -> None:
        assert classify_relay_result("타자: 고의4구") == "BB"

    def test_hbp(self) -> None:
        assert classify_relay_result("타자: 몸에 맞는 볼") == "HBP"

    def test_fielders_choice(self) -> None:
        assert classify_relay_result("타자: 야수선택") == "FC"

    def test_sacrifice_hit(self) -> None:
        assert classify_relay_result("타자: 희생번트") == "SH"

    def test_sacrifice_fly(self) -> None:
        assert classify_relay_result("타자: 희생플라이") == "SF"

    def test_error(self) -> None:
        assert classify_relay_result("타자: 실책") == "E"

    def test_reached_on_error(self) -> None:
        assert classify_relay_result("타자: 실책출루") == "ROE"

    def test_groundout(self) -> None:
        assert classify_relay_result("타자: 땅볼") == "GO"

    def test_double_play(self) -> None:
        assert classify_relay_result("타자: 병살") == "DP"

    def test_noise_returns_none(self) -> None:
        assert classify_relay_result("마운드 방문") is None

    def test_empty_returns_none(self) -> None:
        assert classify_relay_result("") is None

    def test_none_returns_none(self) -> None:
        assert classify_relay_result(None) is None

    @pytest.mark.parametrize(
        "text,expected",
        [
            ("타자: 안타", "H1"),
            ("타자: 홈런", "HR"),
            ("타자: 삼진", "K"),
            ("타자: 볼넷", "BB"),
            ("타자: 야수선택", "FC"),
            ("타자: 희번", "SH"),
            ("타자: 희플", "SF"),
        ],
    )
    def test_parametrized_common_results(self, text: str, expected: str) -> None:
        assert classify_relay_result(text) == expected
