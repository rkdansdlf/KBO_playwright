from __future__ import annotations

from src.utils.relay_text import (
    RELAY_RESULT_KEYWORDS,
    advance_pitch_count,
    compact_relay_text,
    detect_relay_event_type,
    is_relay_noise_text,
    is_relay_result_event_text,
    parse_pitch_count,
)


class TestCompactRelayTextExtended:
    def test_newlines(self):
        assert compact_relay_text("안타\n홈런") == "안타 홈런"

    def test_tabs(self):
        assert compact_relay_text("안타\t홈런") == "안타 홈런"

    def test_multiple_spaces(self):
        assert compact_relay_text("안타    홈런") == "안타 홈런"

    def test_integer_input(self):
        assert compact_relay_text(123) == "123"


class TestParsePitchCountExtended:
    def test_poil(self):
        result = parse_pitch_count("1구 포일")
        assert result == {"balls": None, "strikes": None}

    def test_extra_text_after(self):
        result = parse_pitch_count("5구 볼 다음타자")
        assert result == {"balls": 1, "strikes": 0}

    def test_no_number(self):
        result = parse_pitch_count("구 볼")
        assert result == {"balls": None, "strikes": None}


class TestAdvancePitchCountExtended:
    def test_ball_at_full_count(self):
        b, s, matched = advance_pitch_count("5구 볼", 3, 2)
        assert (b, s, matched) == (4, 2, True)

    def test_strike_at_three(self):
        b, s, matched = advance_pitch_count("5구 스트라이크", 1, 2)
        assert (b, s, matched) == (1, 3, True)

    def test_foul_at_zero_strikes(self):
        b, s, matched = advance_pitch_count("3구 파울", 2, 0)
        assert (b, s, matched) == (2, 1, True)

    def test_foul_at_one_strike(self):
        b, s, matched = advance_pitch_count("3구 파울", 2, 1)
        assert (b, s, matched) == (2, 2, True)

    def test_no_match_preserves_state(self):
        b, s, matched = advance_pitch_count("안타", 2, 1)
        assert (b, s, matched) == (2, 1, False)

    def test_empty_string(self):
        b, s, matched = advance_pitch_count("", 0, 0)
        assert (b, s, matched) == (0, 0, False)

    def test_sequence_of_pitches(self):
        balls, strikes = 0, 0
        balls, strikes, _ = advance_pitch_count("1구 볼", balls, strikes)
        balls, strikes, _ = advance_pitch_count("2구 스트라이크", balls, strikes)
        balls, strikes, _ = advance_pitch_count("3구 볼", balls, strikes)
        balls, strikes, _ = advance_pitch_count("4구 파울", balls, strikes)
        assert (balls, strikes) == (2, 2)


class TestIsRelayNoiseTextExtended:
    def test_game_preparation(self):
        assert is_relay_noise_text("경기 준비중") is True

    def test_game_suspended(self):
        assert is_relay_noise_text("경기 중단") is True

    def test_game_resumed(self):
        assert is_relay_noise_text("경기 재개") is True

    def test_game_canceled(self):
        assert is_relay_noise_text("경기 취소") is True

    def test_mound_visit(self):
        assert is_relay_noise_text("마운드 방문") is True

    def test_video_review(self):
        assert is_relay_noise_text("비디오 판독") is True

    def test_substitution_token(self):
        assert is_relay_noise_text("대타") is True

    def test_hitter_token(self):
        assert is_relay_noise_text("대타 홍길동") is True

    def test_cold_game(self):
        assert is_relay_noise_text("콜드게임") is True

    def test_suspended_token(self):
        assert is_relay_noise_text("서스펜디드") is True

    def test_mvp_token(self):
        assert is_relay_noise_text("MVP") is True

    def test_field_maintenance(self):
        assert is_relay_noise_text("그라운드 정비") is True

    def test_rain(self):
        assert is_relay_noise_text("우천") is True


class TestIsRelayResultEventTextExtended:
    def test_noise_with_colon(self):
        assert is_relay_result_event_text("승리투수: 김철수") is False

    def test_substitution_with_colon(self):
        assert is_relay_result_event_text("홍길동: 교체") is False

    def test_noise_token_with_colon(self):
        assert is_relay_result_event_text("경기 시작: 안타") is False

    def test_valid_strikeout(self):
        assert is_relay_result_event_text("홍길동: 삼진") is True

    def test_valid_walk(self):
        assert is_relay_result_event_text("홍길동: 볼넷") is True

    def test_valid_double_play(self):
        assert is_relay_result_event_text("홍길동: 병살") is True

    def test_valid_sacrifice(self):
        assert is_relay_result_event_text("홍길동: 희생") is True

    def test_valid_error(self):
        assert is_relay_result_event_text("홍길동: 실책") is True

    def test_valid_steal(self):
        assert is_relay_result_event_text("홍길동: 도루") is True


class TestDetectRelayEventTypeExtended:
    def test_noise_returns_unknown(self):
        assert detect_relay_event_type("경기 준비중") == "unknown"

    def test_empty_returns_unknown(self):
        assert detect_relay_event_type("") == "unknown"

    def test_none_returns_unknown(self):
        assert detect_relay_event_type(None) == "unknown"

    def test_steal_with_advance(self):
        assert detect_relay_event_type("홍길동: 도루 성공") == "steal"

    def test_runner_advance_on_advance(self):
        assert detect_relay_event_type("홍길동: 진루") == "runner_advance"

    def test_runner_out_on_out(self):
        assert detect_relay_event_type("홍길동: 주루사") == "runner_out"

    def test_batting_on_double(self):
        assert detect_relay_event_type("홍길동: 2루타") == "batting"

    def test_batting_on_triple(self):
        assert detect_relay_event_type("홍길동: 3루타") == "batting"

    def test_batting_on_single(self):
        assert detect_relay_event_type("홍길동: 1루타") == "batting"

    def test_batting_on_fly(self):
        assert detect_relay_event_type("홍길동: 플라이") == "batting"

    def test_batting_on_ground(self):
        assert detect_relay_event_type("홍길동: 땅볼") == "batting"

    def test_batting_on_line_drive(self):
        assert detect_relay_event_type("홍길동: 라인드라이브") == "batting"

    def test_steal_takes_priority_over_advance(self):
        assert detect_relay_event_type("홍길동: 도루 진루") == "steal"

    def test_advance_takes_priority_over_out(self):
        assert detect_relay_event_type("홍길동: 홈인 주루사") == "runner_advance"

    def test_all_result_keywords_covered(self):
        for keyword in RELAY_RESULT_KEYWORDS:
            text = f"홍길동: {keyword}"
            result = detect_relay_event_type(text)
            assert result != "unknown", f"Keyword '{keyword}' not detected"
