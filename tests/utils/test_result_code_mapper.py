"""Tests for src/utils/result_code_mapper.py."""

from __future__ import annotations

import pytest

from src.utils.result_code_mapper import (
    HIT_CODES,
    ON_BASE_CODES,
    OUT_CODES,
    enrich_result_code,
    is_hit,
    is_on_base,
    is_out,
    is_plate_appearance,
    map_korean_to_result_code,
    result_code_to_label,
)


class TestMapKoreanToResultCode:
    """map_korean_to_result_code 단위 테스트."""

    # ── 홈런 / 장타 ──────────────────────────────────────────────────────────

    def test_home_run(self) -> None:
        assert map_korean_to_result_code("홈런") == "HR"

    def test_triple(self) -> None:
        assert map_korean_to_result_code("3루타") == "H3"

    def test_double(self) -> None:
        assert map_korean_to_result_code("2루타") == "H2"

    # ── 안타 ─────────────────────────────────────────────────────────────────

    def test_single(self) -> None:
        assert map_korean_to_result_code("안타") == "H1"

    def test_single_1루타(self) -> None:
        assert map_korean_to_result_code("1루타") == "H1"

    def test_rbi_single_적시타(self) -> None:
        assert map_korean_to_result_code("적시타") == "H1"

    def test_infield_single(self) -> None:
        assert map_korean_to_result_code("내야안타") == "IH"

    # ── 삼진 ─────────────────────────────────────────────────────────────────

    def test_strikeout(self) -> None:
        assert map_korean_to_result_code("삼진") == "K"

    def test_not_strikeout_낫아웃(self) -> None:
        assert map_korean_to_result_code("낫 아웃") == "K"

    # ── 볼넷 / 사구 ───────────────────────────────────────────────────────────

    def test_walk(self) -> None:
        assert map_korean_to_result_code("볼넷") == "BB"

    def test_intentional_walk(self) -> None:
        assert map_korean_to_result_code("고의4구") == "BB"

    def test_auto_intentional_walk(self) -> None:
        assert map_korean_to_result_code("자동 고의4구") == "BB"

    def test_hbp_몸에맞는볼(self) -> None:
        assert map_korean_to_result_code("몸에 맞는 볼") == "HBP"

    def test_hbp_사구(self) -> None:
        assert map_korean_to_result_code("사구") == "HBP"

    # ── 야수선택 (FC) ─────────────────────────────────────────────────────────

    def test_fielders_choice(self) -> None:
        assert map_korean_to_result_code("야수선택") == "FC"

    # ── 희생 ─────────────────────────────────────────────────────────────────

    def test_sacrifice_hit_full(self) -> None:
        assert map_korean_to_result_code("희생번트") == "SH"

    def test_sacrifice_hit_abbrev(self) -> None:
        assert map_korean_to_result_code("희번") == "SH"

    def test_sacrifice_fly_full(self) -> None:
        assert map_korean_to_result_code("희생플라이") == "SF"

    def test_sacrifice_fly_abbrev(self) -> None:
        assert map_korean_to_result_code("희플") == "SF"

    def test_bunt(self) -> None:
        assert map_korean_to_result_code("번트") == "SH"

    # ── 실책 ─────────────────────────────────────────────────────────────────

    def test_error(self) -> None:
        assert map_korean_to_result_code("실책") == "E"

    def test_reached_on_error(self) -> None:
        assert map_korean_to_result_code("실책출루") == "ROE"

    # ── 아웃 종류 ─────────────────────────────────────────────────────────────

    def test_groundout(self) -> None:
        assert map_korean_to_result_code("땅볼") == "GO"

    def test_flyout_뜬공(self) -> None:
        assert map_korean_to_result_code("뜬공") == "FO"

    def test_flyout_플라이(self) -> None:
        assert map_korean_to_result_code("플라이") == "FO"

    def test_line_drive(self) -> None:
        assert map_korean_to_result_code("라인드라이브") == "LD"

    def test_double_play(self) -> None:
        assert map_korean_to_result_code("병살") == "DP"

    def test_triple_play(self) -> None:
        assert map_korean_to_result_code("삼중살") == "TP"

    # ── 주루 ─────────────────────────────────────────────────────────────────

    def test_stolen_base(self) -> None:
        assert map_korean_to_result_code("도루") == "SB"

    def test_caught_stealing(self) -> None:
        assert map_korean_to_result_code("주루사") == "CS"

    def test_picked_off(self) -> None:
        assert map_korean_to_result_code("견제사") == "PO"

    # ── 투구 이상 ─────────────────────────────────────────────────────────────

    def test_wild_pitch(self) -> None:
        assert map_korean_to_result_code("폭투") == "WP"

    def test_passed_ball(self) -> None:
        assert map_korean_to_result_code("포일") == "PB"

    # ── 콜론 이후 텍스트도 처리 ───────────────────────────────────────────────

    def test_colon_prefix_is_stripped(self) -> None:
        assert map_korean_to_result_code("홍길동: 홈런") == "HR"

    def test_colon_prefix_single(self) -> None:
        assert map_korean_to_result_code("김선수: 안타") == "H1"

    def test_colon_prefix_fc(self) -> None:
        assert map_korean_to_result_code("박타자: 야수선택") == "FC"

    # ── 매핑 불가 / 엣지 케이스 ──────────────────────────────────────────────

    def test_empty_string(self) -> None:
        assert map_korean_to_result_code("") is None

    def test_none_input(self) -> None:
        assert map_korean_to_result_code(None) is None

    def test_whitespace_only(self) -> None:
        assert map_korean_to_result_code("   ") is None

    def test_unknown_text(self) -> None:
        assert map_korean_to_result_code("알 수 없는 결과") is None

    def test_noise_text(self) -> None:
        assert map_korean_to_result_code("마운드 방문") is None

    # ── 우선순위: 내야안타 > 안타 ────────────────────────────────────────────

    def test_infield_single_priority_over_single(self) -> None:
        """'내야안타' 패턴이 '안타'보다 먼저 매칭되어야 한다."""
        assert map_korean_to_result_code("내야안타") == "IH"
        assert map_korean_to_result_code("안타") == "H1"

    # ── 우선순위: 실책출루 > 실책 ────────────────────────────────────────────

    def test_roe_priority_over_error(self) -> None:
        assert map_korean_to_result_code("실책출루") == "ROE"
        assert map_korean_to_result_code("실책") == "E"


class TestEnrichResultCode:
    """enrich_result_code 단위 테스트 (폴백 포함)."""

    def test_mapped_code(self) -> None:
        assert enrich_result_code("김선수: 홈런") == "HR"

    def test_fallback_to_raw_after_colon(self) -> None:
        """매핑 실패 시 `:` 이후 원문 반환."""
        result = enrich_result_code("타자: 알 수 없는 플레이")
        assert result == "알 수 없는 플레이"

    def test_no_colon_unmapped_returns_none(self) -> None:
        """`:` 없고 매핑 실패 시 None."""
        assert enrich_result_code("알 수 없는 플레이") is None

    def test_none_input(self) -> None:
        assert enrich_result_code(None) is None

    def test_empty_input(self) -> None:
        assert enrich_result_code("") is None

    def test_colon_only_returns_none(self) -> None:
        assert enrich_result_code(":") is None

    def test_standard_event_format(self) -> None:
        """'선수명: 결과' 형식."""
        assert enrich_result_code("이대호: 볼넷") == "BB"


class TestResultCodeToLabel:
    """result_code_to_label 단위 테스트."""

    def test_hr_label(self) -> None:
        assert result_code_to_label("HR") == "홈런"

    def test_h1_label(self) -> None:
        assert result_code_to_label("H1") == "안타"

    def test_ih_label(self) -> None:
        assert result_code_to_label("IH") == "내야안타"

    def test_k_label(self) -> None:
        assert result_code_to_label("K") == "삼진"

    def test_fc_label(self) -> None:
        assert result_code_to_label("FC") == "야수선택"

    def test_unknown_code_returns_code(self) -> None:
        assert result_code_to_label("XYZ") == "XYZ"

    def test_none_returns_empty(self) -> None:
        assert result_code_to_label(None) == ""

    def test_lowercase_code(self) -> None:
        """소문자 코드도 처리."""
        assert result_code_to_label("hr") == "홈런"


class TestCategoryHelpers:
    """is_hit / is_out / is_on_base / is_plate_appearance 테스트."""

    @pytest.mark.parametrize("code", ["H1", "IH", "H2", "H3", "HR"])
    def test_hit_codes(self, code: str) -> None:
        assert is_hit(code)

    @pytest.mark.parametrize("code", ["K", "BB", "FC", "E", "GO"])
    def test_non_hit_codes(self, code: str) -> None:
        assert not is_hit(code)

    @pytest.mark.parametrize("code", ["K", "GO", "FO", "LD", "DP", "TP"])
    def test_out_codes(self, code: str) -> None:
        assert is_out(code)

    @pytest.mark.parametrize("code", ["H1", "BB", "FC", "HR"])
    def test_non_out_codes(self, code: str) -> None:
        assert not is_out(code)

    @pytest.mark.parametrize("code", sorted(ON_BASE_CODES))
    def test_on_base_codes(self, code: str) -> None:
        assert is_on_base(code)

    def test_sh_not_plate_appearance(self) -> None:
        assert not is_plate_appearance("SH")

    def test_sf_not_plate_appearance(self) -> None:
        assert not is_plate_appearance("SF")

    def test_sb_not_plate_appearance(self) -> None:
        assert not is_plate_appearance("SB")

    def test_h1_is_plate_appearance(self) -> None:
        assert is_plate_appearance("H1")

    def test_k_is_plate_appearance(self) -> None:
        assert is_plate_appearance("K")

    def test_none_not_plate_appearance(self) -> None:
        assert not is_plate_appearance(None)

    def test_hit_codes_constant_integrity(self) -> None:
        """HIT_CODES에 정의된 모든 코드가 is_hit()에서 True."""
        for code in HIT_CODES:
            assert is_hit(code), f"is_hit({code!r}) should be True"

    def test_out_codes_constant_integrity(self) -> None:
        for code in OUT_CODES:
            assert is_out(code), f"is_out({code!r}) should be True"
