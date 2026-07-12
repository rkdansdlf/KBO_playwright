"""한국어 PBP 텍스트 → 표준 result_code 변환 유틸리티.

KBO 릴레이 중계 텍스트에서 추출된 결과 문자열을 split_calculator,
game_story_builder 등이 사용하는 표준 영문 코드로 변환합니다.

표준 코드 체계:
    타격 결과: H1(1루타), IH(내야안타), H2(2루타), H3(3루타), HR(홈런)
    아웃:      K(삼진), GO(땅볼아웃), FO(플라이아웃), LD(라인드라이브아웃)
               DP(병살), TP(삼중살)
    출루:      BB(볼넷), HBP(사구), FC(야수선택)
    희생:      SH(희생번트), SF(희생플라이)
    실책:      E(실책), ROE(실책출루)
    주루:      SB(도루), CS(주루사/도루실패), PO(견제사)
    기타:      WP(폭투), PB(포일)
"""

from __future__ import annotations

# ──────────────────────────────────────────────────────────────────────────────
# 우선순위 순서로 정의 (더 구체적인 패턴이 앞에 위치)
# ──────────────────────────────────────────────────────────────────────────────

# (패턴, 코드) 리스트 — 첫 번째 매칭 패턴이 적용됨
_ORDERED_PATTERNS: list[tuple[str, str]] = [
    # ── 홈런 / 장타 ──────────────────────────────────────────────
    ("홈런", "HR"),
    ("3루타", "H3"),
    ("2루타", "H2"),
    # ── 안타 (내야안타 우선) ──────────────────────────────────────
    ("내야안타", "IH"),
    ("안타", "H1"),
    ("1루타", "H1"),
    ("적시타", "H1"),
    # ── 삼진 ─────────────────────────────────────────────────────
    ("낫 아웃", "K"),
    ("삼진", "K"),
    # ── 볼넷 / 출루 ──────────────────────────────────────────────
    ("고의4구", "BB"),
    ("자동 고의4구", "BB"),
    ("볼넷", "BB"),
    ("몸에 맞는 볼", "HBP"),
    ("사구", "HBP"),
    # ── 야수선택 ─────────────────────────────────────────────────
    ("야수선택", "FC"),
    # ── 희생 ─────────────────────────────────────────────────────
    ("희생플라이", "SF"),
    ("희플", "SF"),
    ("희생번트", "SH"),
    ("희번", "SH"),
    ("번트", "SH"),
    # ── 실책 ─────────────────────────────────────────────────────
    ("실책출루", "ROE"),
    ("실책", "E"),
    # ── 아웃 종류 ─────────────────────────────────────────────────
    ("삼중살", "TP"),
    ("병살", "DP"),
    ("라인드라이브", "LD"),
    ("직선타", "LD"),
    ("뜬공", "FO"),
    ("플라이", "FO"),
    ("땅볼", "GO"),
    # ── 주루 ─────────────────────────────────────────────────────
    ("도루", "SB"),
    ("주루사", "CS"),
    ("견제사", "PO"),
    ("태그아웃", "CS"),
    ("송구아웃", "CS"),
    # ── 투구 / 기타 ──────────────────────────────────────────────
    ("폭투", "WP"),
    ("포일", "PB"),
]

# 빠른 exact-substring 매핑 (패턴 목록을 dict로 변환)
_PATTERN_TO_CODE: list[tuple[str, str]] = _ORDERED_PATTERNS


def map_korean_to_result_code(text: str | None) -> str | None:
    """한국어 PBP 결과 텍스트를 표준 result_code로 변환합니다.

    `:` 구분자 이후 결과 부분을 입력으로 받거나, 전체 이벤트 설명을
    받아도 동작합니다.

    Args:
        text: 한국어 PBP 텍스트 (예: "안타", "홈런", "야수선택", …)

    Returns:
        표준 result_code 문자열 (예: "H1", "HR", "FC") 또는
        매핑 실패 시 None.

    """
    if not text:
        return None

    # `:` 이후 결과 부분만 추출
    normalized = str(text).strip()
    if ":" in normalized:
        normalized = normalized.split(":", 1)[-1].strip()

    if not normalized:
        return None

    for pattern, code in _PATTERN_TO_CODE:
        if pattern in normalized:
            return code

    return None


def enrich_result_code(description: str | None) -> str | None:
    """이벤트 설명 전체에서 result_code를 추출합니다.

    매핑 실패 시 `:` 이후 원문 텍스트를 폴백으로 반환합니다
    (하위 호환성 유지).

    Args:
        description: 전체 이벤트 설명 텍스트.

    Returns:
        표준 result_code 또는 raw 폴백 텍스트 또는 None.

    """
    if not description:
        return None

    text = str(description).strip()
    if ":" not in text:
        return None

    raw_result = text.split(":", 1)[-1].strip()
    if not raw_result:
        return None

    # 표준 코드로 변환 시도
    code = map_korean_to_result_code(raw_result)
    if code:
        return code

    return raw_result


# ── 역방향: result_code → 한국어 라벨 (UI/리포트용) ─────────────────────────

_CODE_TO_LABEL: dict[str, str] = {
    "HR": "홈런",
    "H3": "3루타",
    "H2": "2루타",
    "H1": "안타",
    "IH": "내야안타",
    "K": "삼진",
    "BB": "볼넷",
    "HBP": "사구",
    "FC": "야수선택",
    "SH": "희생번트",
    "SF": "희생플라이",
    "E": "실책",
    "ROE": "실책출루",
    "DP": "병살",
    "TP": "삼중살",
    "GO": "땅볼",
    "FO": "플라이",
    "LD": "라인드라이브",
    "SB": "도루",
    "CS": "주루사",
    "PO": "견제사",
    "WP": "폭투",
    "PB": "포일",
}


def result_code_to_label(code: str | None) -> str:
    """result_code를 한국어 라벨로 변환합니다 (UI/리포트용).

    Args:
        code: 표준 result_code.

    Returns:
        한국어 라벨 또는 코드 자체.

    """
    if not code:
        return ""
    return _CODE_TO_LABEL.get(str(code).upper(), code)


# ── result_code 카테고리 분류 ───────────────────────────────────────────────

HIT_CODES: frozenset[str] = frozenset({"H1", "IH", "H2", "H3", "HR"})
OUT_CODES: frozenset[str] = frozenset({"K", "GO", "FO", "LD", "DP", "TP"})
ON_BASE_CODES: frozenset[str] = frozenset({"H1", "IH", "H2", "H3", "HR", "BB", "HBP", "FC", "ROE"})
SACRIFICE_CODES: frozenset[str] = frozenset({"SH", "SF"})
BASE_RUNNING_CODES: frozenset[str] = frozenset({"SB", "CS", "PO", "WP", "PB"})


def is_hit(code: str | None) -> bool:
    """result_code가 안타 계열인지 확인합니다."""
    return bool(code and code.upper() in HIT_CODES)


def is_out(code: str | None) -> bool:
    """result_code가 아웃 계열인지 확인합니다."""
    return bool(code and code.upper() in OUT_CODES)


def is_on_base(code: str | None) -> bool:
    """result_code가 출루 계열인지 확인합니다."""
    return bool(code and code.upper() in ON_BASE_CODES)


def is_plate_appearance(code: str | None) -> bool:
    """result_code가 타석 소비 결과인지 확인합니다 (희생/주루 제외)."""
    if not code:
        return False
    upper = code.upper()
    return upper not in SACRIFICE_CODES and upper not in BASE_RUNNING_CODES
