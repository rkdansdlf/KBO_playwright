"""유틸리티: relay text."""

from __future__ import annotations

import re

RELAY_RESULT_KEYWORDS = (
    "안타",
    "1루타",
    "내야안타",
    "적시타",
    "홈런",
    "2루타",
    "3루타",
    "아웃",
    "삼진",
    "볼넷",
    "고의4구",
    "자동 고의4구",
    "사구",
    "몸에 맞는 볼",
    "실책",
    "희생",
    "희번",
    "희플",
    "번트",
    "플라이",
    "뜬공",
    "땅볼",
    "라인드라이브",
    "직선타",
    "병살",
    "삼중살",
    "야수선택",
    "도루",
    "주루사",
    "견제사",
    "태그아웃",
    "터치아웃",
    "송구아웃",
    "낫 아웃",
    "폭투",
    "포일",
    "홈인",
    "득점",
    "진루",
)

_RELAY_NOISE_PATTERNS = (
    re.compile(r"^\s*=+\s*$"),
    re.compile(r"^\d+회\s*(초|말)\s+.*공격\s*$"),
    re.compile(r"^\d+회(초|말)\s+.*공격\s*$"),
    re.compile(r"^\d+번타자\s+"),
    re.compile(r"^\d+구\s+(?:볼|스트라이크|파울|헛스윙|폭투|포일|중계|방어)"),
    re.compile(r"^(?:승리투수|패전투수|세이브|홀드)\s*:"),
    re.compile(r"^(?:경기|게임)\s*(?:시작|종료|중단|재개|취소)"),
    re.compile(r"^피치클락\s+위반"),
)

_RELAY_NOISE_TOKENS = (
    # Game lifecycle
    "경기 준비중",
    "경기 시작",
    "경기 종료",
    "경기 중단",
    "경기 재개",
    "게임 종료",
    # Pitching/coaching
    "마운드 방문",
    "투수판 이탈",
    "코칭스태프",
    "피치클락",
    "피치클락 위반",
    # Field maintenance
    "그라운드 정비",
    "우천",
    # Video review / ABS
    "비디오 판독",
    "심판 자체",
    "ABS 추적",
    "ABS 수신",
    "ABS 확인",
    # Substitutions
    "교체",
    "대타",
    "대주자",
    "대수비",
    # Game summary / admin
    "승리투수",
    "패전투수",
    "세이브",
    "홀드",
    "결승타",
    "MVP",
    # Suspension / delay
    "서스펜디드",
    "콜드게임",
)


def compact_relay_text(description: object) -> str:
    """Handles the compact relay text operation.

    Args:
        description: Description.

    Returns:
        String result.

    """
    return " ".join(str(description or "").strip().split())


def parse_pitch_count(description: str) -> dict[str, int | None]:
    """Parse one pitch log into the count change caused by that pitch.

    The leading ``n구`` value is the pitch ordinal, not the ball/strike count.
    This function therefore returns the count delta for a single pitch, with
    callers responsible for accumulating state across an at-bat.
    """
    desc = compact_relay_text(description)
    if not desc:
        return {"balls": None, "strikes": None}

    match = re.match(r"^(\d+)구\s+(볼|스트라이크|파울|헛스윙|폭투|포일)", desc)
    if not match:
        return {"balls": None, "strikes": None}

    pitch_type = match.group(2)

    if pitch_type in ("볼", "폭투"):
        return {"balls": 1, "strikes": 0}
    if pitch_type in ("스트라이크", "헛스윙", "파울"):
        return {"balls": 0, "strikes": 1}
    return {"balls": None, "strikes": None}


def advance_pitch_count(description: str, balls: int = 0, strikes: int = 0) -> tuple[int, int, bool]:
    """Advance an at-bat count from one raw pitch text.

    Foul balls do not move the count past two strikes. Returns
    ``(balls, strikes, matched_pitch_text)``.
    """
    desc = compact_relay_text(description)
    match = re.match(r"^(\d+)구\s+(볼|스트라이크|파울|헛스윙|폭투|포일)", desc)
    if not match:
        return balls, strikes, False

    pitch_type = match.group(2)
    if pitch_type in {"볼", "폭투"}:
        balls = min(4, balls + 1)
    elif pitch_type in {"스트라이크", "헛스윙"}:
        strikes = min(3, strikes + 1)
    elif pitch_type == "파울":
        strikes = min(2, strikes + 1)
    return balls, strikes, True


def is_relay_noise_text(description: object) -> bool:
    """Returns whether the relay noise text.

    Args:
        description: Description.

    Returns:
        True if the condition is met, False otherwise.

    """
    text = compact_relay_text(description)
    if not text:
        return True
    if any(pattern.search(text) for pattern in _RELAY_NOISE_PATTERNS):
        return True
    return any(token in text for token in _RELAY_NOISE_TOKENS)


def is_relay_result_event_text(description: object) -> bool:
    """Returns whether the relay result event text.

    Args:
        description: Description.

    Returns:
        True if the condition is met, False otherwise.

    """
    text = compact_relay_text(description)
    if is_relay_noise_text(text) or ":" not in text:
        return False
    result_text = text.split(":", 1)[-1].strip()
    if "교체" in result_text:
        return False
    return any(keyword in result_text for keyword in RELAY_RESULT_KEYWORDS)


def detect_relay_event_type(description: object) -> str:
    """Handles the detect relay event type operation.

    Args:
        description: Description.

    Returns:
        String result.

    """
    text = compact_relay_text(description)
    if not is_relay_result_event_text(text):
        return "unknown"
    result_text = text.split(":", 1)[-1].strip()
    if "도루" in result_text:
        return "steal"
    if any(token in result_text for token in ("홈인", "득점", "진루", "폭투", "포일")):
        return "runner_advance"
    if "주루사" in result_text or "견제사" in result_text or "태그아웃" in result_text:
        return "runner_out"
    return "batting"
