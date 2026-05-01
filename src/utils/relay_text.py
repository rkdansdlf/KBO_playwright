from __future__ import annotations

import re
from typing import Any


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
    re.compile(r"^\d+구\s+"),
    re.compile(r"^(승리투수|패전투수|세이브|홀드)\s*:"),
)

_RELAY_NOISE_TOKENS = (
    "마운드 방문",
    "투수판 이탈",
    "코칭스태프",
    "경기 준비중",
    "경기 시작",
    "경기 종료",
    "경기 중단",
    "그라운드 정비",
    "피치클락",
    "비디오 판독",
    "심판 자체",
    "ABS 추적",
    "ABS 수신",
    "교체",
)


def compact_relay_text(description: Any) -> str:
    return " ".join(str(description or "").strip().split())


def is_relay_noise_text(description: Any) -> bool:
    text = compact_relay_text(description)
    if not text:
        return True
    if any(pattern.search(text) for pattern in _RELAY_NOISE_PATTERNS):
        return True
    return any(token in text for token in _RELAY_NOISE_TOKENS)


def is_relay_result_event_text(description: Any) -> bool:
    text = compact_relay_text(description)
    if is_relay_noise_text(text) or ":" not in text:
        return False
    result_text = text.split(":", 1)[-1].strip()
    if "교체" in result_text:
        return False
    return any(keyword in result_text for keyword in RELAY_RESULT_KEYWORDS)


def detect_relay_event_type(description: Any) -> str:
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
