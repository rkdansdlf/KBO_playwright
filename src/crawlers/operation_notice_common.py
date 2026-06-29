"""Shared notice classification rules and helpers for operation notice crawlers."""

from __future__ import annotations

import re

NOTICE_TYPE_RULES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"취소|우천|노게임|우중|폭우", re.IGNORECASE), "CANCEL"),
    (re.compile(r"지연|딜레이|연기", re.IGNORECASE), "DELAY"),
    (re.compile(r"게이트|출입문|입장문|입구", re.IGNORECASE), "GATE_CHANGE"),
    (re.compile(r"입장|제한|금지|규정|반입", re.IGNORECASE), "ENTRY_RULE"),
    (re.compile(r"주차|파킹|주차장", re.IGNORECASE), "PARKING"),
    (re.compile(r"날씨|기상|비|폭우|태풍|강풍", re.IGNORECASE), "WEATHER"),
    (re.compile(r"셔틀|버스|교통|혼잡", re.IGNORECASE), "ENTRY_RULE"),
]

URGENT_KEYWORDS = re.compile(
    r"\[긴급\]|\[필독\]|\[중요\]|긴급공지|즉시|긴급|당장|오늘 취소|경기 취소",
    re.IGNORECASE,
)


def classify_notice(title: str) -> str:
    """
    Classifies notice.

    Args:
        title: Title.
        title: Title.

    Returns:
        String result.

    """
    for pattern, notice_type in NOTICE_TYPE_RULES:
        if pattern.search(title):
            return notice_type
    return "GENERAL"


def is_urgent(title: str) -> bool:
    """
    Return whether the urgent.

    Args:
        title: Title.
        title: Title.

    Returns:
        True if successful, False otherwise.

    """
    return bool(URGENT_KEYWORDS.search(title))
