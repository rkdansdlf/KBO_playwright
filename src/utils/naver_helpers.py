"""유틸리티: naver helpers."""

from __future__ import annotations

import contextlib
from datetime import date, datetime

from src.constants import KST

NAVER_TEAM_MAP: dict[str, str] = {
    "LG": "LG",
    "KT": "KT",
    "NC": "NC",
    "두산": "DB",
    "롯데": "LT",
    "삼성": "SS",
    "키움": "KH",
    "한화": "HH",
    "KIA": "KIA",
    "SSG": "SSG",
}


def parse_iso_date(pub_date: str) -> date | None:
    """
    Parse iso date.

    Args:
        pub_date: Pub Date.
        pub_date: Pub Date.
        pub_date: Pub Date.

    Returns:
        The result of the operation.

    """
    if not pub_date:
        return None
    with contextlib.suppress(ValueError, AttributeError):
        return datetime.fromisoformat(pub_date).date()
    return None


def parse_multi_format_date(raw: str) -> datetime | None:
    """
    Parse multi date.

    Args:
        raw: Raw.
        raw: Raw.
        raw: Raw.

    Returns:
        The result of the operation.

    """
    if not raw:
        return None
    for fmt in ("%Y.%m.%d", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw.strip(), fmt).replace(tzinfo=KST)
        except ValueError:
            continue
    return None


def build_naver_sports_url(oid: str, aid: str) -> str:
    """
    Build naver sports url.

    Args:
        oid: Oid.
        aid: Aid.
        oid: Oid.
        aid: Aid.
        oid: Oid.
        aid: Aid.

    Returns:
        String result.

    """
    if oid and aid:
        return f"https://sports.news.naver.com/kbaseball/news/read?oid={oid}&aid={aid}"
    return ""
