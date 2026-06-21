"""
Naver Search-based operation notice crawler.

Supplements the LG/Doosan website crawlers by querying Naver News API
for real-time game-day notices and urgent announcements.

This is the practical alternative to Twitter/X API (which is paid).
Naver Search API is free up to 25,000 calls/day.

Usage:
    python -m src.crawlers.operation_notice_naver_crawler --save
    python -m src.crawlers.operation_notice_naver_crawler --days 1 --save
"""

from __future__ import annotations

import logging
import re
from datetime import date
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.repositories.operation_notice_repository import OperationNoticeRepository
from src.utils.naver_search_client import NaverSearchClient, NaverSearchResult

logger = logging.getLogger(__name__)

STADIUM_CODE = "JAMSIL"
SOURCE_NAME = "naver_search"
NAVER_NOTICE_DB_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError, OSError)

# Keyword → notice_type classification
NOTICE_TYPE_RULES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"취소|우천|노게임|우중|폭우", re.I), "CANCEL"),
    (re.compile(r"지연|연기|딜레이", re.I), "DELAY"),
    (re.compile(r"게이트|출입문|입장문|입구", re.I), "GATE_CHANGE"),
    (re.compile(r"입장|제한|금지|규정|반입", re.I), "ENTRY_RULE"),
    (re.compile(r"주차|파킹|주차장", re.I), "PARKING"),
    (re.compile(r"날씨|기상|태풍|강풍", re.I), "WEATHER"),
    (re.compile(r"셔틀|버스|교통|혼잡", re.I), "ENTRY_RULE"),
]

URGENT_KEYWORDS = re.compile(r"\[긴급\]|\[필독\]|\[중요\]|긴급|즉시|당장|오늘 취소|경기 취소", re.I)

TEAM_SOURCE_MAP = {
    "LG": "naver_search_LG",
    "OB": "naver_search_두산",
    "NC": "naver_search_NC",
    None: "naver_search_잠실",
}


def _classify(text: str) -> str:
    for pattern, notice_type in NOTICE_TYPE_RULES:
        if pattern.search(text):
            return notice_type
    return "GENERAL"


def _is_urgent(text: str) -> bool:
    return bool(URGENT_KEYWORDS.search(text))


def _infer_game_date(result: NaverSearchResult) -> date | None:
    """Infer game date from publication date (assume same-day notice)."""
    if result.pub_date:
        return result.pub_date.date()
    return date.today()


def _result_to_notice(result: NaverSearchResult) -> dict[str, Any]:
    combined_text = f"{result.title} {result.description}"
    source_name = TEAM_SOURCE_MAP.get(result.team_hint, SOURCE_NAME)
    game_date = _infer_game_date(result)

    return {
        "stadium_code": STADIUM_CODE,
        "notice_type": _classify(combined_text),
        "title": result.title[:500],
        "content": result.description[:2000] if result.description else None,
        "source_name": source_name,
        "source_url": result.link,
        "external_id": result.link,  # URL as stable dedup key
        "published_at": result.pub_date,
        "game_date": game_date,
        "is_urgent": _is_urgent(combined_text),
        "is_confirmed": False,  # News is not official confirmation
        "raw_snapshot": result.raw,
    }


class OperationNoticeNaverCrawler:
    """
    Crawls Naver News for KBO game-day operation notices.

    Complements official team website crawlers with real-time news coverage.
    Results are tagged is_confirmed=False to distinguish from official notices.
    """

    def __init__(self, days_back: int = 3) -> None:
        self.days_back = days_back
        self.client = NaverSearchClient()

    async def run(self, save: bool = False) -> list[dict]:
        logger.info("[NaverNotice] Searching for notices (last %s days)...", self.days_back)

        if not self.client._is_configured():
            logger.warning("[NaverNotice] ⚠️  NAVER_CLIENT_ID / NAVER_CLIENT_SECRET not set.")
            logger.info("[NaverNotice]    Set in .env to enable Naver search-based notice crawling.")
            return []

        search_results = await self.client.search_kbo_notices(days_back=self.days_back)
        logger.info("[NaverNotice] Found %s articles from Naver.", len(search_results))

        notices = [_result_to_notice(r) for r in search_results]

        if save:
            self._save_to_db(notices)
        else:
            for n in notices[:5]:
                logger.info(
                    "  [%s] %s | urgent=%s | %s", n["notice_type"], n["title"][:60], n["is_urgent"], n["published_at"]
                )
            if len(notices) > 5:
                logger.info("  ... and %s more", len(notices) - 5)

        return notices

    def _save_to_db(self, notices: list[dict]) -> None:
        with SessionLocal() as session:
            try:
                repo = OperationNoticeRepository(session)
                created, updated = repo.bulk_upsert(notices)
                session.commit()
                logger.info("[NaverNotice] Saved: %s new, %s updated.", created, updated)
            except NAVER_NOTICE_DB_EXCEPTIONS:
                session.rollback()
                logger.exception("Error saving notices")


if __name__ == "__main__":
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description="Naver-based KBO notice crawler")
    parser.add_argument("--save", action="store_true", help="Save to DB")
    parser.add_argument("--days", type=int, default=3, help="Days back to search")
    args = parser.parse_args()

    asyncio.run(OperationNoticeNaverCrawler(days_back=args.days).run(save=args.save))
