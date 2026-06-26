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
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST
from src.crawlers.operation_notice_common import classify_notice, is_urgent
from src.db.engine import SessionLocal
from src.repositories.operation_notice_repository import OperationNoticeRepository
from src.utils.naver_search_client import NaverSearchClient, NaverSearchResult

if TYPE_CHECKING:
    from datetime import date

logger = logging.getLogger(__name__)

STADIUM_CODE = "JAMSIL"
SOURCE_NAME = "naver_search"
NAVER_NOTICE_DB_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError, OSError)

TEAM_SOURCE_MAP = {
    "LG": "naver_search_LG",
    "OB": "naver_search_두산",
    "NC": "naver_search_NC",
    None: "naver_search_잠실",
}


def _infer_game_date(result: NaverSearchResult) -> date | None:
    """Infer game date from publication date (assume same-day notice)."""
    if result.pub_date:
        return result.pub_date.date()
    return datetime.now(KST).date()


def _result_to_notice(result: NaverSearchResult) -> dict[str, Any]:
    combined_text = f"{result.title} {result.description}"
    source_name = TEAM_SOURCE_MAP.get(result.team_hint, SOURCE_NAME)
    game_date = _infer_game_date(result)

    return {
        "stadium_code": STADIUM_CODE,
        "notice_type": classify_notice(combined_text),
        "title": result.title[:500],
        "content": result.description[:2000] if result.description else None,
        "source_name": source_name,
        "source_url": result.link,
        "external_id": result.link,  # URL as stable dedup key
        "published_at": result.pub_date,
        "game_date": game_date,
        "is_urgent": is_urgent(combined_text),
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
        """Initializes a new instance."""
        self.days_back = days_back
        self.client = NaverSearchClient()

    async def run(self, *, save: bool = False) -> list[dict]:
        """
        Runs run.

        Returns:
            List of results.

        """
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
                    "  [%s] %s | urgent=%s | %s",
                    n["notice_type"],
                    n["title"][:60],
                    n["is_urgent"],
                    n["published_at"],
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
