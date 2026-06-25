"""Crawler for Doosan Bears official operation notices (doosanbears.com).

Targets the Doosan Bears event/notice board to capture game-day
operational announcements for Jamsil Stadium (Doosan's home).

Target URL pattern:
  https://www.doosanbears.com/event/board?page={page}
"""

from __future__ import annotations

import logging
import re
from http import HTTPStatus

from bs4 import BeautifulSoup
from playwright.async_api import Error as PlaywrightError
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.repositories.operation_notice_repository import OperationNoticeRepository
from src.utils.naver_helpers import parse_multi_format_date
from src.utils.throttle import throttle

logger = logging.getLogger(__name__)

STADIUM_CODE = "JAMSIL"
SOURCE_NAME = "두산베어스공식"
BASE_URL = "https://www.doosanbears.com/doorundoorun/notice"
LINK_PREFIX = "https://www.doosanbears.com/doorundoorun"
HOST = "www.doosanbears.com"
DOOSAN_NOTICE_CRAWL_EXCEPTIONS = (PlaywrightError, RuntimeError, ValueError, TypeError, KeyError, OSError)

NOTICE_TYPE_RULES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"취소|우천|노게임", re.IGNORECASE), "CANCEL"),
    (re.compile(r"지연|딜레이|연기", re.IGNORECASE), "DELAY"),
    (re.compile(r"게이트|출입문|입장문", re.IGNORECASE), "GATE_CHANGE"),
    (re.compile(r"입장|제한|금지|규정", re.IGNORECASE), "ENTRY_RULE"),
    (re.compile(r"주차|파킹", re.IGNORECASE), "PARKING"),
    (re.compile(r"날씨|기상|비|폭우|태풍", re.IGNORECASE), "WEATHER"),
]
URGENT_KEYWORDS = re.compile(r"\[긴급\]|\[필독\]|\[중요\]|긴급공지|즉시", re.IGNORECASE)


def _classify_notice(title: str) -> str:
    for pattern, notice_type in NOTICE_TYPE_RULES:
        if pattern.search(title):
            return notice_type
    return "GENERAL"


def _is_urgent(title: str) -> bool:
    return bool(URGENT_KEYWORDS.search(title))


def _parse_date(raw: str) -> datetime | None:
    return parse_multi_format_date(raw)


def _extract_article_id(href: str) -> str | None:
    m = re.search(r"(?:idx|id|seq|no|boardIdx|articleIdx|snSeq)=(\d+)", href, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"/(\d+)(?:\?|$)", href)
    if m:
        return m.group(1)
    return None


class OperationNoticeDoosanCrawler:
    """Crawls Doosan Bears official announcements and maps them to
    StadiumOperationNotice records for JAMSIL stadium.
    """

    def __init__(self, max_pages: int = 5) -> None:
        self.max_pages = max_pages
        self._raw_pages: list[dict] = []

    async def run(self, *, save: bool = False, stop_at_external_id: str | None = None) -> list[dict]:
        all_notices: list[dict] = []
        from src.utils.playwright_pool import AsyncPlaywrightPool

        async with AsyncPlaywrightPool(max_pages=1, block_resources=True) as pool, pool.page() as page:
            for page_no in range(1, self.max_pages + 1):
                url = f"{BASE_URL}?page={page_no}"
                try:
                    await throttle.wait(HOST)
                    resp = await page.goto(url)
                    if not resp or resp.status != HTTPStatus.OK:
                        logger.warning(
                            "[Doosan Notice] HTTP %s on page %d",
                            resp.status if resp else "None",
                            page_no,
                        )
                        break

                    await page.wait_for_timeout(2000)
                    html = await page.content()
                    self._raw_pages.append(
                        {"source_key": "doosan_bears_notices", "url": url, "html": html, "status_code": 200},
                    )

                    notices, hit_stop = self._parse_page(html, stop_at_external_id)
                    all_notices.extend(notices)
                    logger.info("[Doosan Notice] page %s: %s notices", page_no, len(notices))

                    if hit_stop or not notices:
                        break

                except DOOSAN_NOTICE_CRAWL_EXCEPTIONS:
                    logger.exception("[Doosan Notice] Failed to fetch page %d", page_no)
                    break

        logger.info("[Doosan Notice] Total: %s notices", len(all_notices))

        if save:
            self._save_to_db(all_notices)
        else:
            for n in all_notices[:5]:
                logger.info(n)

        return all_notices

    def _parse_page(self, html: str, stop_at_id: str | None) -> tuple[list[dict], bool]:
        soup = BeautifulSoup(html, "html.parser")
        notices: list[dict] = []
        hit_stop = False

        rows = soup.select("ul.list-box li, .list-box li")

        for row in rows:
            anchor = row.find("a", href=True)
            if not anchor:
                continue

            href = anchor["href"]
            href = href.removeprefix("./")
            if not href.startswith("http"):
                if href.startswith("notice/"):
                    href = "https://www.doosanbears.com/doorundoorun/" + href
                elif href.startswith("doorundoorun/"):
                    href = "https://www.doosanbears.com/" + href
                else:
                    href = "https://www.doosanbears.com/doorundoorun/notice/" + href

            external_id = _extract_article_id(href) or href

            if stop_at_id and external_id == stop_at_id:
                hit_stop = True
                break

            title_el = row.select_one(".tit, p.tit")
            title = title_el.get_text(strip=True) if title_el else anchor.get_text(strip=True)
            if not title:
                continue

            date_el = row.select_one(".txt, p.txt")
            published_at = None
            if date_el:
                published_at = _parse_date(date_el.get_text(strip=True))

            notices.append(
                {
                    "stadium_code": STADIUM_CODE,
                    "notice_type": _classify_notice(title),
                    "title": title,
                    "content": None,
                    "published_at": published_at,
                    "game_date": published_at.date() if published_at else None,
                    "source_name": SOURCE_NAME,
                    "source_url": href,
                    "external_id": external_id,
                    "is_urgent": _is_urgent(title),
                    "is_confirmed": True,
                    "raw_snapshot": {"href": href, "title": title},
                },
            )

        return notices, hit_stop

    def _save_to_db(self, notices: list[dict]) -> None:
        with SessionLocal() as session:
            try:
                repo = OperationNoticeRepository(session)
                created, updated = repo.bulk_upsert(notices)
                session.commit()
                logger.info("[Doosan Notice] Saved: %s new, %s updated.", created, updated)
            except SQLAlchemyError:
                session.rollback()
                logger.exception("[Doosan Notice] Database error")
            finally:
                self._raw_pages.clear()


if __name__ == "__main__":
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description="Crawl Doosan Bears operation notices")
    parser.add_argument("--save", action="store_true", help="Save to DB")
    parser.add_argument("--pages", type=int, default=5, help="Max pages to crawl")
    args = parser.parse_args()

    asyncio.run(OperationNoticeDoosanCrawler(max_pages=args.pages).run(save=args.save))
