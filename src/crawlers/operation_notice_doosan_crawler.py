"""
Crawler for Doosan Bears official operation notices (doosanbears.com).

Targets the Doosan Bears event/notice board to capture game-day
operational announcements for Jamsil Stadium (Doosan's home).

Target URL pattern:
  https://www.doosanbears.com/event/board?page={page}
"""
from __future__ import annotations

import logging
import re
from datetime import datetime

import httpx
from bs4 import BeautifulSoup

from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.repositories.operation_notice_repository import OperationNoticeRepository
from src.utils.http_client import DEFAULT_HEADERS as HEADERS
from src.utils.safe_print import safe_print as print
from src.utils.throttle import throttle

logger = logging.getLogger(__name__)

STADIUM_CODE = "JAMSIL"
SOURCE_NAME = "두산베어스공식"
BASE_URL = "https://www.doosanbears.com/doorundoorun/notice"
LINK_PREFIX = "https://www.doosanbears.com/doorundoorun"
HOST = "www.doosanbears.com"

NOTICE_TYPE_RULES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"취소|우천|노게임", re.I), "CANCEL"),
    (re.compile(r"지연|딜레이|연기", re.I), "DELAY"),
    (re.compile(r"게이트|출입문|입장문", re.I), "GATE_CHANGE"),
    (re.compile(r"입장|제한|금지|규정", re.I), "ENTRY_RULE"),
    (re.compile(r"주차|파킹", re.I), "PARKING"),
    (re.compile(r"날씨|기상|비|폭우|태풍", re.I), "WEATHER"),
]
URGENT_KEYWORDS = re.compile(r"\[긴급\]|\[필독\]|\[중요\]|긴급공지|즉시", re.I)


def _classify_notice(title: str) -> str:
    for pattern, notice_type in NOTICE_TYPE_RULES:
        if pattern.search(title):
            return notice_type
    return "GENERAL"


def _is_urgent(title: str) -> bool:
    return bool(URGENT_KEYWORDS.search(title))


def _parse_date(raw: str) -> datetime | None:
    for fmt in ("%Y.%m.%d", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw.strip(), fmt)
        except ValueError:
            continue
    return None


def _extract_article_id(href: str) -> str | None:
    m = re.search(r"(?:idx|id|seq|no|boardIdx|articleIdx|snSeq)=(\d+)", href, re.I)
    if m:
        return m.group(1)
    m = re.search(r"/(\d+)(?:\?|$)", href)
    if m:
        return m.group(1)
    return None


class OperationNoticeDoosanCrawler:
    """
    Crawls Doosan Bears official announcements and maps them to
    StadiumOperationNotice records for JAMSIL stadium.
    """

    def __init__(self, max_pages: int = 5) -> None:
        self.max_pages = max_pages
        self._raw_pages: list[dict] = []

    async def run(self, save: bool = False, stop_at_external_id: str | None = None) -> list[dict]:
        all_notices: list[dict] = []
        from src.utils.playwright_pool import AsyncPlaywrightPool

        async with AsyncPlaywrightPool(max_pages=1, block_resources=True) as pool:
            async with pool.page() as page:
                for page_no in range(1, self.max_pages + 1):
                    url = f"{BASE_URL}?page={page_no}"
                    try:
                        await throttle.wait(HOST)
                        resp = await page.goto(url)
                        if not resp or resp.status != 200:
                            logger.warning("[Doosan Notice] HTTP %s on page %d", resp.status if resp else "None", page_no)
                            break

                        await page.wait_for_timeout(2000)
                        html = await page.content()
                        self._raw_pages.append(
                            {"source_key": "doosan_bears_notices", "url": url, "html": html, "status_code": 200}
                        )

                        notices, hit_stop = self._parse_page(html, stop_at_external_id)
                        all_notices.extend(notices)
                        print(f"[Doosan Notice] page {page_no}: {len(notices)} notices")

                        if hit_stop or not notices:
                            break

                    except Exception as e:
                        logger.exception("[Doosan Notice] Failed to fetch page %d: %s", page_no, e)
                        break

        print(f"[Doosan Notice] Total: {len(all_notices)} notices")

        if save:
            self._save_to_db(all_notices)
        else:
            for n in all_notices[:5]:
                print(n)

        return all_notices

    def _parse_page(
        self, html: str, stop_at_id: str | None
    ) -> tuple[list[dict], bool]:
        soup = BeautifulSoup(html, "html.parser")
        notices: list[dict] = []
        hit_stop = False

        rows = soup.select("ul.list-box li, .list-box li")

        for row in rows:
            anchor = row.find("a", href=True)
            if not anchor:
                continue

            href = anchor["href"]
            if href.startswith("./"):
                href = href[2:]
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
                }
            )

        return notices, hit_stop

    def _save_to_db(self, notices: list[dict]) -> None:
        with SessionLocal() as session:
            try:
                repo = OperationNoticeRepository(session)
                created, updated = repo.bulk_upsert(notices)
                session.commit()
                print(f"[Doosan Notice] Saved: {created} new, {updated} updated.")
            except SQLAlchemyError as e:
                session.rollback()
                logger.error(f"[Doosan Notice] Database error: {e}", exc_info=True)
                print(f"[Doosan Notice] Error: {e}")
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
