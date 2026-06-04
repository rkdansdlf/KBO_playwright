"""
Crawler for LG Twins official operation notices (lgtwins.com).

Scrapes game-day notices including gate changes, entry restrictions,
rain delays, and general announcements. Supports incremental crawling
by tracking the last seen external_id (article ID).

Target URL pattern:
  https://www.lgtwins.com/service/announcement?pageNo={page}
"""
from __future__ import annotations

import logging
import re
from datetime import datetime
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from src.db.engine import SessionLocal
from src.repositories.operation_notice_repository import OperationNoticeRepository
from src.utils.http_client import DEFAULT_HEADERS as HEADERS
from src.utils.safe_print import safe_print as print
from src.utils.throttle import throttle

logger = logging.getLogger(__name__)

STADIUM_CODE = "JAMSIL"
SOURCE_NAME = "LG트윈스공식"
BASE_URL = "https://www.lgtwins.com/service/announcement"
LINK_PREFIX = "https://www.lgtwins.com"
HOST = "www.lgtwins.com"

# Map title keywords → notice_type
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
    """Parse date strings like '2026.06.03' or '2026-06-03'."""
    for fmt in ("%Y.%m.%d", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw.strip(), fmt)
        except ValueError:
            continue
    return None


def _extract_article_id(href: str) -> str | None:
    """Extract article ID from URL query params or path."""
    m = re.search(r"(?:idx|id|seq|no|articleIdx)=(\d+)", href)
    if m:
        return m.group(1)
    m = re.search(r"/(\d+)(?:\?|$)", href)
    if m:
        return m.group(1)
    return None


class OperationNoticeLGCrawler:
    """
    Crawls LG Twins official announcements and maps them to
    StadiumOperationNotice records for JAMSIL stadium.
    """

    def __init__(self, max_pages: int = 5) -> None:
        self.max_pages = max_pages
        self._raw_pages: list[dict] = []

    async def run(self, save: bool = False, stop_at_external_id: str | None = None) -> list[dict]:
        """
        Crawl notices. If stop_at_external_id is provided, stops when
        a previously seen article is encountered (incremental mode).
        """
        all_notices: list[dict] = []

        async with httpx.AsyncClient(headers=HEADERS, timeout=15, follow_redirects=True) as client:
            for page in range(1, self.max_pages + 1):
                url = f"{BASE_URL}?pageNo={page}"
                try:
                    await throttle.wait(HOST)
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        logger.warning("[LG Notice] HTTP %s on page %d", resp.status_code, page)
                        break

                    html = resp.text
                    self._raw_pages.append(
                        {"source_key": "lg_twins_notices", "url": url, "html": html, "status_code": 200}
                    )

                    notices, hit_stop = self._parse_page(html, stop_at_external_id)
                    all_notices.extend(notices)

                    print(f"[LG Notice] page {page}: {len(notices)} notices")
                    if hit_stop or not notices:
                        break

                except httpx.HTTPError:
                    logger.exception("[LG Notice] Failed to fetch page %d", page)
                    break

        print(f"[LG Notice] Total: {len(all_notices)} notices")

        if save:
            self._save_to_db(all_notices)
        else:
            for n in all_notices[:5]:
                print(n)

        return all_notices

    def _parse_page(
        self, html: str, stop_at_id: str | None
    ) -> tuple[list[dict], bool]:
        """Parse one listing page. Returns (notices, hit_stop_id)."""
        soup = BeautifulSoup(html, "html.parser")
        notices: list[dict] = []
        hit_stop = False

        # LG Twins notice list: <ul class="board-list"> or <table> with anchor tags
        rows = soup.select("ul.board-list li, table.board-table tbody tr, .list-wrap .item")
        if not rows:
            # Fallback: grab all anchor tags with article-like hrefs
            rows = [a.parent for a in soup.find_all("a", href=re.compile(r"announcement|notice|board"))]

        for row in rows:
            anchor = row.find("a", href=True)
            if not anchor:
                continue

            href = anchor["href"]
            if not href.startswith("http"):
                href = LINK_PREFIX + href

            external_id = _extract_article_id(href) or href

            if stop_at_id and external_id == stop_at_id:
                hit_stop = True
                break

            title_el = row.select_one(".title, .subject, td.subject, .board-title, strong")
            title = title_el.get_text(strip=True) if title_el else anchor.get_text(strip=True)
            if not title:
                continue

            date_el = row.select_one(".date, td.date, .board-date, time")
            published_at = None
            if date_el:
                published_at = _parse_date(date_el.get_text(strip=True))

            notices.append(
                {
                    "stadium_code": STADIUM_CODE,
                    "notice_type": _classify_notice(title),
                    "title": title,
                    "content": None,  # detail page crawl is optional
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
                print(f"[LG Notice] Saved: {created} new, {updated} updated.")
            except Exception as e:
                session.rollback()
                print(f"[LG Notice] Error: {e}")
            finally:
                self._raw_pages.clear()


if __name__ == "__main__":
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description="Crawl LG Twins operation notices")
    parser.add_argument("--save", action="store_true", help="Save to DB")
    parser.add_argument("--pages", type=int, default=5, help="Max pages to crawl")
    args = parser.parse_args()

    asyncio.run(OperationNoticeLGCrawler(max_pages=args.pages).run(save=args.save))
