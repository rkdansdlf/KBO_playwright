"""Crawler for KBO official event/promotion links."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.repositories.source_registry_repository import DataSourceRepository, save_raw_snapshots
from src.repositories.team_event_repository import TeamEventRepository
from src.utils.playwright_pool import AsyncPlaywrightPool

logger = logging.getLogger(__name__)

KBO_EVENT_SOURCE_KEY = "kbo_official_events"
KBO_EVENT_BASE_URL = "https://www.koreabaseball.com"
KBO_EVENT_KEYWORDS = ("이벤트", "event", "프로모션", "행사")
KBO_EVENT_CRAWL_EXCEPTIONS = (PlaywrightError, PlaywrightTimeoutError, RuntimeError, ValueError, TypeError, OSError)
KBO_EVENT_SAVE_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError, OSError)


def extract_kbo_event_links(html: str, base_url: str = KBO_EVENT_BASE_URL) -> list[dict[str, object]]:
    """Extract likely KBO official event/promotion links from a page."""
    soup = BeautifulSoup(html, "html.parser")
    events: list[dict[str, object]] = []
    seen_urls: set[str] = set()
    for link in soup.select("a[href]"):
        title = link.get_text(" ", strip=True)
        href = link.get("href") or ""
        haystack = f"{title} {href}".lower()
        if not title or not any(keyword.lower() in haystack for keyword in KBO_EVENT_KEYWORDS):
            continue
        source_url = urljoin(base_url, href)
        if source_url in seen_urls:
            continue
        seen_urls.add(source_url)
        events.append(
            {
                "event_scope": "kbo",
                "team_id": None,
                "title": title[:300],
                "description": None,
                "event_type": "promotion",
                "source_url": source_url,
                "published_at": None,
                "last_seen_at": datetime.now(UTC).replace(tzinfo=None),
                "status": "unknown",
            },
        )
    return events


class KboEventCrawler:
    """Fetch KBO official page and extract event/promotion link candidates."""

    def __init__(self, base_url: str = KBO_EVENT_BASE_URL) -> None:
        self.base_url = base_url
        self._raw_pages: list[dict[str, object]] = []

    async def run(self, *, save: bool = False) -> list[dict[str, object]]:
        html = await self._fetch_html()
        self._raw_pages.append(
            {
                "source_key": KBO_EVENT_SOURCE_KEY,
                "url": self.base_url,
                "html": html,
                "status_code": 200,
            },
        )
        events = extract_kbo_event_links(html, self.base_url)
        logger.info("[KBO_EVENT] Found %s official event link candidates.", len(events))
        if save:
            self._save_to_db(events)
        return events

    async def _fetch_html(self) -> str:
        pool = AsyncPlaywrightPool(
            max_pages=1,
            context_kwargs={
                "locale": "ko-KR",
                "timezone_id": "Asia/Seoul",
                "viewport": {"width": 1920, "height": 1080},
            },
        )
        await pool.start()
        page = await pool.acquire()
        try:
            await page.goto(self.base_url, wait_until="domcontentloaded", timeout=30_000)
            await page.wait_for_timeout(1500)
            return await page.content()
        except KBO_EVENT_CRAWL_EXCEPTIONS:
            logger.exception("[KBO_EVENT] Failed to fetch %s", self.base_url)
            raise
        finally:
            await pool.release(page)
            await pool.close()

    def _save_to_db(self, events: list[dict[str, object]]) -> None:
        with SessionLocal() as session:
            try:
                saved_snaps = save_raw_snapshots(session, self._raw_pages)
                source = DataSourceRepository(session).get_by_key(KBO_EVENT_SOURCE_KEY)
                repo = TeamEventRepository(session)
                saved_events = 0
                for event in events:
                    payload = dict(event)
                    if source:
                        payload["source_id"] = source.id
                    repo.save(payload)
                    saved_events += 1
                session.commit()
                logger.info("[KBO_EVENT] Saved %s events, %s snapshots.", saved_events, saved_snaps)
            except KBO_EVENT_SAVE_EXCEPTIONS:
                session.rollback()
                logger.exception("[KBO_EVENT] Save failed")
                raise
