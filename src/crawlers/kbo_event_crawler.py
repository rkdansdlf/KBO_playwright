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
KBO_EVENT_DEFAULT_URLS = (
    "https://www.koreabaseball.com/Kbo/BusinessAndEvent/Mvp.aspx",
    "https://www.koreabaseball.com/Kbo/BusinessAndEvent/Draft.aspx",
    "https://www.koreabaseball.com/Kbo/BusinessAndEvent/MediaDay.aspx",
    "https://www.koreabaseball.com/Kbo/BusinessAndEvent/RecordClass/LessonInfo.aspx",
    "https://www.koreabaseball.com/Kbo/BusinessAndEvent/SafeGuide.aspx",
    "https://www.koreabaseball.com/Kbo/BusinessAndEvent/KboArchive/PurchaseGuide.aspx",
    "https://www.koreabaseball.com/Kbo/BusinessAndEvent/OnSiteViewingSupport.aspx",
)
KBO_EVENT_KEYWORDS = ("이벤트", "event", "프로모션", "행사")
KBO_EVENT_CRAWL_EXCEPTIONS = (PlaywrightError, PlaywrightTimeoutError, RuntimeError, ValueError, TypeError, OSError)
KBO_EVENT_SAVE_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError, OSError)
GENERIC_PAGE_TITLES = {"메인", "신청하기", "신청확인"}
GENERIC_LINK_TITLES = {"신청하기", "신청 확인", "신청확인", "행사 개요"}


def extract_kbo_event_links(html: str, base_url: str = KBO_EVENT_BASE_URL) -> list[dict[str, object]]:
    """Extract likely KBO official event/promotion links from a page."""
    soup = BeautifulSoup(html, "html.parser")
    events: list[dict[str, object]] = []
    seen_urls: set[str] = set()
    for link in soup.select("a[href]"):
        title = link.get_text(" ", strip=True)
        href = link.get("href") or ""
        normalized_href = href.strip().lower()
        if normalized_href.startswith(("#", "javascript:")) or title in GENERIC_LINK_TITLES:
            continue
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


def extract_kbo_event_page(html: str, source_url: str) -> dict[str, object] | None:
    """Build one event payload for an official KBO business/event page."""
    soup = BeautifulSoup(html, "html.parser")
    title = _extract_page_title(soup)
    if not title:
        return None
    return _build_event_payload(title, source_url)


def _extract_page_title(soup: BeautifulSoup) -> str | None:
    title_node = soup.select_one("title")
    if not title_node:
        return None
    parts = [part.strip() for part in title_node.get_text(" ", strip=True).split("|") if part.strip()]
    for part in parts:
        if part not in GENERIC_PAGE_TITLES and part not in {"KBO", "주요 사업/행사"}:
            return part[:300]
    return None


def _build_event_payload(title: str, source_url: str) -> dict[str, object]:
    return {
        "event_scope": "kbo",
        "team_id": None,
        "title": title[:300],
        "description": None,
        "event_type": "promotion",
        "source_url": source_url,
        "published_at": None,
        "last_seen_at": datetime.now(UTC).replace(tzinfo=None),
        "status": "unknown",
    }


class KboEventCrawler:
    """Fetch KBO official page and extract event/promotion link candidates."""

    def __init__(self, base_url: str | None = None) -> None:
        """Initializes a new instance."""
        self.urls = (base_url,) if base_url else KBO_EVENT_DEFAULT_URLS
        self._raw_pages: list[dict[str, object]] = []

    async def run(self, *, save: bool = False) -> list[dict[str, object]]:
        """Runs run.

        Returns:
            List of results.

        """
        events: list[dict[str, object]] = []
        seen_urls: set[str] = set()
        for url in self.urls:
            html, final_url = await self._fetch_html(url)
            self._raw_pages.append(
                {
                    "source_key": KBO_EVENT_SOURCE_KEY,
                    "url": final_url,
                    "html": html,
                    "status_code": 200,
                },
            )
            page_event = extract_kbo_event_page(html, final_url)
            if page_event and final_url not in seen_urls:
                events.append(page_event)
                seen_urls.add(final_url)
            for event in extract_kbo_event_links(html, final_url):
                source_url = str(event["source_url"])
                if source_url in seen_urls:
                    continue
                events.append(event)
                seen_urls.add(source_url)
        logger.info("[KBO_EVENT] Found %s official event link candidates.", len(events))
        if save:
            self._save_to_db(events)
        return events

    async def _fetch_html(self, url: str) -> tuple[str, str]:
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
            await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            await page.wait_for_timeout(1500)
            return await page.content(), page.url
        except KBO_EVENT_CRAWL_EXCEPTIONS:
            logger.exception("[KBO_EVENT] Failed to fetch %s", url)
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
