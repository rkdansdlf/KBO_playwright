"""Crawler for stadium seat section information from team websites."""

from __future__ import annotations

import asyncio
import logging
import re
from typing import TYPE_CHECKING, Any

import httpx
from bs4 import BeautifulSoup
from sqlalchemy.exc import SQLAlchemyError

from src.crawlers.base import BaseHttpCrawler
from src.crawlers.http_client import CrawlerHttpClient, HttpPolicy
from src.db.engine import SessionLocal
from src.repositories.source_registry_repository import save_raw_snapshots
from src.repositories.stadium_seat_section_repository import StadiumSeatSectionRepository
from src.utils.http_client import DEFAULT_HEADERS as HEADERS

if TYPE_CHECKING:
    from src.crawlers.result import CrawlResult
    from src.utils.request_policy import RequestPolicy

logger = logging.getLogger(__name__)

SEAT_CRAWLER_NAME = "seat_crawler"
SEAT_CRAWL_EXCEPTIONS = (httpx.HTTPError, RuntimeError, ValueError, TypeError, OSError)
SEAT_SAVE_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)
MIN_SEAT_SECTION_NAME_LENGTH = 2

TEAM_SEAT_SOURCES: dict[str, dict[str, Any]] = {
    "LG": {
        "source_key": "lg_twins_seat",
        "stadium_id": "JAMSIL",
        "url": "https://www.lgtwins.com/ticket/seatinfo",
    },
    "OB": {
        "source_key": "seoul_stadium_seat",
        "stadium_id": "JAMSIL",
        "url": "https://www.lgtwins.com/ticket/seatinfo",
    },
}

SECTION_PATTERNS = [
    re.compile(r"([가-힣]+(?:석|존|zone|Zone))"),
    re.compile(r"(블루|오렌지|레드|네이비|그린|화이트|골드|[1-3][Ff])\s*(.*?)(?:석|존)"),
]


class SeatCrawler(BaseHttpCrawler):
    """SeatCrawler class."""

    def __init__(
        self,
        request_delay: float = 0.5,
        policy: RequestPolicy | None = None,
        http_client: CrawlerHttpClient | None = None,
    ) -> None:
        """Initialize SeatCrawler.

        Args:
            request_delay: Request delay in seconds.
            policy: Optional request policy.
            http_client: Transport to use. Defaults to a client named for this
                crawler. Tests inject a client backed by a mock transport.

        """
        super().__init__(request_delay=request_delay, policy=policy, default_headers=HEADERS)
        # The shared client already waits on an adaptive rate limiter, retries
        # with backoff, and validates every redirect target. The hand-rolled
        # `httpx.AsyncClient` this replaced carried none of that, so a failing
        # team page lost the request after one attempt.
        self._http = http_client or CrawlerHttpClient(
            name=SEAT_CRAWLER_NAME,
            policy=HttpPolicy(base_delay_seconds=request_delay, timeout_seconds=15.0),
            headers=HEADERS,
        )
        self._raw_pages: list[dict] = []

    async def run(self, *, save: bool = False, team_filter: str | None = None) -> list[dict[str, Any]]:
        """Run run.

        Args:
            save: Whether to persist the results.
            team_filter: Team Filter.
            save: Whether to persist the results.
            team_filter: Team Filter.

        Returns:
            List of results.

        """
        all_sections = []

        for team_code, info in TEAM_SEAT_SOURCES.items():
            if team_filter and team_code != team_filter:
                continue
            try:
                sections = await self._crawl_team_seats(team_code, info)
                all_sections.extend(sections)
                logger.info("[SEAT] %s: %s sections found", team_code, len(sections))
            except SEAT_CRAWL_EXCEPTIONS:
                logger.exception("Failed to crawl seats for %s", team_code)

        logger.info("[SEAT] Total: %s sections", len(all_sections))

        if save:
            await asyncio.to_thread(self._save_to_db, all_sections)
        else:
            for s in all_sections[:5]:
                logger.info(s)

        return all_sections

    async def _fetch_seat_page(self, url: str) -> CrawlResult[str]:
        """Fetch one seat page, classifying the outcome.

        Split from :meth:`_crawl_team_seats` so the transport reports a typed
        result rather than an empty list. The two are indistinguishable from the
        outside -- a 404, a timeout and a page with no seat names on it all
        produced `[]` -- and only one of them is a reason to look again.
        """
        return await self._http.fetch_text(url)

    async def _crawl_team_seats(self, team_code: str, info: dict) -> list[dict[str, Any]]:
        sections = []
        try:
            result = await self._fetch_seat_page(info["url"])
            if not result.ok:
                # The taxonomy code is already attached, so a failure carries one
                # name from here into the ledger and the metric rather than being
                # re-derived from a message later.
                logger.warning(
                    "Seat page fetch failed for %s: %s (%s)",
                    team_code,
                    result.error_code or result.outcome.value,
                    result.url or info["url"],
                )
                return []
            html = result.data
            self._raw_pages.append(
                {
                    "source_key": info["source_key"],
                    "url": info["url"],
                    "html": html,
                    "status_code": result.http_status,
                },
            )
            sections = self._parse_seat_page(html, team_code, info)
        except SEAT_CRAWL_EXCEPTIONS:
            logger.exception("Failed to parse seat page for %s", team_code)
        return sections

    def _parse_seat_page(self, html: str, _team_code: str, info: dict) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        sections = []
        seen = set()

        for pattern in SECTION_PATTERNS:
            for match in pattern.finditer(text):
                name = match.group(0).strip()
                if name in seen or len(name) < MIN_SEAT_SECTION_NAME_LENGTH:
                    continue
                seen.add(name)
                sections.append(
                    {
                        "stadium_id": info["stadium_id"],
                        "section_name": name,
                        "section_code": name,
                        "seat_grade": name,
                        "source_id": None,
                    },
                )

        return sections

    def _save_to_db(self, data: list[dict]) -> None:
        with SessionLocal() as session:
            try:
                saved_snaps = save_raw_snapshots(session, self._raw_pages)
                repo = StadiumSeatSectionRepository(session)
                count = 0
                for item in data:
                    try:
                        repo.save(item)
                        count += 1
                    except SEAT_SAVE_EXCEPTIONS:
                        logger.exception("Seat section save failed: %s", item.get("section_name", ""))
                session.commit()
                logger.info("[SEAT] Saved %s section records, %s snapshots.", count, saved_snaps)
            except SEAT_SAVE_EXCEPTIONS:
                session.rollback()
                logger.exception("Seat batch save error")
            finally:
                self._raw_pages.clear()
