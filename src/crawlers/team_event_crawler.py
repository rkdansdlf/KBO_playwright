"""Crawler for KBO and team events/news from official team websites."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import httpx
from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST
from src.crawlers.http_client import CrawlerHttpClient, HttpPolicy
from src.db.engine import SessionLocal
from src.parsers.team_event_parser import parse_team_events
from src.repositories.source_registry_repository import save_raw_snapshots
from src.repositories.team_event_repository import TeamEventRepository
from src.utils.http_client import DEFAULT_HEADERS as HEADERS

if TYPE_CHECKING:
    from src.crawlers.result import CrawlResult

logger = logging.getLogger(__name__)

TEAM_EVENT_CRAWLER_NAME = "team_event_crawler"
TEAM_EVENT_CRAWL_EXCEPTIONS = (httpx.HTTPError, RuntimeError, ValueError, TypeError, OSError)
TEAM_EVENT_SAVE_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)

TEAM_NEWS_SOURCES: dict[str, dict] = {
    "LG": {
        "url": "https://www.lgtwins.com/twins/feed/events?page={page}",
        "link_prefix": "https://www.lgtwins.com",
    },
    "HH": {
        "url": "https://www.hanwhaeagles.co.kr/FA/CN/PCFACN01.do?page={page}",
        "link_prefix": "",
    },
    "OB": {
        "url": "https://www.doosanbears.com/doosan/v1/web/doorun/events?page={page0}&size=8",
        "link_prefix": "https://www.doosanbears.com",
        # This entry used to carry `verify_ssl: False`, added when Doosan's
        # public API served an incomplete TLS chain. The chain now validates
        # under the bundle the shared client uses, so the override only opened
        # this one team to a man in the middle -- the same defect Phase 54
        # removed from `ticket_crawler`.
    },
    "SK": {
        "url": "https://www.ssglanders.com/media/news?page={page}",
        "link_prefix": "https://www.ssglanders.com",
    },
    "NC": {
        "url": "https://www.ncdinos.com/dinos/news.do?newsType=event&pageNo={page}",
        "link_prefix": "https://www.ncdinos.com",
    },
    "HT": {
        "url": "https://www.kiatigers.com/news/notice?page={page}",
        "link_prefix": "https://www.kiatigers.com",
    },
    "LT": {
        "url": "https://www.giantsclub.com/news/notice?page={page}",
        "link_prefix": "https://www.giantsclub.com",
    },
    "SS": {
        "url": "https://www.samsunglions.com/news/notice/list.asp?page={page}",
        "link_prefix": "https://www.samsunglions.com",
    },
    "KT": {
        "url": "https://www.ktwiz.co.kr/news/notice?page={page}",
        "link_prefix": "https://www.ktwiz.co.kr",
    },
    "WO": {
        "url": "https://www.heroesbaseball.co.kr/story/heroesNews/list.do?page={page}",
        "link_prefix": "https://www.heroesbaseball.co.kr/story/heroesNews/",
    },
}

TEAM_TO_SOURCE_KEY = {
    "LG": "lg_twins_events",
    "HH": "hanwha_eagles_events",
    "OB": "doosan_bears_events",
    "SK": "ssg_landers_events",
    "NC": "nc_dinos_events",
    "HT": "kia_tigers_events",
    "LT": "lotte_giants_events",
    "SS": "samsung_lions_events",
    "KT": "kt_wiz_events",
    "WO": "kiwoom_heroes_events",
}


class TeamEventCrawler:
    """TeamEventCrawler class."""

    def __init__(self, days_back: int = 30, http_client: CrawlerHttpClient | None = None) -> None:
        """Initialize a new instance.

        Args:
            days_back: Days Back.
            http_client: Transport to use. Defaults to a client named for this
                crawler. Tests inject a client backed by a mock transport.

        """
        self.days_back = days_back

        # Throttling, retries, redirect validation and TLS verification are the
        # shared client's. The hand-rolled client this replaced verified nothing
        # for Doosan and gave up after one attempt on every team.
        self._http = http_client or CrawlerHttpClient(
            name=TEAM_EVENT_CRAWLER_NAME,
            policy=HttpPolicy(timeout_seconds=15.0),
            headers=HEADERS,
        )
        self.cutoff_date = datetime.now(KST) - timedelta(days=days_back)
        self._raw_pages: list[dict] = []

    async def _fetch_page(self, url: str) -> CrawlResult[str]:
        """Fetch one team page, classifying the outcome.

        A classified result rather than an exception, because the sweep needs to
        tell two failures apart: a page that does not exist ends the pagination,
        while a page that was merely unreachable should not.
        """
        return await self._http.fetch_text(url)

    async def run(self, *, save: bool = False, team_filter: str | None = None) -> list[dict]:
        """Run run.

        Args:
            save: Whether to persist the results.
            team_filter: Team Filter.
            save: Whether to persist the results.
            team_filter: Team Filter.

        Returns:
            List of results.

        """
        all_events = []

        for team_code, config in TEAM_NEWS_SOURCES.items():
            if team_filter and team_code != team_filter:
                continue
            try:
                events = await self._crawl_team(team_code, config)
                all_events.extend(events)
                logger.info("[EVENT] %s: %s events found", team_code, len(events))
            except TEAM_EVENT_CRAWL_EXCEPTIONS:
                logger.exception("Failed to crawl events for %s", team_code)

        logger.info("[EVENT] Total: %s events", len(all_events))

        if save:
            await asyncio.to_thread(self._save_to_db, all_events)
        else:
            for e in all_events[:5]:
                logger.info(e)

        return all_events

    async def _crawl_team(self, team_code: str, config: dict) -> list[dict]:
        events = []
        seen_event_keys: set[tuple[str | None, str | None, str | None]] = set()
        for page in range(1, 4):
            url = config["url"].format(page=page, page0=page - 1)
            result = await self._fetch_page(url)
            if not result.ok:
                # The old code broke out of the pagination on *any* non-200 and
                # continued on a transport error. Preserved here by outcome: a
                # page that will never exist ends the sweep, while one the
                # shared client already retried and could not reach does not
                # take the remaining pages with it.
                logger.warning("Failed to fetch %s: %s", url, result.error_code or result.outcome.value)
                if result.should_retry:
                    continue
                break

            html = result.data
            source_key = TEAM_TO_SOURCE_KEY[team_code]
            self._raw_pages.append(
                {
                    "source_key": source_key,
                    "url": url,
                    "html": html,
                    "status_code": result.http_status,
                },
            )
            metadata = {
                "url": url,
                "cutoff_days": self.days_back,
                "fetched_at": datetime.now(UTC).replace(tzinfo=None).isoformat(),
            }
            page_events = parse_team_events(html, source_key, metadata)
            new_events = []
            for event in page_events:
                event_key = (event.get("team_id"), event.get("title"), event.get("source_url"))
                if event_key in seen_event_keys:
                    continue
                seen_event_keys.add(event_key)
                new_events.append(event)

            events.extend(new_events)
            if not page_events or not new_events:
                break

        return events

    def _save_to_db(self, data: list[dict]) -> None:
        with SessionLocal() as session:
            try:
                saved_snaps = save_raw_snapshots(session, self._raw_pages)
                event_repo = TeamEventRepository(session)
                count = 0
                for item in data:
                    try:
                        event_repo.save(item)
                        count += 1
                    except TEAM_EVENT_SAVE_EXCEPTIONS:
                        logger.exception("Event save failed: %s", item.get("title", "")[:50])
                session.commit()
                logger.info("[EVENT] Saved %s event records, %s snapshots.", count, saved_snaps)
            except TEAM_EVENT_SAVE_EXCEPTIONS:
                session.rollback()
                logger.exception("Event batch save error")
            finally:
                self._raw_pages.clear()


if __name__ == "__main__":
    import argparse
    import asyncio

    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true")
    parser.add_argument("--team", type=str, default=None, help="Team code filter")
    args = parser.parse_args()
    asyncio.run(TeamEventCrawler().run(save=args.save, team_filter=args.team))
