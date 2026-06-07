"""
Crawler for KBO and team events/news from official team websites.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import httpx

from src.db.engine import SessionLocal
from src.parsers.team_event_parser import parse_team_events
from src.repositories.source_registry_repository import save_raw_snapshots
from src.repositories.team_event_repository import TeamEventRepository
from src.utils.http_client import DEFAULT_HEADERS as HEADERS
from src.utils.safe_print import safe_print as print
from src.utils.throttle import throttle

logger = logging.getLogger(__name__)

TEAM_NEWS_SOURCES: dict[str, dict] = {
    "LG": {
        "url": "https://www.lgtwins.com/service/announcement?pageNo={page}",
        "link_prefix": "https://www.lgtwins.com",
    },
    "HH": {
        "url": "https://www.hanwhaeagles.co.kr/FA/CN/PCFACN01.do?page={page}",
        "link_prefix": "",
    },
    "OB": {
        "url": "https://www.doosanbears.com/event/board?page={page}",
        "link_prefix": "https://www.doosanbears.com",
    },
    "SK": {
        "url": "https://www.ssglanders.com/news/notice?pageIndex={page}",
        "link_prefix": "https://www.ssglanders.com",
    },
    "NC": {
        "url": "https://www.ncdinos.com/notice?page={page}",
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
        "url": "https://www.heroesbaseball.co.kr/news/noticeList?page={page}",
        "link_prefix": "https://www.heroesbaseball.co.kr",
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
    def __init__(self, days_back: int = 30):
        self.days_back = days_back
        self.cutoff_date = datetime.now() - timedelta(days=days_back)
        self._raw_pages: list[dict] = []

    async def run(self, save: bool = False, team_filter: str | None = None) -> list[dict]:
        all_events = []
        for team_code, config in TEAM_NEWS_SOURCES.items():
            if team_filter and team_code != team_filter:
                continue
            try:
                events = await self._crawl_team(team_code, config)
                all_events.extend(events)
                print(f"[EVENT] {team_code}: {len(events)} events found")
            except Exception:
                logger.exception("Failed to crawl events for %s", team_code)

        print(f"[EVENT] Total: {len(all_events)} events")

        if save:
            self._save_to_db(all_events)
        else:
            for e in all_events[:5]:
                print(e)

        return all_events

    async def _crawl_team(self, team_code: str, config: dict) -> list[dict]:
        events = []
        async with httpx.AsyncClient(headers=HEADERS, timeout=15, follow_redirects=True) as client:
            for page in range(1, 4):
                url = config["url"].format(page=page)
                try:
                    host = urlparse(url).hostname or "koreabaseball.com"
                    await throttle.wait(host)
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        break
                    html = resp.text
                    source_key = TEAM_TO_SOURCE_KEY[team_code]
                    self._raw_pages.append(
                        {
                            "source_key": source_key,
                            "url": url,
                            "html": html,
                            "status_code": resp.status_code,
                        }
                    )
                    metadata = {
                        "url": url,
                        "cutoff_days": self.days_back,
                        "fetched_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
                    }
                    page_events = parse_team_events(html, source_key, metadata)
                    events.extend(page_events)
                    if not page_events:
                        break
                except httpx.HTTPError:
                    logger.warning("Failed to fetch %s", url)
                    continue

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
                    except Exception:
                        logger.exception("Event save failed: %s", item.get("title", "")[:50])
                session.commit()
                print(f"[EVENT] Saved {count} event records, {saved_snaps} snapshots.")
            except Exception as e:
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
