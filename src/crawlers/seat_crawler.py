"""
Crawler for stadium seat section information from team websites.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from src.db.engine import SessionLocal
from src.repositories.source_registry_repository import save_raw_snapshots
from src.repositories.stadium_seat_section_repository import StadiumSeatSectionRepository
from src.utils.http_client import DEFAULT_HEADERS as HEADERS
from src.utils.throttle import throttle

logger = logging.getLogger(__name__)

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


class SeatCrawler:
    def __init__(self) -> None:
        self._raw_pages: list[dict] = []

    async def run(self, save: bool = False, team_filter: str | None = None) -> list[dict[str, Any]]:
        all_sections = []
        for team_code, info in TEAM_SEAT_SOURCES.items():
            if team_filter and team_code != team_filter:
                continue
            try:
                sections = await self._crawl_team_seats(team_code, info)
                all_sections.extend(sections)
                logger.info(f"[SEAT] {team_code}: {len(sections)} sections found")
            except Exception:
                logger.exception("Failed to crawl seats for %s", team_code)

        logger.info(f"[SEAT] Total: {len(all_sections)} sections")

        if save:
            self._save_to_db(all_sections)
        else:
            for s in all_sections[:5]:
                logger.info(s)

        return all_sections

    async def _crawl_team_seats(self, team_code: str, info: dict) -> list[dict[str, Any]]:
        sections = []
        async with httpx.AsyncClient(headers=HEADERS, timeout=15, follow_redirects=True) as client:
            try:
                host = urlparse(info["url"]).hostname or "koreabaseball.com"
                await throttle.wait(host)
                resp = await client.get(info["url"])
                if resp.status_code != 200:
                    return []
                html = resp.text
                self._raw_pages.append(
                    {
                        "source_key": info["source_key"],
                        "url": info["url"],
                        "html": html,
                        "status_code": resp.status_code,
                    }
                )
                sections = self._parse_seat_page(html, team_code, info)
            except httpx.HTTPError:
                logger.exception("Failed to fetch seat page for %s", team_code)
        return sections

    def _parse_seat_page(self, html: str, team_code: str, info: dict) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        sections = []
        seen = set()

        for pattern in SECTION_PATTERNS:
            for match in pattern.finditer(text):
                name = match.group(0).strip()
                if name in seen or len(name) < 2:
                    continue
                seen.add(name)
                sections.append(
                    {
                        "stadium_id": info["stadium_id"],
                        "section_name": name,
                        "section_code": name,
                        "seat_grade": name,
                        "source_id": None,
                    }
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
                    except Exception:
                        logger.exception("Seat section save failed: %s", item.get("section_name", ""))
                session.commit()
                logger.info(f"[SEAT] Saved {count} section records, {saved_snaps} snapshots.")
            except Exception:
                session.rollback()
                logger.exception("Seat batch save error")
            finally:
                self._raw_pages.clear()
