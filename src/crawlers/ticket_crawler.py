"""
Crawler for ticket prices, open rules, and seat information from KBO and team pages.
Sources:
  - KBO ticket info: https://www.koreabaseball.com/Kbo/League/Map.aspx
  - Team ticket pages (LG, etc.)
"""

from __future__ import annotations

import logging
from datetime import datetime, time
from http import HTTPStatus
from typing import Any
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.parsers.ticket_parser import parse_ticket_page
from src.repositories.source_registry_repository import save_raw_snapshots
from src.repositories.ticket_open_rule_repository import TicketOpenRuleRepository
from src.repositories.ticket_price_repository import TicketPriceRepository
from src.utils.http_client import DEFAULT_HEADERS as HEADERS
from src.utils.throttle import throttle

logger = logging.getLogger(__name__)

TICKET_SAVE_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)

# Mapping of teams to their ticket platforms and stadiums
TEAM_TICKET_INFO: dict[str, dict[str, Any]] = {
    "LG": {
        "stadium_id": "JAMSIL",
        "platform": "Ticketlink",
        "ticket_url": "https://www.lgtwins.com/ticket/general",
        "open_offset_days": 7,
        "open_time": time(11, 0),
    },
    "HH": {
        "stadium_id": "HANBAT",
        "platform": "Ticketlink",
        "ticket_url": None,
        "open_offset_days": 7,
        "open_time": time(11, 0),
    },
    "SS": {
        "stadium_id": "DAEGU",
        "platform": "Ticketlink",
        "ticket_url": None,
        "open_offset_days": 7,
        "open_time": time(11, 0),
    },
    "KT": {
        "stadium_id": "SUWON",
        "platform": "Ticketlink",
        "ticket_url": None,
        "open_offset_days": 7,
        "open_time": time(11, 0),
    },
    "HT": {
        "stadium_id": "GWANGJU",
        "platform": "Ticketlink",
        "ticket_url": None,
        "open_offset_days": 7,
        "open_time": time(11, 0),
    },
    "OB": {
        "stadium_id": "JAMSIL",
        "platform": "Interpark",
        "ticket_url": None,
        "open_offset_days": 7,
        "open_time": time(11, 0),
    },
    "WO": {
        "stadium_id": "GOCHEOK",
        "platform": "Interpark",
        "ticket_url": None,
        "open_offset_days": 7,
        "open_time": time(11, 0),
    },
    "SK": {
        "stadium_id": "MUNHAK",
        "platform": "self",
        "ticket_url": "https://www.ssglanders.com/ticket/main",
        "open_offset_days": 7,
        "open_time": time(11, 0),
    },
    "LT": {
        "stadium_id": "SAJIK",
        "platform": "self",
        "ticket_url": "https://www.giantsclub.com/ticket",
        "open_offset_days": 7,
        "open_time": time(11, 0),
    },
    "NC": {
        "stadium_id": "CHANGWON",
        "platform": "self",
        "ticket_url": "https://www.ncdinos.com/ticket",
        "open_offset_days": 7,
        "open_time": time(11, 0),
    },
}


class TicketCrawler:
    def __init__(self) -> None:
        self.kbo_ticket_url = "https://www.koreabaseball.com/Kbo/League/Map.aspx"
        self.current_season = datetime.now().year
        self._raw_pages: list[dict] = []

    TICKET_SOURCE_KEY_MAP: dict[str, str] = {
        "LG": "lg_twins_ticket",
        "HH": "hanwha_eagles_ticket",
        "SS": "samsung_lions_ticket",
        "KT": "kt_wiz_ticket",
        "OB": "doosan_bears_ticket",
        "LT": "lotte_giants_ticket",
        "HT": "kia_tigers_ticket",
        "NC": "nc_dinos_ticket",
        "SK": "ssg_landers_ticket",
        "WO": "kiwoom_heroes_ticket",
    }

    async def run(self, save: bool = False, season: int | None = None) -> list[dict[str, Any]]:
        if season:
            self.current_season = season

        prices = await self._crawl_kbo_ticket_map()
        open_rules = self._build_open_rules()

        if not prices:
            prices = await self._crawl_lg_ticket_page()

        logger.info("[TICKET] Found %s price entries, %s open rules", len(prices), len(open_rules))

        if save:
            self._save_to_db(prices, open_rules)

        return prices

    async def _crawl_kbo_ticket_map(self) -> list[dict[str, Any]]:
        """Crawl the KBO ticket map page to extract team ticket URLs, then crawl each team's page."""
        async with httpx.AsyncClient(headers=HEADERS, timeout=15, follow_redirects=True) as client:
            try:
                host = urlparse(self.kbo_ticket_url).hostname or "koreabaseball.com"
                await throttle.wait(host)
                resp = await client.get(self.kbo_ticket_url)
                if resp.status_code != HTTPStatus.OK:
                    return []
                html = resp.text
                self._raw_pages.append(
                    {
                        "source_key": "kbo_ticket_map",
                        "url": self.kbo_ticket_url,
                        "html": html,
                        "status_code": resp.status_code,
                    },
                )
            except httpx.HTTPError:
                logger.exception("KBO ticket map fetch failed")
                return []

        soup = BeautifulSoup(html, "html.parser")
        team_view = soup.find("ul", class_="teamView")
        if team_view:
            for link in team_view.find_all("a"):
                href = link.get("href", "")
                img = link.find("img")
                alt = img.get("alt", "").strip() if img else ""
                team_code = self._alt_to_team_code(alt)
                if team_code and href:
                    current_url = TEAM_TICKET_INFO[team_code].get("ticket_url")
                    if not current_url and href.startswith("//"):
                        TEAM_TICKET_INFO[team_code]["ticket_url"] = "https:" + href
                    elif not current_url and href.startswith("http"):
                        TEAM_TICKET_INFO[team_code]["ticket_url"] = href

        prices = await self._crawl_team_ticket_pages()
        if not prices:
            prices = await self._crawl_lg_ticket_page()

        return prices

    def _alt_to_team_code(self, alt: str) -> str | None:
        alt_lower = alt.lower()
        for code, kr in [
            ("LG", "lg"),
            ("HH", "한화"),
            ("SS", "삼성"),
            ("KT", "kt"),
            ("OB", "두산"),
            ("LT", "롯데"),
            ("HT", "기아"),
            ("NC", "nc"),
            ("SK", "ssg"),
            ("WO", "키움"),
        ]:
            if kr in alt_lower:
                return code
        return None

    async def _crawl_team_ticket_pages(self) -> list[dict[str, Any]]:
        """Crawl each team's ticket page for price info."""
        all_prices = []
        for team_code, info in TEAM_TICKET_INFO.items():
            if team_code == "LG":
                continue
            url = info.get("ticket_url")
            if not url:
                continue
            try:
                async with httpx.AsyncClient(headers=HEADERS, timeout=15, follow_redirects=True) as c:
                    host = urlparse(url).hostname or "koreabaseball.com"
                    await throttle.wait(host)
                    resp = await c.get(url)
                    if resp.status_code != HTTPStatus.OK:
                        continue
                    html = resp.text
                    source_key = self.TICKET_SOURCE_KEY_MAP.get(team_code, "")
                    self._raw_pages.append(
                        {
                            "source_key": source_key,
                            "url": url,
                            "html": html,
                            "status_code": resp.status_code,
                        },
                    )
                    prices = parse_ticket_page(html, source_key, {"season": self.current_season})
                    all_prices.extend(prices)
            except httpx.HTTPError:
                logger.exception("Failed to crawl ticket page for %s", team_code)

        return all_prices

    async def _crawl_lg_ticket_page(self) -> list[dict[str, Any]]:
        """Crawl LG Twins ticket page for price structure."""
        lg_info = TEAM_TICKET_INFO["LG"]
        url = lg_info["ticket_url"]
        if not url:
            return []

        try:
            async with httpx.AsyncClient(headers=HEADERS, timeout=15, follow_redirects=True) as c:
                host = urlparse(url).hostname or "koreabaseball.com"
                await throttle.wait(host)
                resp = await c.get(url)
                if resp.status_code != HTTPStatus.OK:
                    return []
                html = resp.text
                self._raw_pages.append(
                    {
                        "source_key": "lg_twins_ticket",
                        "url": url,
                        "html": html,
                        "status_code": resp.status_code,
                    },
                )
        except httpx.HTTPError:
            logger.exception("LG ticket page fetch failed")
            return []

        return parse_ticket_page(html, "lg_twins_ticket", {"season": self.current_season})

    def _build_open_rules(self) -> list[dict[str, Any]]:
        """Build initial ticket open rules from known data."""
        rules = []
        for team_code, info in TEAM_TICKET_INFO.items():
            rules.append(
                {
                    "team_id": team_code,
                    "platform": info["platform"],
                    "open_offset_days": info["open_offset_days"],
                    "open_time": info["open_time"],
                    "source_id": None,
                    "note": None,
                },
            )
        return rules

    def _team_code_to_kr(self, code: str) -> str | None:
        mapping = {
            "LG": "LG",
            "HH": "한화",
            "SS": "삼성",
            "KT": "KT",
            "OB": "두산",
            "LT": "롯데",
            "HT": "KIA",
            "NC": "NC",
            "SK": "SSG",
            "WO": "키움",
        }
        return mapping.get(code)

    def _save_to_db(self, prices: list[dict], open_rules: list[dict]) -> None:
        with SessionLocal() as session:
            try:
                saved_snaps = save_raw_snapshots(session, self._raw_pages)

                price_repo = TicketPriceRepository(session)
                price_count = 0
                for item in prices:
                    try:
                        price_repo.save(item)
                        price_count += 1
                    except TICKET_SAVE_EXCEPTIONS:
                        logger.exception("Ticket price save failed: %s", item)

                rule_repo = TicketOpenRuleRepository(session)
                rule_count = 0
                for item in open_rules:
                    try:
                        rule_repo.save(item)
                        rule_count += 1
                    except TICKET_SAVE_EXCEPTIONS:
                        logger.exception("Open rule save failed: %s", item)

                session.commit()
                logger.info("[TICKET] Saved %s prices, %s rules, %s snapshots.", price_count, rule_count, saved_snaps)
            except TICKET_SAVE_EXCEPTIONS:
                session.rollback()
                logger.exception("Ticket price batch save error")
            finally:
                self._raw_pages.clear()


if __name__ == "__main__":
    import argparse
    import asyncio

    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true")
    parser.add_argument("--season", type=int, default=None, help="Season year")
    args = parser.parse_args()
    asyncio.run(TicketCrawler().run(save=args.save, season=args.season))
