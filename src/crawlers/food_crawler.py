"""
Crawler for stadium food vendor and menu information from team websites.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.repositories.source_registry_repository import save_raw_snapshots
from src.repositories.stadium_food_repository import StadiumFoodMenuItemRepository, StadiumFoodVendorRepository
from src.utils.http_client import DEFAULT_HEADERS as HEADERS
from src.utils.safe_print import safe_print as print
from src.utils.throttle import throttle

logger = logging.getLogger(__name__)

TEAM_FOOD_SOURCES: dict[str, dict[str, Any]] = {
    "LT": {
        "source_key": "lotte_giants_fnb",
        "stadium_id": "SAJIK",
        "url": "https://www.giantsclub.com/food",
    },
    "NC": {
        "source_key": "nc_dinos_food_seat",
        "stadium_id": "CHANGWON",
        "url": "https://www.ncdinos.com/stadium/food",
    },
}

MENU_PATTERN = re.compile(r"([가-힣a-zA-Z0-9\s]{2,30})\s*:?\s*(\d{1,3}(?:,\d{3})*)\s*(?:원)")


class FoodCrawler:
    def __init__(self):
        self._raw_pages: list[dict] = []

    async def run(self, save: bool = False, team_filter: str | None = None) -> list[dict[str, Any]]:
        all_vendors = []
        for team_code, info in TEAM_FOOD_SOURCES.items():
            if team_filter and team_code != team_filter:
                continue
            try:
                vendors = await self._crawl_team_food(team_code, info)
                all_vendors.extend(vendors)
                print(f"[FOOD] {team_code}: {len(vendors)} vendors found")
            except Exception:
                logger.exception("Failed to crawl food for %s", team_code)

        print(f"[FOOD] Total: {len(all_vendors)} vendors")
        if save:
            self._save_to_db(all_vendors)
        else:
            for v in all_vendors[:5]:
                print(v)
        return all_vendors

    async def _crawl_team_food(self, team_code: str, info: dict) -> list[dict[str, Any]]:
        vendors = []
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
                vendors = self._parse_food_page(html, info)
            except httpx.HTTPError:
                logger.exception("Failed to fetch food page for %s", team_code)
        return vendors

    def _parse_food_page(self, html: str, info: dict) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        vendors = []

        menu_matches = MENU_PATTERN.findall(text)
        menus = []
        for name, price_str in menu_matches:
            menus.append(
                {
                    "menu_name": name.strip(),
                    "price": int(price_str.replace(",", "")),
                    "category": "etc",
                }
            )

        if menus:
            vendors.append(
                {
                    "vendor": {
                        "stadium_id": info["stadium_id"],
                        "vendor_name": f"{info['stadium_id']} 구장 매점",
                        "order_method": "onsite",
                        "confidence": "low",
                    },
                    "menus": menus,
                }
            )

        return vendors

    def _save_to_db(self, data: list[dict]) -> None:
        with SessionLocal() as session:
            try:
                saved_snaps = save_raw_snapshots(session, self._raw_pages)
                vendor_repo = StadiumFoodVendorRepository(session)
                menu_repo = StadiumFoodMenuItemRepository(session)
                vendor_count = 0
                menu_count = 0
                for entry in data:
                    try:
                        vendor = vendor_repo.save(entry["vendor"])
                        vendor_count += 1
                        for menu in entry.get("menus", []):
                            menu_repo.save({"vendor_id": vendor.id, **menu})
                            menu_count += 1
                    except Exception:
                        logger.exception("Food save failed: %s", entry.get("vendor", {}).get("vendor_name", ""))
                session.commit()
                print(f"[FOOD] Saved {vendor_count} vendors, {menu_count} menus, {saved_snaps} snapshots.")
            except SQLAlchemyError as e:
                session.rollback()
                logger.error(f"[FOOD] Database error: {e}", exc_info=True)
                print(f"[FOOD] Error: {e}")
            finally:
                self._raw_pages.clear()
