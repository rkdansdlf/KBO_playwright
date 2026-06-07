"""
Crawler for stadium parking lot and fee information from team websites.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from src.db.engine import SessionLocal
from src.repositories.parking_lot_repository import ParkingFeeRuleRepository, ParkingLotRepository
from src.repositories.source_registry_repository import save_raw_snapshots
from src.utils.http_client import DEFAULT_HEADERS as HEADERS
from src.utils.throttle import throttle

logger = logging.getLogger(__name__)

TEAM_PARKING_SOURCES: dict[str, dict[str, Any]] = {
    "SK": {
        "source_key": "ssg_landers_parking",
        "stadium_id": "MUNHAK",
        "url": "https://www.ssglanders.com/stadium/parking",
    },
    "SS": {
        "source_key": "daegu_parking",
        "stadium_id": "DAEGU",
        "url": "https://www.samsunglions.com/stadium/waytocome",
    },
}

PARKING_FEE_PATTERN = re.compile(
    r"(기본|추가|일일|행사|경기|무료)\s*(?:요금|시간|금액)?\s*:?\s*(\d{1,3}(?:,\d{3})*)\s*(?:원)"
)


class ParkingCrawler:
    def __init__(self):
        self._raw_pages: list[dict] = []

    async def run(self, save: bool = False, team_filter: str | None = None) -> list[dict[str, Any]]:
        all_lots = []
        for team_code, info in TEAM_PARKING_SOURCES.items():
            if team_filter and team_code != team_filter:
                continue
            try:
                lots = await self._crawl_team_parking(team_code, info)
                all_lots.extend(lots)
                logger.info(f"[PARKING] {team_code}: {len(lots)} lots found")
            except Exception:
                logger.exception("Failed to crawl parking for %s", team_code)

        logger.info(f"[PARKING] Total: {len(all_lots)} lots")
        if save:
            self._save_to_db(all_lots)
        else:
            for lot in all_lots[:5]:
                logger.info(lot)
        return all_lots

    async def _crawl_team_parking(self, team_code: str, info: dict) -> list[dict[str, Any]]:
        lots = []
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
                lots = self._parse_parking_page(html, info)
            except httpx.HTTPError:
                logger.exception("Failed to fetch parking page for %s", team_code)
        return lots

    def _parse_parking_page(self, html: str, info: dict) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        lots = []

        fees = []
        for match in PARKING_FEE_PATTERN.finditer(text):
            label, amount = match.group(1), match.group(2).replace(",", "")
            fees.append({"label": label, "amount": int(amount)})

        lot_name = f"{info['stadium_id']} 주차장"
        lot_data = {
            "stadium_id": info["stadium_id"],
            "name": lot_name,
            "lot_type": "official",
            "is_event_day_available": True,
            "reservation_required": False,
        }
        lots.append({"lot": lot_data, "fee_rules": fees})

        return lots

    def _save_to_db(self, data: list[dict]) -> None:
        with SessionLocal() as session:
            try:
                saved_snaps = save_raw_snapshots(session, self._raw_pages)
                lot_repo = ParkingLotRepository(session)
                fee_repo = ParkingFeeRuleRepository(session)
                lot_count = 0
                fee_count = 0
                for entry in data:
                    try:
                        lot = lot_repo.save(entry["lot"])
                        lot_count += 1
                        for fee in entry.get("fee_rules", []):
                            fee_repo.save({"parking_lot_id": lot.id, **fee})
                            fee_count += 1
                    except Exception:
                        logger.exception("Parking save failed: %s", entry.get("lot", {}).get("name", ""))
                session.commit()
                logger.info(f"[PARKING] Saved {lot_count} lots, {fee_count} fee rules, {saved_snaps} snapshots.")
            except Exception:
                session.rollback()
                logger.exception("Parking batch save error")
            finally:
                self._raw_pages.clear()
