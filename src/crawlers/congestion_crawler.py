"""Congestion crawler for Jamsil Stadium area.

Collects real-time congestion data from:
  1. 서울시 실시간 도시데이터 API (Seoul Open Data) — primary
  2. Naver/Kakao place popular times — secondary (scraping)

Coverage:
  - 잠실 야구장 권역 (overall area)
  - 잠실역 2호선 (subway station)
  - 잠실역 8호선 (subway station)
  - 몽촌토성역 8호선 (subway station)
  - 주요 게이트 구역 (gate zones, via proxied area codes)

Scheduling recommendation:
  - Run every 5 minutes between D-3h and D+2h on game days.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any

import httpx
from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST
from src.db.engine import SessionLocal
from src.repositories.congestion_repository import CongestionRepository
from src.utils.date_helpers import parse_date_str
from src.utils.seoul_api_client import CongestionSnapshot, get_jamsil_congestion_batch

logger = logging.getLogger(__name__)

STADIUM_CODE = "JAMSIL"
CONGESTION_CRAWL_EXCEPTIONS = (httpx.HTTPError, RuntimeError, ValueError, TypeError, KeyError, OSError)

# Additional location metadata for DB records
AREA_LOCATION_META: dict[str, dict] = {
    "잠실 야구장": {"location_type": "area", "location_label": "잠실야구장_권역"},
    "잠실역(2호선)": {"location_type": "subway_station", "location_label": "잠실역_2호선"},
    "석촌호수(동호)": {"location_type": "area", "location_label": "석촌호수_동호"},
}


def _snapshot_to_record(
    snap: CongestionSnapshot,
    game_date: date,
    measured_at: datetime,
) -> dict[str, Any]:
    meta = AREA_LOCATION_META.get(
        snap.location_label,
        {"location_type": "area", "location_label": snap.location_label},
    )
    return {
        "stadium_code": STADIUM_CODE,
        "location_type": meta["location_type"],
        "location_label": meta["location_label"],
        "measured_at": measured_at,
        "game_date": game_date,
        "congestion_level": snap.congestion_level,
        "congestion_index": snap.congestion_index,
        "people_count": snap.people_count,
        "source": snap.source,
        "raw_data": snap.raw_data,
    }


class CongestionCrawler:
    """Orchestrates congestion data collection from multiple sources."""

    def __init__(self, stadium_code: str = STADIUM_CODE) -> None:
        """Initializes a new instance."""
        self.stadium_code = stadium_code

    async def run(
        self,
        game_date: date | None = None,
        *,
        save: bool = False,
    ) -> list[dict]:
        """Runs run.

        Args:
            game_date: Game Date.

        Returns:
            List of results.

        """
        game_date = game_date or datetime.now(KST).date()
        measured_at = datetime.now(UTC).replace(tzinfo=None)

        logger.info("[Congestion] Collecting for %s at %s UTC", game_date, measured_at.strftime("%H:%M"))

        # Source 1: Seoul Open Data API
        snapshots = await self._collect_seoul_api()
        logger.info("[Congestion] Seoul API: %s zones", len(snapshots))

        records = [_snapshot_to_record(snap, game_date, measured_at) for snap in snapshots]

        logger.info("[Congestion] Total records: %s", len(records))

        if save:
            self._save_to_db(records)
        else:
            for rec in records:
                logger.info(
                    "  %s | %s | level=%s | index=%s",
                    rec["location_label"],
                    rec["location_type"],
                    rec["congestion_level"],
                    rec["congestion_index"],
                )

        return records

    async def _collect_seoul_api(self) -> list[CongestionSnapshot]:
        try:
            return await get_jamsil_congestion_batch()
        except CONGESTION_CRAWL_EXCEPTIONS:
            logger.exception("[Congestion] Seoul API batch failed")
            return []

    def _save_to_db(self, records: list[dict]) -> None:
        with SessionLocal() as session:
            try:
                repo = CongestionRepository(session)
                created, updated = repo.bulk_upsert(records)
                session.commit()
                logger.info("[Congestion] Saved: %s new, %s updated.", created, updated)
            except SQLAlchemyError:
                session.rollback()
                logger.exception("[Congestion] Database error")


if __name__ == "__main__":
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description="Collect congestion data for Jamsil Stadium")
    parser.add_argument("--save", action="store_true", help="Save to DB")
    parser.add_argument("--game-date", type=str, default=None, help="Game date YYYYMMDD")
    args = parser.parse_args()

    gdate = None
    if args.game_date:
        gdate = parse_date_str(args.game_date)

    asyncio.run(CongestionCrawler().run(game_date=gdate, save=args.save))
