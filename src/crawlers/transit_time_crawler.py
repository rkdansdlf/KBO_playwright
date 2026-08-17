"""Transit time crawler for Jamsil Stadium.

Measures real travel durations from subway stations and bus stops
near Jamsil Stadium at regular intervals on game days.

Configured origin points for JAMSIL:
  - 잠실역 2호선 7번출구 (Line 2)
  - 잠실역 8호선 4번출구 (Line 8, shared station)
  - 몽촌토성역 8호선 1번출구 (Line 8)
  - 잠실나루역 2호선 2번출구 (Line 2)
  - 잠실역 환승센터 버스 정류장 (Bus hub)

Scheduling recommendation (via scheduler.py):
  - Run every 15 minutes between D-2h and D+1h on game days.

"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from src.constants import KST
from src.crawlers.base import BaseCrawler
from src.db.engine import SessionLocal
from src.repositories.transit_time_repository import TransitTimeRepository
from src.utils.map_api_client import get_transit_times_batch

if TYPE_CHECKING:
    from src.utils.request_policy import RequestPolicy

logger = logging.getLogger(__name__)

STADIUM_CODE = "JAMSIL"

# Canonical origin points for Jamsil Stadium
JAMSIL_ORIGINS: list[dict] = [
    {
        "label": "잠실역_2호선_7번출구",
        "lat": 37.5133,
        "lng": 127.0999,
        "mode": "walk",
    },
    {
        "label": "잠실역_8호선_4번출구",
        "lat": 37.5133,
        "lng": 127.0999,
        "mode": "walk",
    },
    {
        "label": "몽촌토성역_8호선_1번출구",
        "lat": 37.5138,
        "lng": 127.0580,
        "mode": "walk",
    },
    {
        "label": "잠실나루역_2호선_2번출구",
        "lat": 37.5152,
        "lng": 127.0889,
        "mode": "walk",
    },
    {
        "label": "잠실역_환승센터_버스정류장",
        "lat": 37.5131,
        "lng": 127.1002,
        "mode": "bus",
    },
    {
        "label": "잠실야구장_공영주차장",
        "lat": 37.5113,
        "lng": 127.0721,
        "mode": "car",
    },
]


class TransitTimeCrawler(BaseCrawler):
    """collect transit time measurements from map APIs and persists them."""

    def __init__(
        self,
        stadium_code: str = STADIUM_CODE,
        origins: list[dict] | None = None,
        request_delay: float = 0.5,
        policy: RequestPolicy | None = None,
    ) -> None:
        """Initialize a new instance.

        Args:
            stadium_code: Stadium Code.
            origins: Origins.
            request_delay: Request delay in seconds.
            policy: Request policy.

        """
        super().__init__(request_delay=request_delay, policy=policy)
        self.stadium_code = stadium_code
        self.origins = origins or JAMSIL_ORIGINS
        self._origin_map = {o["label"]: o for o in self.origins if "label" in o}

    async def run(
        self,
        game_date: date | None = None,
        *,
        save: bool = False,
    ) -> list[dict]:
        """Measure transit times from all configured origins.

        Args:
            game_date: Game Date.
            save: Whether to persist the results.

        """
        game_date = game_date or datetime.now(KST).date()

        measured_at = datetime.now(UTC).replace(tzinfo=None)

        logger.info(
            "[Transit] Measuring %s origins for %s at %s UTC",
            len(self.origins),
            game_date,
            measured_at.strftime("%H:%M"),
        )

        # Fetch transit times for each mode
        walk_origins = [o for o in self.origins if o["mode"] in ("walk", "subway")]
        bus_origins = [o for o in self.origins if o["mode"] == "bus"]
        car_origins = [o for o in self.origins if o["mode"] == "car"]

        all_results = []
        for mode_origins, mode in [
            (walk_origins, "walk"),
            (bus_origins, "mixed"),
            (car_origins, "car"),
        ]:
            if not mode_origins:
                continue
            results = await get_transit_times_batch(
                [{"label": o["label"], "lat": o["lat"], "lng": o["lng"]} for o in mode_origins],
                mode=mode,  # type: ignore[arg-type]
            )
            all_results.extend(results)

        records = []
        for r in all_results:
            origin_info = self._origin_map.get(r.origin_label, {})
            records.append(
                {
                    "stadium_code": self.stadium_code,
                    "origin_label": r.origin_label,
                    "origin_lat": origin_info.get("lat"),
                    "origin_lng": origin_info.get("lng"),
                    "destination_label": getattr(r, "destination_label", "잠실야구장"),
                    "transport_mode": getattr(r, "transport_mode", getattr(r, "transit_mode", "walk")),
                    "duration_minutes": r.duration_minutes,
                    "distance_meters": r.distance_meters,
                    "congestion_factor": getattr(r, "congestion_factor", None),
                    "measured_at": measured_at,
                    "game_date": game_date,
                    "source_api": getattr(r, "source_api", getattr(r, "source", "unknown")),
                    "raw_response": getattr(r, "raw_response", None),
                }
            )

        logger.info("[Transit] Total measurements: %s", len(records))

        if save:
            await asyncio.to_thread(self._save_to_db, records)
        else:
            for rec in records:
                logger.info(
                    "  %s -> %s [%s]: %s min (%s m)",
                    rec["origin_label"],
                    rec["destination_label"],
                    rec["transport_mode"],
                    rec["duration_minutes"],
                    rec["distance_meters"],
                )

        return records

    def _save_to_db(self, records: list[dict]) -> None:
        with SessionLocal() as session:
            try:
                repo = TransitTimeRepository(session)
                save_fn = getattr(repo, "bulk_upsert", None) or repo.bulk_save
                count = save_fn(records)
                session.commit()
                logger.info("[Transit] Saved %s transit time records.", count)
            except Exception:
                session.rollback()
                logger.exception("[Transit] Database error")


if __name__ == "__main__":
    import argparse
    import asyncio

    from src.utils.date_helpers import parse_date_str

    parser = argparse.ArgumentParser(description="Collect stadium transit times")
    parser.add_argument("--save", action="store_true", help="Save results to database")
    parser.add_argument("--date", type=str, default=None, help="Game date (YYYY-MM-DD)")
    args = parser.parse_args()

    g_date = parse_date_str(args.date) if args.date else None
    crawler = TransitTimeCrawler()
    asyncio.run(crawler.run(game_date=g_date, save=args.save))
