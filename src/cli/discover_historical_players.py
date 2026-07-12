"""CLI to discover and store historical player IDs from 1982 to current.

Phase 1 of the Retired Player Backfill project.

"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime
from typing import TYPE_CHECKING

from src.constants import KST
from src.crawlers.retire.listing import RetiredPlayerListingCrawler
from src.db.engine import SessionLocal
from src.models.player import PlayerBasic

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


async def discover_and_save_players(start_year: int, end_year: int, active_year: int) -> None:
    """Save discover and players.

    Args:
        start_year: Start Year.
        end_year: End Year.
        active_year: Active Year.
        start_year: Start Year.
        end_year: End Year.
        active_year: Active Year.

    """
    crawler = RetiredPlayerListingCrawler(request_delay=1.0)

    logger.info("🚀 Starting historical player discovery from %s to %s...", start_year, end_year)
    logger.info("📡 Comparing against active roster year: %s", active_year)

    seasons = range(start_year, end_year + 1)
    historical_players = await crawler.collect_historical_player_ids(seasons)
    active_players = await crawler.collect_player_ids_for_year(active_year)

    active_ids = set(active_players.keys())

    logger.info("✨ Discovered %s unique historical player IDs.", len(historical_players))

    # Save to DB
    with SessionLocal() as session:
        new_count = 0
        update_count = 0

        for pid_str, name in historical_players.items():
            pid = int(pid_str)
            is_active = pid_str in active_ids

            existing = session.query(PlayerBasic).filter_by(player_id=pid).first()
            if not existing:
                # New player discovered
                new_player = PlayerBasic(
                    player_id=pid,
                    name=name,
                    status="active" if is_active else "retired",
                    status_source="discovery",
                )
                session.add(new_player)
                new_count += 1
            else:
                # Update existing if status changed
                new_status = "active" if is_active else "retired"
                if existing.status not in (new_status, "staff"):
                    existing.status = new_status
                    existing.status_source = "discovery"
                    update_count += 1

            # Commit in batches
            if (new_count + update_count) % 100 == 0:
                session.commit()

        session.commit()
        logger.info("✅ DB Update complete: %s new players added, %s players updated.", new_count, update_count)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    _current_year = datetime.now(KST).year

    parser = argparse.ArgumentParser(description="Discover and store historical player IDs")
    parser.add_argument("--start", type=int, default=1982)
    parser.add_argument("--end", type=int, default=_current_year - 1)
    parser.add_argument("--active-year", type=int, default=_current_year)
    args = parser.parse_args(argv)

    asyncio.run(discover_and_save_players(args.start, args.end, args.active_year))
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
