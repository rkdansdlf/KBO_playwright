"""
KBO Periodic Extras Orchestrator.
Fetches Futures league data and retired player listings.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from src.db.engine import SessionLocal
from src.sync.oci_sync import OCISync

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")


async def run_periodic_extras(
    year: int,
    sync: bool = False,
) -> None:
    logger.info(f"\n{'=' * 60}")  # noqa: G004
    logger.info("🚀 KBO Periodic Extras Started for Year: %s", year)
    logger.info(f"{'=' * 60}")  # noqa: G004

    # 1. Futures League Data (Hitter)
    # Note: We assume these crawlers have a main() or similar entrypoint
    logger.info("\n🔮 Step 1: Crawling Futures League Batting Stats...")
    try:
        import subprocess
        import sys

        cmd = [sys.executable, "-m", "src.crawlers.futures.futures_batting", "--year", str(year), "--save"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info("   ✅ Futures Hitter output:\n%s", result.stdout)
        else:
            logger.error("   ❌ Futures Hitter failed:\n%s", result.stderr)
    except Exception:
        logger.exception("   ❌ Error crawling futures stats")

    # 2. Retired Player Listing
    logger.info("\n👴 Step 2: Crawling Retired Player Listings...")
    try:
        # retired listing usually doesn't need a year, or it's for all
        cmd = [sys.executable, "-m", "src.crawlers.retire.listing", "--save"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info("   ✅ Retired Listing output:\n%s", result.stdout)
        else:
            logger.error("   ❌ Retired Listing failed:\n%s", result.stderr)
    except Exception:
        logger.exception("   ❌ Error crawling retired players")

    if sync:
        logger.info("\n☁️ Step 3: Synchronizing to OCI...")
        oci_url = os.getenv("OCI_DB_URL")
        if not oci_url:
            logger.warning("   ⚠️ OCI_DB_URL not set, skipping sync")
        else:
            with SessionLocal() as session:
                syncer = OCISync(oci_url, session)
                try:
                    # Sync reference player rows before dependent season-stat tables.
                    syncer.sync_player_basic()
                    syncer.sync_players()
                    syncer.sync_player_season_batting(year=year)
                    syncer.sync_player_season_pitching(year=year)
                    logger.info("   ✅ OCI synchronization completed")
                except Exception:
                    logger.exception("   ❌ OCI sync error")
                finally:
                    syncer.close()

    logger.info(f"\n{'=' * 60}")  # noqa: G004
    logger.info("🏁 Periodic Extras Finished")
    logger.info(f"{'=' * 60}\n")  # noqa: G004


def main() -> int:
    parser = argparse.ArgumentParser(description="KBO Periodic Extras Orchestrator")
    parser.add_argument("--year", type=int, help="Target year. Defaults to current year.")
    parser.add_argument("--sync", action="store_true", help="Sync to OCI")

    args = parser.parse_args()

    year = args.year or datetime.now(KST).year
    asyncio.run(run_periodic_extras(year, sync=args.sync))


if __name__ == "__main__":
    main()
