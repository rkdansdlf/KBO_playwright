"""KBO Periodic Extras Orchestrator.

Fetch Futures league data and retired player listings.

"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.sync.oci_sync import OCISync

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")
PERIODIC_SUBPROCESS_EXCEPTIONS = (OSError, RuntimeError, ValueError)
PERIODIC_SYNC_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)


async def _run_subprocess(cmd: list[str]) -> tuple[int, str, str]:
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    return process.returncode or 0, stdout.decode(), stderr.decode()


async def run_periodic_extras(
    year: int,
    *,
    sync: bool = False,
) -> None:
    """Run periodic extras.

    Args:
        year: Season year.
        sync: Whether to sync to remote database.
        year: Season year.

    """
    logger.info("\n%s", "=" * 60)

    logger.info("🚀 KBO Periodic Extras Started for Year: %s", year)
    logger.info("%s", "=" * 60)

    # 1. Futures League Data (Hitter)
    # Note: We assume these crawlers have a main() or similar entrypoint
    logger.info("\n🔮 Step 1: Crawling Futures League Batting Stats...")
    try:
        cmd = [sys.executable, "-m", "src.crawlers.futures.futures_batting", "--year", str(year), "--save"]
        returncode, stdout, stderr = await _run_subprocess(cmd)
        if returncode == 0:
            logger.info("   ✅ Futures Hitter output:\n%s", stdout)
        else:
            logger.error("   ❌ Futures Hitter failed:\n%s", stderr)
    except PERIODIC_SUBPROCESS_EXCEPTIONS:
        logger.exception("   ❌ Error crawling futures stats")

    # 2. Retired Player Listing
    logger.info("\n👴 Step 2: Crawling Retired Player Listings...")
    try:
        # retired listing usually doesn't need a year, or it's for all
        cmd = [sys.executable, "-m", "src.crawlers.retire.listing", "--save"]
        returncode, stdout, stderr = await _run_subprocess(cmd)
        if returncode == 0:
            logger.info("   ✅ Retired Listing output:\n%s", stdout)
        else:
            logger.error("   ❌ Retired Listing failed:\n%s", stderr)
    except PERIODIC_SUBPROCESS_EXCEPTIONS:
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
                except PERIODIC_SYNC_EXCEPTIONS:
                    logger.exception("   ❌ OCI sync error")
                finally:
                    syncer.close()

    logger.info("\n%s", "=" * 60)
    logger.info("🏁 Periodic Extras Finished")
    logger.info("%s\n", "=" * 60)


def main() -> int:
    """Run the main entry point for this CLI command."""
    parser = argparse.ArgumentParser(description="KBO Periodic Extras Orchestrator")
    parser.add_argument("--year", type=int, help="Target year. Defaults to current year.")
    parser.add_argument("--sync", action="store_true", help="Sync to OCI")

    args = parser.parse_args()

    year = args.year or datetime.now(KST).year
    asyncio.run(run_periodic_extras(year, sync=args.sync))
    return 0


if __name__ == "__main__":
    main()
