"""KBO Periodic Extras Orchestrator.

Fetch Futures league data and retired player listings.

"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")
PERIODIC_SUBPROCESS_EXCEPTIONS = (OSError, RuntimeError, ValueError)


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
) -> None:
    """Run periodic extras.

    Args:
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

    logger.info("\n%s", "=" * 60)
    logger.info("🏁 Periodic Extras Finished")
    logger.info("%s\n", "=" * 60)


def main() -> int:
    """Run the main entry point for this CLI command."""
    parser = argparse.ArgumentParser(description="KBO Periodic Extras Orchestrator")
    parser.add_argument("--year", type=int, help="Target year. Defaults to current year.")

    args = parser.parse_args()

    year = args.year or datetime.now(KST).year
    asyncio.run(run_periodic_extras(year))
    return 0


if __name__ == "__main__":
    main()
