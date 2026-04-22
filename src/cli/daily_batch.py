"""
Compatibility wrapper for legacy automation entrypoints.

`closing` now delegates to the postgame finalize pipeline and `live` delegates to
the real-time refresh pipeline so old operational calls do not keep running the
deprecated batch logic.

Usage:
    python -m src.cli.daily_batch --mode closing
    python -m src.cli.daily_batch --mode live
"""
import argparse
import asyncio
import datetime
from datetime import timedelta
import logging
import os
from zoneinfo import ZoneInfo

from src.cli.live_crawler import run_live_crawler_cycle
from src.cli.run_daily_update import run_update
from src.crawlers.schedule_crawler import ScheduleCrawler
from dateutil.relativedelta import relativedelta
from src.services.schedule_collection_service import save_schedule_games

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("daily_batch.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DailyBatch")
KST = ZoneInfo("Asia/Seoul")

async def run_startup():
    """
    Startup Routine (Runs on Docker container start)
    1. Update Schedule for Upcoming 3 Months (Current + 2)
    2. Sync basic metadata if needed (optional)
    """
    logger.info("=== Starting Startup Routine ===")
    
    # Crawl Schedule for Current + Next 2 Months (Total 3 months coverage)
    now = datetime.datetime.now()
    targets = []
    for i in range(3):
        d = now + relativedelta(months=i)
        targets.append((d.year, d.month))

    logger.info(f"Step 1: Updating Schedule for {targets}")
    
    sch_crawler = ScheduleCrawler()
    
    total_saved = 0
    for year, month in targets:
        try:
            games = await sch_crawler.crawl_schedule(year, month)
            logger.info(f"   Crawled {year}-{month:02d}: Found {len(games)} games")
            result = save_schedule_games(games, log=logger.warning)
            total_saved += result.saved
        except Exception as e:
            logger.error(f"   Failed to crawl schedule for {year}-{month:02d}: {e}")

    logger.info(f"Schedule update complete. Total games upserted: {total_saved}")
    logger.info("=== Startup Routine Finished ===")


async def run_closing(target_date: str = None):
    if not target_date:
        yesterday = datetime.datetime.now(KST) - timedelta(days=1)
        target_date = yesterday.strftime("%Y%m%d")

    logger.info("Delegating legacy closing mode to run_daily_update for %s", target_date)
    await run_update(
        target_date,
        sync=bool(os.getenv("OCI_DB_URL")),
        headless=True,
    )


async def run_live_watcher():
    logger.info("Delegating legacy live mode to run_live_crawler_cycle")
    await run_live_crawler_cycle(sync_to_oci=bool(os.getenv("OCI_DB_URL")))

def main():
    parser = argparse.ArgumentParser(description="KBO Daily Automation Batch")
    parser.add_argument("--mode", choices=["closing", "live", "startup"], required=True, help="Execution mode")
    parser.add_argument("--date", type=str, help="Target date (YYYYMMDD) for closing mode. Defaults to yesterday.")
    
    args = parser.parse_args()
    
    if args.mode == "closing":
        asyncio.run(run_closing(args.date))
    elif args.mode == "live":
        asyncio.run(run_live_watcher())
    elif args.mode == "startup":
        asyncio.run(run_startup())

if __name__ == "__main__":
    main()
