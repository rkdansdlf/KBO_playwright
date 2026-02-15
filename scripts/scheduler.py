"""
APScheduler-based automation for KBO data collection.

Jobs:
1. crawl_games_regular: Daily at 03:00 KST (run run_daily_update for previous KST day)
2. crawl_futures_profile: Weekly Sunday at 05:00 KST (sync all Futures stats)
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Sequence
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from tenacity import retry, stop_after_attempt, wait_exponential

from src.cli.crawl_futures import main as crawl_futures_main
from src.cli.run_daily_update import main as run_daily_update_main
from src.utils.safe_print import safe_print as print

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scheduler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")

# Ensure logs directory exists
Path('logs').mkdir(exist_ok=True)


def _previous_day_kst() -> str:
    """Return yesterday in KST as YYYYMMDD."""
    return (datetime.now(KST) - timedelta(days=1)).strftime('%Y%m%d')


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=60, max=300))
def crawl_daily_games():
    """
    Daily job: Run unified daily update entrypoint.

    Runs at 03:00 KST daily to collect previous KST day's schedule+details.
    Uses exponential backoff retry on failures (3 attempts max).
    """
    logger.info("=== Starting Daily Games Crawl ===")

    try:
        target_date = _previous_day_kst()
        logger.info("Running run_daily_update for target_date=%s", target_date)
        run_daily_update_main(['--date', target_date])

        logger.info("=== Daily Games Crawl Completed Successfully ===")

    except Exception as e:
        logger.error(f"Daily games crawl failed: {e}", exc_info=True)
        raise  # Re-raise for tenacity retry


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=120, max=600))
def crawl_all_futures_profiles():
    """
    Weekly job: Crawl Futures league stats from all player profiles.

    Runs on Sunday at 05:00 KST to sync season-cumulative Futures stats.
    Uses exponential backoff retry on failures (3 attempts max).
    """
    logger.info("=== Starting Weekly Futures Profile Crawl ===")

    try:
        current_year = datetime.now().year

        # Crawl Futures stats with recommended settings
        logger.info(f"Crawling Futures stats for active players in {current_year}")
        crawl_futures_main([
            '--season', str(current_year),
            '--concurrency', '2',  # Low concurrency to respect rate limits
            '--delay', '2.0'  # 2-second delay between requests
        ])

        logger.info("=== Weekly Futures Profile Crawl Completed Successfully ===")

    except Exception as e:
        logger.error(f"Futures profile crawl failed: {e}", exc_info=True)
        raise  # Re-raise for tenacity retry


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="APScheduler for KBO daily/futures jobs")
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Run only one daily update job immediately and exit.",
    )
    parser.add_argument(
        "--no-startup-run",
        action="store_true",
        help="Disable one-time startup run regardless of STARTUP_RUN env.",
    )
    return parser


def main(argv: Sequence[str] | None = None):
    """Initialize and start the APScheduler."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.run_once:
        crawl_daily_games()
        return

    scheduler = BlockingScheduler(timezone='Asia/Seoul')

    # Job 1: Daily regular season games (03:00 KST)
    scheduler.add_job(
        crawl_daily_games,
        trigger=CronTrigger(hour=3, minute=0),
        id='crawl_games_regular',
        name='Daily Regular Season Games Crawl',
        misfire_grace_time=3600,  # Allow 1 hour grace period
        max_instances=1  # Prevent concurrent runs
    )
    logger.info("Registered job: crawl_games_regular (Daily 03:00 KST)")

    # Job 2: Weekly Futures profile sync (Sunday 05:00 KST)
    scheduler.add_job(
        crawl_all_futures_profiles,
        trigger=CronTrigger(day_of_week='sun', hour=5, minute=0),
        id='crawl_futures_profile',
        name='Weekly Futures Profile Sync',
        misfire_grace_time=7200,  # Allow 2 hour grace period
        max_instances=1  # Prevent concurrent runs
    )
    logger.info("Registered job: crawl_futures_profile (Weekly Sunday 05:00 KST)")

    # Optional one-time startup backfill
    startup_run = os.getenv('STARTUP_RUN', '1') == '1' and not args.no_startup_run
    if startup_run:
        try:
            logger.info("Performing one-time startup crawl (run_daily_update)")
            crawl_daily_games()
        except Exception:
            logger.exception("Startup crawl failed; scheduler will continue with cron jobs")

    print("\n" + "="*60)
    print(" KBO Crawler Scheduler Started")
    print("="*60)
    print(f" Timezone: Asia/Seoul")
    print(f" Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    print("\nScheduled Jobs:")
    print("  1. Daily Games Crawl: Every day at 03:00 KST")
    print("  2. Futures Profile Sync: Every Sunday at 05:00 KST")
    print("="*60 + "\n")

    logger.info("Scheduler started successfully")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user")
        print("\nScheduler stopped")


if __name__ == "__main__":
    main()
