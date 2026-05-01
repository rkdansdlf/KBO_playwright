"""
APScheduler-based automation for KBO data collection.

Jobs:
1. crawl_games_regular: Daily at 03:00 KST (run run_daily_update for previous KST day)
2. crawl_pregame_refresh: Every 15 minutes, 10:00-23:45 KST (today + configurable lookahead)
3. crawl_live_refresh: Every 2 minutes, 12:00-23:30 KST
4. crawl_futures_profile: Weekly Sunday at 05:00 KST (sync all Futures stats)
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from logging.handlers import RotatingFileHandler
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Sequence
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_exponential

from src.cli.crawl_futures import main as crawl_futures_main
from src.cli.daily_preview_batch import run_preview_batch
from src.cli.live_crawler import run_live_crawler_cycle
from src.cli.run_daily_update import main as run_daily_update_main
from src.db.engine import SessionLocal
from src.utils.safe_print import safe_print as print
from src.utils.alerting import SlackWebhookClient

# Configure logging
log_path = Path('logs/scheduler.log')
log_path.parent.mkdir(exist_ok=True)

handler = RotatingFileHandler(log_path, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        handler,
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")
FALSE_ENV_VALUES = {"0", "false", "no", "off"}
JOB_RUN_LOCK = Lock()
MISSING_PREGAME_ALERTED_DATES: set[str] = set()


def alert_failure(retry_state):
    """Alert on final tenacity failure and re-raise the original exception."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    func_name = retry_state.fn.__name__
    import traceback

    if exc:
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        error_text = str(exc)
    else:
        tb = "No exception was attached to retry state."
        error_text = "unknown"

    logger.error(f"Job {func_name} failed permanently after {retry_state.attempt_number} attempts: {exc}")
    try:
        SlackWebhookClient.send_error_alert(f"Job: {func_name}\nError: {error_text}\n\n{tb}")
    except Exception:
        logger.exception("Failed to send failure alert for job %s", func_name)

    if exc:
        raise exc
    raise RuntimeError(f"Job {func_name} failed permanently without an attached exception")


def alert_success(func_name: str):
    """Send optional success notification."""
    if os.getenv("NOTIFY_SUCCESS", "0") == "1":
        try:
            SlackWebhookClient.send_alert(f"✅ KBO Job {func_name} completed successfully.")
        except Exception:
            logger.exception("Failed to send success alert for job %s", func_name)


# Ensure logs directory exists
Path('logs').mkdir(exist_ok=True)


def _previous_day_kst() -> str:
    """Return yesterday in KST as YYYYMMDD."""
    return (datetime.now(KST) - timedelta(days=1)).strftime('%Y%m%d')


def _pregame_target_dates(now: datetime | None = None) -> list[str]:
    """Return pregame dates to refresh for the current scheduler tick."""
    current = now or datetime.now(KST)
    if current.tzinfo is None:
        current = current.replace(tzinfo=KST)
    else:
        current = current.astimezone(KST)

    try:
        lookahead_days = int(os.getenv("PREGAME_LOOKAHEAD_DAYS", "2"))
    except ValueError:
        lookahead_days = 2
    lookahead_days = max(0, min(lookahead_days, 7))

    return [
        (current + timedelta(days=offset)).strftime("%Y%m%d")
        for offset in range(lookahead_days + 1)
    ]


def _minutes_until_next_pregame_tick(now: datetime | None = None) -> float | None:
    current = now or datetime.now(KST)
    if current.tzinfo is None:
        current = current.replace(tzinfo=KST)
    else:
        current = current.astimezone(KST)

    if current.hour < 10 or current.hour > 23:
        return None

    base = current.replace(second=0, microsecond=0)
    minute = base.minute
    if minute % 15 == 0:
        next_tick = base
    else:
        next_minute = ((minute // 15) + 1) * 15
        if next_minute >= 60:
            next_tick = (base.replace(minute=0) + timedelta(hours=1))
        else:
            next_tick = base.replace(minute=next_minute)

    if next_tick.hour > 23:
        return None
    return (next_tick - current).total_seconds() / 60.0


def _should_skip_live_for_pregame(now: datetime | None = None) -> bool:
    try:
        threshold_minutes = int(os.getenv("LIVE_SKIP_BEFORE_PREGAME_MINUTES", "10"))
    except ValueError:
        threshold_minutes = 10
    if threshold_minutes <= 0:
        return False

    minutes_until_pregame = _minutes_until_next_pregame_tick(now)
    return minutes_until_pregame is not None and minutes_until_pregame <= threshold_minutes


def _pregame_refresh_summary(target_date: str) -> tuple[int, int, int]:
    """Return scheduled games, missing starters count, and preview-missing count for a date."""
    try:
        datetime.strptime(target_date, "%Y%m%d")
    except ValueError:
        return 0, 0, 0

    query = text(
        """
        SELECT
            COUNT(*) AS scheduled_total,
            SUM(
                CASE
                    WHEN (g.away_pitcher IS NULL OR g.away_pitcher = '')
                      OR (g.home_pitcher IS NULL OR g.home_pitcher = '')
                    THEN 1 ELSE 0
                END
            ) AS starters_missing,
            SUM(CASE WHEN p.game_id IS NULL THEN 1 ELSE 0 END) AS preview_missing
        FROM game g
        LEFT JOIN (
            SELECT DISTINCT game_id
            FROM game_summary
            WHERE summary_type = '프리뷰'
        ) p ON p.game_id = g.game_id
        WHERE UPPER(g.game_status) = 'SCHEDULED'
          AND REPLACE(CAST(g.game_date AS TEXT), '-', '') = :target_date
        """
    )

    with SessionLocal() as session:
        row = session.execute(query, {"target_date": target_date}).first()

    if row is None:
        return 0, 0, 0

    return (
        int(row.scheduled_total or 0),
        int(row.starters_missing or 0),
        int(row.preview_missing or 0),
    )


def _env_enabled(name: str, default: str = "1") -> bool:
    return os.getenv(name, default).strip().lower() not in FALSE_ENV_VALUES


def _pregame_sync_to_oci_enabled() -> bool:
    return _env_enabled("PREGAME_SYNC_TO_OCI") and bool(os.getenv("OCI_DB_URL"))


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=60, max=300),
    retry_error_callback=alert_failure,
)
def crawl_daily_games():
    """
    Daily job: Run unified daily update entrypoint.

    Runs at 03:00 KST daily to collect previous KST day's schedule+details.
    Uses exponential backoff retry on failures (3 attempts max).
    """
    with JOB_RUN_LOCK:
        logger.info("=== Starting Daily Games Crawl ===")

        try:
            target_date = _previous_day_kst()
            logger.info("Running run_daily_update for target_date=%s", target_date)
            run_daily_update_main(['--date', target_date, '--seed-tomorrow-preview'])

            logger.info("=== Daily Games Crawl Completed Successfully ===")
            alert_success("crawl_daily_games")

        except Exception as e:
            logger.error(f"Daily games crawl attempt failed: {e}")
            raise


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=30, max=120),
    retry_error_callback=alert_failure,
)
def crawl_pregame_refresh():
    with JOB_RUN_LOCK:
        failures: list[str] = []
        refresh_only_missing = _env_enabled("PREGAME_REFRESH_ONLY_MISSING", "1")
        alert_on_missing = _env_enabled("PREGAME_MISSING_ALERT", "0")
        sync_to_oci = _pregame_sync_to_oci_enabled()
        if sync_to_oci:
            logger.info("Pregame OCI sync is enabled")
        elif not _env_enabled("PREGAME_SYNC_TO_OCI"):
            logger.info("Pregame OCI sync is disabled by PREGAME_SYNC_TO_OCI")
        else:
            logger.warning("Pregame OCI sync is disabled because OCI_DB_URL is not set")
        if refresh_only_missing:
            logger.info("Pregame refresh mode: missing only")
        else:
            logger.info("Pregame refresh mode: full date window")

        for target_date in _pregame_target_dates():
            scheduled_count, starters_missing, preview_missing = _pregame_refresh_summary(target_date)
            if scheduled_count == 0:
                continue

            should_refresh = not refresh_only_missing or starters_missing > 0 or preview_missing > 0
            if not should_refresh:
                logger.info(
                    "Skipping pregame refresh for target_date=%s (all starters/preview present)",
                    target_date,
                )
                MISSING_PREGAME_ALERTED_DATES.discard(target_date)
                continue

            logger.info(
                "Running pregame refresh for target_date=%s (starters_missing=%s, preview_missing=%s)",
                target_date,
                starters_missing,
                preview_missing,
            )
            saved_ids = asyncio.run(run_preview_batch(target_date, sync_to_oci=sync_to_oci))
            if saved_ids and sync_to_oci:
                logger.info("Pregame OCI sync completed for target_date=%s games=%s", target_date, len(saved_ids))
            post_refresh = _pregame_refresh_summary(target_date)
            if post_refresh[0] and (post_refresh[1] > 0 or post_refresh[2] > 0):
                if alert_on_missing and target_date not in MISSING_PREGAME_ALERTED_DATES:
                    try:
                        SlackWebhookClient.send_alert(
                            f"⚠️ Pregame missing remains for {target_date}: "
                            f"starters_missing={post_refresh[1]}, preview_missing={post_refresh[2]}"
                        )
                    except Exception:
                        logger.exception("Failed to send pregame missing alert for target_date=%s", target_date)
                    MISSING_PREGAME_ALERTED_DATES.add(target_date)
            else:
                MISSING_PREGAME_ALERTED_DATES.discard(target_date)
            if scheduled_count and not saved_ids:
                failures.append(f"{target_date}: scheduled={scheduled_count}, saved=0")

        if failures:
            raise RuntimeError("Pregame refresh saved no preview rows: " + "; ".join(failures))

        alert_success("crawl_pregame_refresh")


def crawl_live_refresh():
    if _should_skip_live_for_pregame():
        logger.info("Skipping live refresh because pregame refresh is due soon")
        return

    with JOB_RUN_LOCK:
        logger.info("Running live refresh cycle")
        asyncio.run(run_live_crawler_cycle())


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=120, max=600),
    retry_error_callback=alert_failure,
)
def crawl_all_futures_profiles():
    """
    Weekly job: Crawl Futures league stats from all player profiles.

    Runs on Sunday at 05:00 KST to sync season-cumulative Futures stats.
    Uses exponential backoff retry on failures (3 attempts max).
    """
    with JOB_RUN_LOCK:
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
            alert_success("crawl_all_futures_profiles")

        except Exception as e:
            logger.error(f"Futures profile crawl attempt failed: {e}")
            raise


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="APScheduler for KBO daily/futures jobs")
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Run only one daily update job immediately and exit.",
    )
    parser.add_argument(
        "--run-pregame-once",
        action="store_true",
        help="Run only one pregame refresh job immediately and exit.",
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
    if args.run_pregame_once:
        crawl_pregame_refresh()
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

    scheduler.add_job(
        crawl_pregame_refresh,
        trigger=CronTrigger(hour="10-23", minute="*/15"),
        id='crawl_pregame_refresh',
        name='Pregame Refresh',
        misfire_grace_time=900,
        max_instances=1,
    )
    logger.info("Registered job: crawl_pregame_refresh (Every 15m, 10:00-23:45 KST, today + lookahead)")

    scheduler.add_job(
        crawl_live_refresh,
        trigger=CronTrigger(hour="12-22", minute="*/2"),
        id='crawl_live_refresh_day',
        name='Live Refresh Day Window',
        misfire_grace_time=180,
        max_instances=1,
    )
    scheduler.add_job(
        crawl_live_refresh,
        trigger=CronTrigger(hour=23, minute="0-30/2"),
        id='crawl_live_refresh_night',
        name='Live Refresh Night Window',
        misfire_grace_time=180,
        max_instances=1,
    )
    logger.info("Registered job: crawl_live_refresh (Every 2m, 12:00-23:30 KST)")

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
    print("  2. Pregame Refresh: Every 15 minutes, 10:00-23:45 KST, today + lookahead")
    print("  3. Live Refresh: Every 2 minutes, 12:00-23:30 KST")
    print("  4. Futures Profile Sync: Every Sunday at 05:00 KST")
    print("="*60 + "\n")

    logger.info("Scheduler started successfully")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user")
        print("\nScheduler stopped")


if __name__ == "__main__":
    main()
