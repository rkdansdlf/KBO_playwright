"""
APScheduler-based automation for KBO data collection.

Jobs:
 1. crawl_games_regular: Daily at 03:00 KST (run_daily_update)
 2. compute_standings: Daily at 03:30 KST (standings + home/away + trends)
 3. aggregate_team_defense: Daily at 03:45 KST (fielding/baserunning)
 4. compute_rankings: Daily at 04:00 KST (sabermetric rankings)
 5. sync_from_oci: Daily at 05:00 KST (OCI hydration)
 6. generate_quality_report: Daily at 05:15 KST
 7. crawl_phase1_extra: Daily at 06:00 KST (broadcast, MVP, injury, etc.)
 8. crawl_pregame_refresh: Every 15m, 10:00-23:45 KST
 9. crawl_live_refresh: Every 2m, 12:00-23:30 KST
10. crawl_futures_profile: Weekly Sunday at 05:00 KST
11. compute_park_factor: Weekly Sunday at 05:30 KST
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from threading import Lock
from typing import Sequence
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from apscheduler.events import EVENT_JOB_ERROR
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_exponential

from src.cli.crawl_futures import main as crawl_futures_main
from src.cli.daily_preview_batch import run_preview_batch
from src.cli.live_crawler import run_live_crawler_cycle
from src.cli.run_daily_update import format_stability_alert_summary
from src.cli.run_daily_update import main as run_daily_update_main
from src.db.engine import SessionLocal
from src.utils.alerting import SlackWebhookClient
from src.utils.safe_print import safe_print as print

# Configure logging
log_path = Path("logs/scheduler.log")
log_path.parent.mkdir(exist_ok=True)

handler = RotatingFileHandler(log_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[handler, logging.StreamHandler()],
)
logger = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")
FALSE_ENV_VALUES = {"0", "false", "no", "off"}

# Granular locking to prevent long-running batch jobs from blocking real-time updates
LIVE_LOCK = Lock()  # For live refresh and pregame refresh
DAILY_LOCK = Lock()  # For daily update/finalize
MAINTENANCE_LOCK = Lock()  # For weekly futures sync and OCI hydration/reports

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

    # Do NOT re-raise — let retry_error_callback suppress so the scheduler survives.
    if exc:
        logger.warning(f"Job {func_name} permanently failed but scheduler continues: {error_text}")
    return None


def alert_success(func_name: str, details: str | None = None):
    """Send optional success notification."""
    if os.getenv("NOTIFY_SUCCESS", "0") == "1":
        try:
            message = f"✅ KBO Job {func_name} completed successfully."
            if details:
                message = f"{message}\n{details}"
            SlackWebhookClient.send_alert(message)
        except Exception:
            logger.exception("Failed to send success alert for job %s", func_name)


# Ensure logs directory exists
Path("logs").mkdir(exist_ok=True)


def _previous_day_kst() -> str:
    """Return yesterday in KST as YYYYMMDD."""
    return (datetime.now(KST) - timedelta(days=1)).strftime("%Y%m%d")


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

    return [(current + timedelta(days=offset)).strftime("%Y%m%d") for offset in range(lookahead_days + 1)]


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
            next_tick = base.replace(minute=0) + timedelta(hours=1)
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
def _run_hydration(year: int, target_date: str | None = None):
    """Run OCI to Local hydration via CLI main."""
    from src.cli.hydrate_runtime_from_oci import main as hydrate_main

    logger.info("Starting hydration for year=%d, date=%s", year, target_date)
    args = ["--year", str(year)]
    if target_date:
        args.extend(["--date", target_date])
    hydrate_main(args)
    logger.info("Hydration completed successfully")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=30, max=300),
    retry_error_callback=alert_failure,
)
def crawl_daily_games():
    """
    Daily job: Run unified daily update entrypoint.

    Runs at 03:00 KST daily to collect previous KST day's schedule+details.
    Uses exponential backoff retry on failures (3 attempts max).
    """
    with DAILY_LOCK:
        logger.info("=== Starting Daily Games Crawl ===")

        try:
            target_date = _previous_day_kst()
            logger.info("Running run_daily_update for target_date=%s", target_date)

            args = ["--date", target_date, "--seed-tomorrow-preview"]
            if bool(os.getenv("OCI_DB_URL")):
                logger.info("Enabling OCI sync for daily update")
                args.append("--sync")

            update_result = run_daily_update_main(args)

            logger.info("=== Daily Games Crawl Completed Successfully ===")
            alert_success("crawl_daily_games", format_stability_alert_summary(update_result))
        except Exception as e:
            logger.error(f"Daily games crawl attempt failed: {e}")
            raise


def backfill_missed_daily_crawls(lookback_days: int = 7) -> list[str]:
    """Check recent days for COMPLETED games missing detail data and re-collect."""
    from sqlalchemy import text as sa_text

    from src.db.engine import SessionLocal

    start = datetime.now(KST).date() - timedelta(days=lookback_days)
    backfilled: list[str] = []
    with SessionLocal() as session:
        rows = session.execute(
            sa_text("""
                SELECT g.game_date
                FROM game g
                LEFT JOIN game_batting_stats b ON g.game_id = b.game_id
                WHERE g.game_date >= :start
                  AND g.game_status IN ('COMPLETED', 'DRAW')
                GROUP BY g.game_date
                HAVING COUNT(DISTINCT g.game_id) > 0
                   AND COUNT(b.game_id) = 0
                ORDER BY g.game_date
            """),
            {"start": start},
        ).fetchall()
    for (game_date_str,) in rows:
        date_compact = (
            game_date_str.strftime("%Y%m%d")
            if hasattr(game_date_str, "strftime")
            else str(game_date_str).replace("-", "")
        )
        logger.info("Backfilling missed daily crawl for %s", date_compact)
        try:
            run_daily_update_main(["--date", date_compact])
            backfilled.append(date_compact)
        except Exception:
            logger.exception("Backfill failed for %s", date_compact)
    return backfilled


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=30, max=120),
    retry_error_callback=alert_failure,
)
def crawl_pregame_refresh():
    with LIVE_LOCK:
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

    with LIVE_LOCK:
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
    with MAINTENANCE_LOCK:
        logger.info("=== Starting Weekly Futures Profile Crawl ===")

        try:
            current_year = datetime.now().year

            # Crawl Futures stats with recommended settings
            logger.info(f"Crawling Futures stats for active players in {current_year}")
            crawl_futures_main(
                [
                    "--season",
                    str(current_year),
                    "--concurrency",
                    "2",  # Low concurrency to respect rate limits
                    "--delay",
                    "2.0",  # 2-second delay between requests
                ]
            )

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


def sync_from_oci_job():
    """
    Sync job: Hydrate local DB from OCI after GitHub Actions run window.
    Runs daily at 05:00 KST.
    """
    with MAINTENANCE_LOCK:
        logger.info("=== Starting OCI to Local Sync (Hydration) ===")
        current_year = datetime.now(KST).year
        # Hydrate for the current year
        _run_hydration(current_year)
        logger.info("=== OCI to Local Sync Completed Successfully ===")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=60, max=300),
    retry_error_callback=alert_failure,
)
def generate_daily_report_job():
    """
    Quality report job: Analyze previous day's data integrity.
    Runs daily at 05:15 KST.
    """
    from src.cli.generate_quality_report import main as report_main

    with MAINTENANCE_LOCK:
        logger.info("=== Starting Daily Quality Report Generation ===")
        target_date = _previous_day_kst()
        # Run report and notify if issues found
        report_main(["--date", target_date, "--force-notify"])
        logger.info("=== Daily Quality Report Generation Completed ===")


def crawl_phase1_extra_job():
    """
    Phase 1: Supplementary crawlers (broadcast, MVP, injury, foreign players, manager changes).
    Runs daily at 06:00 KST (after daily game crawl and standings compute).
    """
    with MAINTENANCE_LOCK:
        logger.info("=== Starting Phase 1 Extra Crawlers ===")
        try:
            from src.cli.crawl_phase1_extra import run_all_crawlers

            asyncio.run(run_all_crawlers(save=True))
            logger.info("=== Phase 1 Extra Crawlers Completed Successfully ===")
        except Exception:
            logger.exception("Phase 1 extra crawlers failed")


def compute_standings_job():
    """
    Compute daily standings with home/away splits, recent 10, weekly trends.
    Runs daily at 03:30 KST (after game crawl at 03:00).
    """
    with DAILY_LOCK:
        logger.info("=== Starting Standings Computation ===")
        try:
            from src.cli.calculate_standings import main as standings_main

            current_year = datetime.now(KST).year
            standings_main(["--year", str(current_year)])
            logger.info("=== Standings Computation Completed ===")
        except Exception:
            logger.exception("Standings computation failed")


def compute_park_factor_job():
    """
    Compute park factor for all stadiums.
    Runs weekly Sunday at 05:30 KST.
    """
    with MAINTENANCE_LOCK:
        logger.info("=== Starting Park Factor Computation ===")
        try:
            from src.aggregators.park_factor_calculator import ParkFactorCalculator
            from src.db.engine import SessionLocal

            current_year = datetime.now(KST).year
            with SessionLocal() as session:
                calc = ParkFactorCalculator(session)
                results = calc.calculate(current_year)
                logger.info(f"Park Factor computed for {len(results)} stadiums")
        except Exception:
            logger.exception("Park Factor computation failed")


def aggregate_team_defense_job():
    """
    Aggregate team-level fielding and baserunning stats.
    Runs daily at 03:45 KST (after standings).
    """
    with MAINTENANCE_LOCK:
        logger.info("=== Starting Team Defense Aggregation ===")
        try:
            from src.aggregators.team_fielding_aggregator import TeamFieldingAggregator
            from src.db.engine import SessionLocal
            from src.models.team import Team

            current_year = datetime.now(KST).year
            with SessionLocal() as session:
                teams = [t.team_id for t in session.query(Team.team_id).filter(Team.is_active).all()]
                agg = TeamFieldingAggregator(session)
                agg.run_all(current_year, teams)
                logger.info("=== Team Defense Aggregation Completed ===")
        except Exception:
            logger.exception("Team defense aggregation failed")


def compute_rankings_job():
    """
    Compute sabermetric rankings (wOBA, wRC+, WAR, OPS+).
    Runs daily at 04:00 KST.
    """
    with MAINTENANCE_LOCK:
        logger.info("=== Starting Rankings Computation ===")
        try:
            from src.aggregators.ranking_aggregator import RankingAggregator
            from src.db.engine import SessionLocal

            current_year = datetime.now(KST).year
            with SessionLocal() as session:
                agg = RankingAggregator(session)
                agg.run_for_season(current_year)
                logger.info("=== Rankings Computation Completed ===")
        except Exception:
            logger.exception("Rankings computation failed")


def job_error_listener(event):
    """Listener for failed APScheduler jobs."""
    if event.exception:
        import traceback

        job = event.job_id
        exc = event.exception
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))

        logger.error(f"Job {job} failed: {exc}")
        try:
            SlackWebhookClient.send_error_alert(f"🚨 <b>Scheduler Job Failed: {job}</b>\nError: {exc}\n\n{tb}")
        except Exception:
            logger.exception("Failed to send Slack alert for failed job %s", job)


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

    scheduler = BlockingScheduler(timezone="Asia/Seoul")
    scheduler.add_listener(job_error_listener, EVENT_JOB_ERROR)

    # Job 1: Daily regular season games (03:00 KST)
    scheduler.add_job(
        crawl_daily_games,
        trigger=CronTrigger(hour=3, minute=0),
        id="crawl_games_regular",
        name="Daily Regular Season Games Crawl",
        misfire_grace_time=3600,  # Allow 1 hour grace period
        max_instances=1,  # Prevent concurrent runs
    )
    logger.info("Registered job: crawl_games_regular (Daily 03:00 KST)")

    # Job 1.5: Sync from OCI (05:00 KST) - After GitHub Actions finish
    scheduler.add_job(
        sync_from_oci_job,
        trigger=CronTrigger(hour=5, minute=0),
        id="sync_from_oci",
        name="OCI to Local Sync (Hydration)",
        misfire_grace_time=3600,
        max_instances=1,
    )
    logger.info("Registered job: sync_from_oci (Daily 05:00 KST)")

    # Job 1.5: Standings Computation (03:30 KST)
    scheduler.add_job(
        compute_standings_job,
        trigger=CronTrigger(hour=3, minute=30),
        id="compute_standings",
        name="Daily Standings Computation",
        misfire_grace_time=3600,
        max_instances=1,
    )
    logger.info("Registered job: compute_standings (Daily 03:30 KST)")

    # Job 1.6: Team Defense Aggregation (03:45 KST)
    scheduler.add_job(
        aggregate_team_defense_job,
        trigger=CronTrigger(hour=3, minute=45),
        id="aggregate_team_defense",
        name="Daily Team Defense Aggregation",
        misfire_grace_time=3600,
        max_instances=1,
    )
    logger.info("Registered job: aggregate_team_defense (Daily 03:45 KST)")

    # Job 1.7: Rankings Computation (04:00 KST)
    scheduler.add_job(
        compute_rankings_job,
        trigger=CronTrigger(hour=4, minute=0),
        id="compute_rankings",
        name="Daily Rankings Computation",
        misfire_grace_time=3600,
        max_instances=1,
    )
    logger.info("Registered job: compute_rankings (Daily 04:00 KST)")

    # Job 1.8: Phase 1 Extra Crawlers (06:00 KST)
    scheduler.add_job(
        crawl_phase1_extra_job,
        trigger=CronTrigger(hour=6, minute=0),
        id="crawl_phase1_extra",
        name="Phase 1 Extra Crawlers",
        misfire_grace_time=7200,
        max_instances=1,
    )
    logger.info("Registered job: crawl_phase1_extra (Daily 06:00 KST)")

    # Job 1.9: Daily Quality Report (05:15 KST)
    scheduler.add_job(
        generate_daily_report_job,
        trigger=CronTrigger(hour=5, minute=15),
        id="generate_quality_report",
        name="Daily Quality Report Generation",
        misfire_grace_time=3600,
        max_instances=1,
    )
    logger.info("Registered job: generate_quality_report (Daily 05:15 KST)")

    # Job 2: Weekly Futures profile sync (Sunday 05:00 KST)
    scheduler.add_job(
        crawl_all_futures_profiles,
        trigger=CronTrigger(day_of_week="sun", hour=5, minute=0),
        id="crawl_futures_profile",
        name="Weekly Futures Profile Sync",
        misfire_grace_time=7200,  # Allow 2 hour grace period
        max_instances=1,  # Prevent concurrent runs
    )
    logger.info("Registered job: crawl_futures_profile (Weekly Sunday 05:00 KST)")

    # Job 2.5: Weekly Park Factor Computation (Sunday 05:30 KST)
    scheduler.add_job(
        compute_park_factor_job,
        trigger=CronTrigger(day_of_week="sun", hour=5, minute=30),
        id="compute_park_factor",
        name="Weekly Park Factor Computation",
        misfire_grace_time=7200,
        max_instances=1,
    )
    logger.info("Registered job: compute_park_factor (Weekly Sunday 05:30 KST)")

    scheduler.add_job(
        crawl_pregame_refresh,
        trigger=CronTrigger(hour="10-23", minute="*/15"),
        id="crawl_pregame_refresh",
        name="Pregame Refresh",
        misfire_grace_time=900,
        max_instances=1,
    )
    logger.info("Registered job: crawl_pregame_refresh (Every 15m, 10:00-23:45 KST, today + lookahead)")

    scheduler.add_job(
        crawl_live_refresh,
        trigger=CronTrigger(hour="12-22", minute="*/2"),
        id="crawl_live_refresh_day",
        name="Live Refresh Day Window",
        misfire_grace_time=180,
        max_instances=1,
    )
    scheduler.add_job(
        crawl_live_refresh,
        trigger=CronTrigger(hour=23, minute="0-30/2"),
        id="crawl_live_refresh_night",
        name="Live Refresh Night Window",
        misfire_grace_time=180,
        max_instances=1,
    )
    logger.info("Registered job: crawl_live_refresh (Every 2m, 12:00-23:30 KST)")

    # Optional one-time startup backfill for missed days
    startup_run = os.getenv("STARTUP_RUN", "1") == "1" and not args.no_startup_run
    if startup_run:
        try:
            logger.info("Performing one-time startup crawl (run_daily_update)")
            crawl_daily_games()
        except Exception:
            logger.exception("Startup crawl failed; scheduler will continue with cron jobs")

        try:
            backfilled = backfill_missed_daily_crawls()
            if backfilled:
                logger.info("Startup backfill completed for dates: %s", backfilled)
        except Exception:
            logger.exception("Startup backfill failed; scheduler will continue with cron jobs")

    print("\n" + "=" * 60)
    print(" KBO Crawler Scheduler Started")
    print("=" * 60)
    print(" Timezone: Asia/Seoul")
    print(f" Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print("\nScheduled Jobs:")
    print("  1. Daily Games Crawl: Every day at 03:00 KST")
    print("  2. Standings Computation: Every day at 03:30 KST")
    print("  3. Team Defense Aggregation: Every day at 03:45 KST")
    print("  4. Rankings Computation: Every day at 04:00 KST")
    print("  5. OCI to Local Sync: Every day at 05:00 KST")
    print("  6. Daily Quality Report: Every day at 05:15 KST")
    print("  7. Phase 1 Extra Crawlers: Every day at 06:00 KST")
    print("  8. Pregame Refresh: Every 15 minutes, 10:00-23:45 KST, today + lookahead")
    print("  9. Live Refresh: Every 2 minutes, 12:00-23:30 KST")
    print(" 10. Futures Profile Sync: Every Sunday at 05:00 KST")
    print(" 11. Park Factor Computation: Every Sunday at 05:30 KST")
    print("=" * 60 + "\n")

    logger.info("Scheduler started successfully")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user")
        print("\nScheduler stopped")


if __name__ == "__main__":
    main()
