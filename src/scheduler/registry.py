"""Scheduler registry, job registration, signal handling, and execution orchestrator."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_SUBMITTED
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from src.cli.monthly_unified_audit import crawl_monthly_unified_audit_job
from src.scheduler.config import (
    KST,
    SCHEDULER_JOB_EXCEPTIONS,
    _env_int,
)
from src.scheduler.jobs.daily import (
    backfill_missed_daily_crawls,
    crawl_daily_games,
    crawl_futures_schedule_job,
    crawl_kbo_press_releases_job,
    crawl_p0_non_game_job,
    crawl_p1p2_data_job,
    crawl_phase1_extra_job,
    daily_gap_report_job,
    lock_health_check_job,
)
from src.scheduler.jobs.live import (
    crawl_live_refresh,
    crawl_pregame_refresh,
)
from src.scheduler.jobs.maintenance import (
    _crawl_team_info_history,
    aggregate_team_defense_job,
    auto_heal_games_job,
    backup_db_job,
    cleanup_stale_data_job,
    compute_park_factor_job,
    compute_rankings_job,
    compute_standings_job,
    crawl_fan_culture_job,
    crawl_retired_players_job,
    data_integrity_check_job,
    heal_unverified_pbp_job,
    rag_identity_drift_job,
    recalc_milestones_and_rag_job,
    sparse_terms_catchup_job,
    sync_rag_incremental_job,
    weekly_sla_report_job,
)
from src.scheduler.jobs.sentinel import rag_audit_sentinel_job, selector_drift_sentinel_job
from src.scheduler.jobs.stadium import (
    crawl_congestion_job,
    crawl_operation_notices_job,
    crawl_operation_notices_naver_job,
    crawl_transit_time_job,
)
from src.scheduler.locks import (
    _ensure_single_scheduler_instance,
    lock_skip_monitor_job,
)
from src.scheduler.metrics import job_lifecycle_listener
from src.utils.metrics import start_metrics_server
from src.utils.sentry import init_sentry

logger = logging.getLogger("src.scheduler.registry")

_SCHEDULER_REF: BlockingScheduler | None = None


def build_arg_parser() -> argparse.ArgumentParser:
    """Build command line argument parser for the KBO scheduler."""
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
        "--run-retire-once",
        action="store_true",
        help="Run only one retired player crawl job immediately and exit.",
    )
    parser.add_argument(
        "--run-auto-heal-once",
        action="store_true",
        help="Run only one auto-healer job (stuck/inconsistent games) immediately and exit.",
    )
    parser.add_argument(
        "--run-integrity-check-once",
        action="store_true",
        help="Run only one data integrity check job immediately and exit.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of players for run-retire-once (useful for testing).",
    )
    parser.add_argument(
        "--no-startup-run",
        action="store_true",
        help="Disable one-time startup run regardless of STARTUP_RUN env.",
    )
    return parser


def _shutdown_handler(signum: int, _frame: object) -> None:
    logger.info("Received signal %s. Stopping scheduler gracefully...", signum)
    if _SCHEDULER_REF is not None:
        try:
            # Wait for an in-flight crawler before launchd starts a replacement
            # process, otherwise both processes can write the same game rows.
            _SCHEDULER_REF.shutdown(wait=True)
        except (OSError, RuntimeError) as e:
            logger.warning("Error during scheduler shutdown: %s", e)
    mod = sys.modules.get("scripts.scheduler") or sys.modules.get("src.scheduler")
    for lock_name in ("LIVE_LOCK", "DAILY_LOCK", "MAINTENANCE_LOCK", "SQLITE_WRITE_LOCK"):
        lock = getattr(mod, lock_name, None) if mod else None
        if lock is None:
            continue
        try:
            lock.release()
        except (OSError, RuntimeError) as e:
            logger.warning("Error releasing lock %s: %s", lock.name, e)
    sys.exit(0)


def _dispatch_single_run(args: argparse.Namespace) -> bool:
    if args.run_once:
        crawl_daily_games()
        return True
    if args.run_pregame_once:
        crawl_pregame_refresh()
        return True
    if args.run_retire_once:
        crawl_retired_players_job(limit=args.limit)
        return True
    if args.run_auto_heal_once:
        auto_heal_games_job()
        return True
    if args.run_integrity_check_once:
        data_integrity_check_job()
        return True
    return False


def _start_scheduler(args: argparse.Namespace) -> None:
    mod = sys.modules.get("scripts.scheduler") or sys.modules.get("src.scheduler")
    scheduler_cls = getattr(mod, "BlockingScheduler", BlockingScheduler) if mod else BlockingScheduler
    trigger_cls = getattr(mod, "CronTrigger", CronTrigger) if mod else CronTrigger

    scheduler = scheduler_cls(timezone="Asia/Seoul")
    global _SCHEDULER_REF  # noqa: PLW0603
    _SCHEDULER_REF = scheduler
    scheduler.add_listener(job_lifecycle_listener, EVENT_JOB_SUBMITTED | EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    jobs = [
        (
            selector_drift_sentinel_job,
            trigger_cls(hour=5, minute=40),
            "selector_drift_sentinel",
            "Daily Selector Drift Canary Check",
            3600,
        ),
        (
            crawl_daily_games,
            trigger_cls(hour=3, minute=0),
            "crawl_daily_games",
            "Daily Games Crawl (Schedule + Details)",
            7200,
        ),
        (crawl_phase1_extra_job, trigger_cls(hour=6, minute=0), "crawl_phase1_extra", "Phase 1 Extra Crawlers", 7200),
        (
            crawl_p1p2_data_job,
            trigger_cls(hour=6, minute=45),
            "crawl_p1p2_data",
            "P1/P2 Seat/Parking/Food Crawlers",
            7200,
        ),
        (
            lock_health_check_job,
            trigger_cls(hour=6, minute=50),
            "lock_health_check",
            "Scheduler Lock Health Check (post P1/P2)",
            600,
        ),
        (
            crawl_p0_non_game_job,
            trigger_cls(hour=6, minute=20),
            "crawl_p0_non_game",
            "P0 Non-Game Data Crawl (Events/Roster/Tickets)",
            3600,
        ),
        (
            crawl_retired_players_job,
            trigger_cls(day=1, hour=2, minute=0),
            "crawl_retired_players",
            "Monthly Retired Player Crawl",
            3600,
        ),
        (
            crawl_monthly_unified_audit_job,
            trigger_cls(day=1, hour=3, minute=0),
            "crawl_monthly_unified_audit",
            "Monthly Unified Audit (PA + Team Stats)",
            3600,
        ),
        (
            daily_gap_report_job,
            trigger_cls(hour=7, minute=0),
            "daily_gap_report",
            "Daily Gap Report Summary Notification (07:00 KST)",
            3600,
        ),
        (
            crawl_kbo_press_releases_job,
            trigger_cls(hour=6, minute=10),
            "crawl_kbo_press_releases",
            "KBO Official Press Releases Crawl (06:10 KST)",
            3600,
        ),
        (
            crawl_futures_schedule_job,
            trigger_cls(hour=6, minute=30),
            "crawl_futures_schedule",
            "Futures League Schedule Crawl (06:30 KST)",
            3600,
        ),
        (
            recalc_milestones_and_rag_job,
            trigger_cls(hour=7, minute=15),
            "recalc_milestones_and_rag",
            "Milestone Recalculation and RAG Indexing",
            7200,
        ),
        (
            weekly_sla_report_job,
            trigger_cls(day_of_week="mon", hour=6, minute=0),
            "weekly_sla_report",
            "Weekly SLA Report",
            7200,
        ),
        (
            compute_park_factor_job,
            trigger_cls(day_of_week="sun", hour=5, minute=30),
            "compute_park_factor",
            "Weekly Park Factor Computation",
            7200,
        ),
    ]
    for fn, trigger, job_id, name, grace in jobs:
        scheduler.add_job(
            fn,
            trigger=trigger,
            id=job_id,
            name=name,
            misfire_grace_time=grace,
            max_instances=1,
        )
        logger.info("Registered job: %s", job_id)

    scheduler.add_job(
        crawl_pregame_refresh,
        trigger=trigger_cls(hour="10-23", minute="*/15"),
        id="crawl_pregame_refresh",
        name="Pregame Refresh",
        misfire_grace_time=900,
        max_instances=1,
    )
    scheduler.add_job(
        crawl_live_refresh,
        trigger=trigger_cls(hour="12-22", second="*/10"),
        id="crawl_live_refresh_day",
        name="Live Refresh Day Window",
        misfire_grace_time=5,
        max_instances=1,
    )
    scheduler.add_job(
        crawl_live_refresh,
        trigger=trigger_cls(hour=23, minute="0-30", second="*/10"),
        id="crawl_live_refresh_night",
        name="Live Refresh Night Window",
        misfire_grace_time=5,
        max_instances=1,
    )
    logger.info("Registered job: crawl_live_refresh (Every 10s, 12:00-23:30 KST)")

    tier2_jobs = [
        (compute_standings_job, trigger_cls(hour=3, minute=30), "compute_standings", 7200),
        (aggregate_team_defense_job, trigger_cls(hour=3, minute=45), "aggregate_team_defense", 7200),
        (compute_rankings_job, trigger_cls(hour=4, minute=0), "compute_rankings", 7200),
        (auto_heal_games_job, trigger_cls(hour=4, minute=15), "auto_heal_games", 7200),
        (heal_unverified_pbp_job, trigger_cls(hour=4, minute=30), "heal_pbp", 7200),
        (data_integrity_check_job, trigger_cls(hour=4, minute=45), "data_integrity_check", 7200),
        (sync_rag_incremental_job, trigger_cls(hour=5, minute=0), "sync_rag_incremental", 7200),
        (sparse_terms_catchup_job, trigger_cls(hour=5, minute=40), "sparse_terms_catchup", 7200),
        (rag_audit_sentinel_job, trigger_cls(hour=6, minute=5), "rag_audit_sentinel", 7200),
        (rag_identity_drift_job, trigger_cls(hour=6, minute=20), "rag_identity_drift", 7200),
        (backup_db_job, trigger_cls(day_of_week="sun", hour=2, minute=0), "backup_db_weekly", 7200),
        (
            cleanup_stale_data_job,
            trigger_cls(day_of_week="sun", hour=2, minute=30),
            "cleanup_stale_data_weekly",
            7200,
        ),
    ]
    for fn, trigger, job_id, grace in tier2_jobs:
        scheduler.add_job(fn, trigger=trigger, id=job_id, name=job_id, misfire_grace_time=grace, max_instances=1)
        logger.info("Registered job: %s (Daily)", job_id)

    scheduler.add_job(
        crawl_transit_time_job,
        trigger=trigger_cls(hour="10-23", minute="*/15"),
        id="crawl_transit_time",
        name="Stadium Transit Time Measurement (JAMSIL)",
        misfire_grace_time=600,
        max_instances=1,
    )
    scheduler.add_job(
        crawl_congestion_job,
        trigger=trigger_cls(hour="10-23", minute="*/5"),
        id="crawl_congestion",
        name="Stadium Congestion Data (JAMSIL)",
        misfire_grace_time=300,
        max_instances=1,
    )
    scheduler.add_job(
        crawl_operation_notices_job,
        trigger=trigger_cls(hour=9, minute=0),
        id="crawl_operation_notices_morning",
        name="Operation Notices — Morning",
        misfire_grace_time=3600,
        max_instances=1,
    )
    scheduler.add_job(
        crawl_operation_notices_job,
        trigger=trigger_cls(hour=11, minute=30),
        id="crawl_operation_notices_daygame",
        name="Operation Notices — Day-of-Game",
        misfire_grace_time=3600,
        max_instances=1,
    )
    scheduler.add_job(
        crawl_operation_notices_naver_job,
        trigger=trigger_cls(hour=9, minute=30),
        id="crawl_naver_notices_morning",
        name="Naver Notice — Morning",
        misfire_grace_time=3600,
        max_instances=1,
    )
    scheduler.add_job(
        crawl_operation_notices_naver_job,
        trigger=trigger_cls(hour=13, minute=0),
        id="crawl_naver_notices_afternoon",
        name="Naver Notice — Afternoon",
        misfire_grace_time=3600,
        max_instances=1,
    )
    scheduler.add_job(
        crawl_fan_culture_job,
        trigger=trigger_cls(day_of_week="sat", hour=4, minute=0),
        id="crawl_fan_culture",
        name="Fan Culture Data Crawl",
        misfire_grace_time=7200,
        max_instances=1,
    )
    scheduler.add_job(
        _crawl_team_info_history,
        trigger=trigger_cls(day_of_week="sun", hour=6, minute=0),
        id="crawl_team_info_history",
        name="Team Info/History Refresh",
        misfire_grace_time=7200,
        max_instances=1,
    )
    scheduler.add_job(
        lock_skip_monitor_job,
        trigger=trigger_cls(minute="*/15"),
        id="lock_skip_monitor",
        name="Lock Skip Rate Monitor",
        misfire_grace_time=300,
        max_instances=1,
    )

    if os.getenv("STARTUP_RUN", "1") == "1" and not args.no_startup_run:
        try:
            backfilled = backfill_missed_daily_crawls()
            if backfilled:
                logger.info("Startup backfill completed for dates: %s", backfilled)
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Startup backfill failed; scheduler will continue with cron jobs")

    logger.info(
        "\n%s\n KBO Crawler Scheduler Started\n%s\n Timezone: Asia/Seoul\n Start Time: %s\n%s",
        "=" * 60,
        "=" * 60,
        datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "=" * 60,
    )

    signal.signal(signal.SIGTERM, _shutdown_handler)
    signal.signal(signal.SIGINT, _shutdown_handler)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user")


def main(argv: Sequence[str] | None = None) -> None:
    """Entry point for KBO scheduler."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if _dispatch_single_run(args):
        return
    _ensure_single_scheduler_instance()
    init_sentry()
    start_metrics_server(_env_int("PROMETHEUS_PORT", 8000))
    _start_scheduler(args)
