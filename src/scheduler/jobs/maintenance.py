"""Maintenance, calculation, audit, and recovery jobs for KBO scheduler."""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime

from tenacity import retry, stop_after_attempt, wait_exponential

from src.cli.crawl_retire import main as crawl_retire_main
from src.db.engine import SessionLocal
from src.scheduler.alerting import alert_failure, alert_success
from src.scheduler.config import (
    KST,
    SCHEDULER_JOB_EXCEPTIONS,
)
from src.scheduler.jobs.live import _previous_day_kst
from src.scheduler.locks import (
    MAINTENANCE_LOCK,
    _scheduler_job_lock,
    _with_lock_skip_guard,
)

logger = logging.getLogger("src.scheduler.jobs.maintenance")

# Write-intent gates required by build_rag_index._write_target_errors for the
# production Oracle RAG target. Scoped to this job only so manual CLI builds
# stay guarded by default.
_RAG_INCREMENTAL_WRITE_ENV = {
    "RAG_TARGET_ENV": "production",
    "RAG_INDEX_ALLOW_WRITE": "1",
    "RAG_INDEX_ALLOW_PRODUCTION_WRITE": "1",
}


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=120, max=600),
    retry_error_callback=alert_failure,
)
@_with_lock_skip_guard
def crawl_retired_players_job(limit: int | None = None) -> None:
    """Monthly job: Crawl retired/inactive player statistics. Runs on 1st of month at 02:00 KST."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Monthly Retired Player Crawl ===")
        try:
            current_year = datetime.now(KST).year
            start_year = 1982
            end_year = current_year - 1

            logger.info("Crawling retired players from %d to %d (active_year=%d)", start_year, end_year, current_year)
            args = [
                "--start-year",
                str(start_year),
                "--end-year",
                str(end_year),
                "--active-year",
                str(current_year),
                "--concurrency",
                "2",
                "--delay",
                "2.0",
            ]
            if limit is not None:
                args.extend(["--limit", str(limit)])

            crawl_retire_main(args)

            logger.info("=== Monthly Retired Player Crawl Completed Successfully ===")
            alert_success("crawl_retired_players_job")

        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Retired player crawl attempt failed")
            raise


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=60, max=300),
    retry_error_callback=alert_failure,
)
@_with_lock_skip_guard
def _crawl_team_info_history() -> None:
    """Weekly job: Refresh team info and team history data."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Team Info/History Refresh ===")
        try:
            from src.crawlers.team_history_crawler import TeamHistoryCrawler
            from src.crawlers.team_info_crawler import TeamInfoCrawler

            crawler_info = TeamInfoCrawler()
            data_info = asyncio.run(crawler_info.crawl(save=True))
            asyncio.run(crawler_info.save(data_info))

            crawler_hist = TeamHistoryCrawler()
            data_hist = asyncio.run(crawler_hist.crawl())
            asyncio.run(crawler_hist.save(data_hist))
            logger.info("=== Team Info/History Refresh Completed ===")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Team info/history refresh failed")


@_with_lock_skip_guard
def weekly_sla_report_job() -> None:
    """Weekly SLA report job: computes past 7 days SLA and alerts. Runs Monday 06:00 KST."""
    from src.monitoring.sla_tracker import SlaTracker

    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Weekly SLA Report Generation ===")
        with SessionLocal() as session:
            tracker = SlaTracker(session)
            tracker.send_weekly_sla_report()
        logger.info("=== Weekly SLA Report Generation Completed ===")


@_with_lock_skip_guard
def compute_standings_job() -> None:
    """Compute daily standings with home/away splits, recent 10, weekly trends."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Standings Computation ===")
        try:
            from src.cli.calculate_standings import compute_all_standings

            current_year = datetime.now(KST).year
            compute_all_standings(current_year)
            logger.info("=== Standings Computation Completed ===")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Standings computation failed")


@_with_lock_skip_guard
def aggregate_team_defense_job() -> None:
    """Aggregate daily team defense statistics (SB, CS, CS%, PB, WP). Runs daily at 03:45 KST."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Team Defense Aggregation ===")
        try:
            from src.aggregators.team_defense_aggregator import aggregate_team_defense

            current_year = datetime.now(KST).year
            aggregate_team_defense(current_year)
            logger.info("=== Team Defense Aggregation Completed ===")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Team defense aggregation failed")


@_with_lock_skip_guard
def compute_rankings_job() -> None:
    """Compute daily player rankings across all categories. Runs daily at 04:00 KST."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Rankings Computation ===")
        try:
            from src.cli.calculate_rankings import rebuild_rankings

            current_year = datetime.now(KST).year
            rebuild_rankings(current_year)
            logger.info("=== Rankings Computation Completed ===")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Rankings computation failed")


@_with_lock_skip_guard
def auto_heal_games_job() -> None:
    """Auto-Healer: scan for stuck SCHEDULED/UNRESOLVED games and score sum mismatches."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Auto-Healer (Stuck & Inconsistent Games) ===")
        try:
            from src.cli.auto_healer import run_healer_async

            unresolved_count = asyncio.run(run_healer_async(dry_run=False))
            if unresolved_count == 0:
                logger.info("=== Auto-Healer Completed (0 unresolved) ===")
            else:
                logger.warning("=== Auto-Healer Completed with unresolved games (count=%d) ===", unresolved_count)
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Auto-Healer job failed")


@_with_lock_skip_guard
def heal_unverified_pbp_job() -> None:
    """PBP Healer: scan for unverified PBP games and re-crawl from KBO official site."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting PBP Auto-Healer ===")
        try:
            import os

            from src.cli.auto_healer import run_pbp_healer

            lookback = os.getenv("PBP_HEALER_LOOKBACK_DAYS", "3")
            exit_code = run_pbp_healer(["--lookback-days", lookback])
            if exit_code == 0:
                logger.info("=== PBP Auto-Healer Completed (no failures) ===")
            else:
                logger.warning("=== PBP Auto-Healer Completed with some failures (exit_code=%d) ===", exit_code)
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("PBP Auto-Healer job failed")


@_with_lock_skip_guard
def data_integrity_check_job() -> None:
    """Run post-crawl data integrity validation (daily at 04:45 KST)."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Data Integrity Check ===")
        try:
            target_date = _previous_day_kst()
            from src.cli.data_integrity_checker import run_integrity_checks

            report = run_integrity_checks(target_date)
            if report.failed_checks == 0:
                logger.info("=== Data Integrity Check Passed (%d checks) ===", report.total_checks)
            else:
                logger.warning(
                    "=== Data Integrity Check Failed (%d/%d checks failed) ===",
                    report.failed_checks,
                    report.total_checks,
                )
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Data Integrity Check job failed")


@_with_lock_skip_guard
def sync_rag_incremental_job() -> None:
    """RAG Vector DB Incremental Sync Job: sync latest season data into the Oracle RAG index."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting RAG Vector DB Incremental Sync ===")
        try:
            from src.cli.build_rag_index import main as build_rag_index_main

            previous_env = {key: os.environ.get(key) for key in _RAG_INCREMENTAL_WRITE_ENV}
            os.environ.update(_RAG_INCREMENTAL_WRITE_ENV)
            try:
                current_year = datetime.now(KST).year
                build_rag_index_main(
                    [
                        "--source",
                        "all",
                        "--season",
                        str(current_year),
                        "--skip-existing",
                    ]
                )
            finally:
                for key, value in previous_env.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value
            logger.info("=== RAG Vector DB Incremental Sync Completed Successfully ===")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("RAG Vector DB Incremental Sync job failed")


@_with_lock_skip_guard
def backup_db_job() -> None:
    """Weekly SQLite Backup & Integrity Check Job (Sunday 02:00 KST)."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Weekly SQLite Backup & Integrity Check ===")
        try:
            from scripts.maintenance.backup_db import run_backup

            res = run_backup()
            if res:
                logger.info("=== Weekly SQLite Backup Completed: %s ===", res)
            else:
                logger.warning("=== Weekly SQLite Backup Failed ===")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Weekly SQLite Backup job failed")


@_with_lock_skip_guard
def compute_park_factor_job() -> None:
    """Compute park factor for all stadiums (Sunday 05:30 KST)."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Park Factor Computation ===")
        try:
            from src.aggregators.park_factor_calculator import ParkFactorCalculator

            current_year = datetime.now(KST).year
            with SessionLocal() as session:
                calc = ParkFactorCalculator(session)
                results = calc.calculate(current_year)
                logger.info("Park Factor computed for %d stadiums", len(results))
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Park Factor computation failed")


@_with_lock_skip_guard
def recalc_milestones_and_rag_job() -> None:
    """Recalculate player milestones and index RAG knowledge chunks."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Milestone Recalculation and RAG Indexing ===")
        try:
            from src.cli.index_rag_knowledge import main as index_main
            from src.cli.recalc_milestones import main as recalc_main

            recalc_main([])
            index_main([])
            logger.info("=== Milestone Recalculation and RAG Indexing Completed ===")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Milestone recalculation and RAG indexing failed")


@_with_lock_skip_guard
def crawl_fan_culture_job() -> None:
    """Fan culture data job: crawl cheer songs, chants, and rivalries from Namuwiki."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("[FanCulture] Starting fan culture data crawl")
        try:
            from src.crawlers.fan_culture_crawler import FanCultureCrawler

            asyncio.run(FanCultureCrawler().run(save=True))
            logger.info("[FanCulture] Fan culture data crawl completed")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Fan culture job failed")
