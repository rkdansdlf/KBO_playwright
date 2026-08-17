"""APScheduler-based automation for KBO data collection.

This module is a lightweight entry point and backward-compatibility facade
for the ``src.scheduler`` package.
"""

from __future__ import annotations

import datetime
import logging
from pathlib import Path
import subprocess
import sys

# Ensure project root is in sys.path BEFORE any src imports
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from requests import RequestException
import requests

from src.cli.live_crawler import run_live_crawler_cycle
from src.cli.run_daily_update import format_stability_alert_summary
from src.cli.run_daily_update import main as run_daily_update_main
from src.db.engine import SessionLocal
from src.scheduler import (
    ALERT_EXCEPTIONS,
    DAILY_LOCK,
    FALSE_ENV_VALUES,
    KST,
    LIVE_LOCK,
    LOCK_SKIP_ALERT_THRESHOLD,
    MAINTENANCE_LOCK,
    SCHEDULER_JOB_EXCEPTIONS,
    SQLITE_WRITE_LOCK,
    SQLITE_WRITE_LOCK_TIMEOUT_SECONDS,
    _LAST_LOCK_SKIP,
    _LockSkipped,
    _SCHEDULER_PID_FILE,
    _SCHEDULER_REF,
    _analyze_game_rows,
    _backfill_phase_detail,
    _backfill_phase_pbp,
    _backfill_phase_preview,
    _backfill_phase_profiles,
    _categorize_game_row,
    _compact_date,
    _crawl_team_info_history,
    _dispatch_single_run,
    _ensure_single_scheduler_instance,
    _env_enabled,
    _env_float,
    _env_int,
    _find_detail_gaps,
    _find_pbp_gaps,
    _find_player_profile_gaps,
    _find_preview_gaps,
    _from_compact_date,
    _get_adaptive_polling_engine,
    _get_live_poll_interval_seconds,
    _interval_from_analysis,
    _live_refresh_max_games_per_cycle,
    _parse_start_time,
    _parse_update_time,
    _pregame_preview_detail_has_starters,
    _pregame_refresh_summary,
    _pregame_target_dates,
    _previous_day_kst,
    _process_pregame_date,
    _release_scheduler_pid_file,
    _scheduler_job_lock,
    _scheduler_pid_alive,
    _scheduler_uses_sqlite_database,
    _should_skip_live_for_pregame,
    _shutdown_handler,
    _sqlite_writer_lock,
    _start_scheduler,
    _to_compact_date,
    _with_lock_skip_guard,
    _write_p1p2_run_marker,
    aggregate_team_defense_job,
    alert_failure,
    alert_success,
    alert_warning,
    auto_heal_games_job,
    backfill_missed_daily_crawls,
    backup_db_job,
    build_arg_parser,
    compute_park_factor_job,
    compute_rankings_job,
    compute_standings_job,
    crawl_congestion_job,
    crawl_daily_games,
    crawl_fan_culture_job,
    crawl_futures_schedule_job,
    crawl_kbo_press_releases_job,
    crawl_live_refresh,
    crawl_operation_notices_job,
    crawl_operation_notices_naver_job,
    crawl_p0_non_game_job,
    crawl_p1p2_data_job,
    crawl_phase1_extra_job,
    crawl_pregame_refresh,
    crawl_retired_players_job,
    crawl_transit_time_job,
    daily_gap_report_job,
    data_integrity_check_job,
    heal_unverified_pbp_job,
    job_lifecycle_listener,
    job_start_times,
    lock_skip_monitor_job,
    log_path,
    main,
    recalc_milestones_and_rag_job,
    sync_rag_incremental_job,
    weekly_sla_report_job,
)
from src.scheduler.jobs.live import (
    LAST_LIVE_POLL_INTERVAL,
    LAST_LIVE_RUN_TIME,
    LAST_PREGAME_RUN_TIME,
)
from src.utils.alerting import SlackWebhookClient
from src.utils.lock import LockAcquisitionError
from src.utils.metrics import KBO_SCHEDULER_LOCK_SKIP_TOTAL

logger = logging.getLogger("scripts.scheduler")
HTTP_STATUS_OK = 200


def selector_drift_sentinel_job() -> None:
    """Daily Canary Check for KBO Website Selector Drift."""
    try:
        from src.monitoring.selector_drift_sentinel import (
            PageContract,
            create_default_kbo_sentinel,
        )

        sentinel = create_default_kbo_sentinel()
        sentinel.register_contract(
            PageContract(
                page_name="schedule",
                required_selectors=(".tbl",),
                min_table_columns={".tbl": 2},
            ),
        )

        page_url = "https://www.koreabaseball.com/Schedule/Schedule.aspx"
        response = requests.get(page_url, timeout=20)
        if response.status_code != HTTP_STATUS_OK:
            logger.warning("[Sentinel] KBO schedule page fetch returned HTTP %s", response.status_code)
            return

        report = sentinel.check_html("schedule", response.text)
        if report.is_healthy:
            logger.info("[Sentinel] Schedule page contract healthy (drift check passed).")
            return

        logger.warning(
            "[Sentinel] Selector drift detected on schedule page: missing_selectors=%s mismatched_columns=%s",
            list(report.missing_selectors),
            list(report.mismatched_columns),
        )
        try:
            from src.utils.alerting import SlackWebhookClient

            SlackWebhookClient.send_alert(
                f"⚠️ Selector drift detected on KBO schedule page: "
                f"missing={list(report.missing_selectors)} columns={list(report.mismatched_columns)}",
            )
        except ALERT_EXCEPTIONS:
            logger.exception("[Sentinel] Failed to send drift alert")
    except (RequestException, RuntimeError, ValueError, TypeError, OSError):
        logger.exception("[Sentinel] Selector drift canary check failed")


def lock_health_check_job() -> None:
    """Post-run lock-health verification for the 06:45 P1/P2 job."""
    logger.info("=== Starting Scheduler Lock Health Check ===")
    try:
        result = subprocess.run(
            [sys.executable, "scripts/check_p1p2_lock_health.py", "--require-run"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        logger.exception("Scheduler lock health check failed to launch")
        alert_warning("lock_health_check", details=f"Could not run check script: {exc}")
        return

    output = f"{result.stdout or ''}{result.stderr or ''}"
    for line in output.splitlines():
        logger.info("[lock_health] %s", line)
    if result.returncode != 0:
        alert_warning("lock_health_check", details=output[-1500:])
        logger.warning("Scheduler lock health check reported problems (alert sent).")
    else:
        logger.info("=== Scheduler Lock Health Check Passed ===")


__all__ = [
    "ALERT_EXCEPTIONS",
    "DAILY_LOCK",
    "FALSE_ENV_VALUES",
    "HTTP_STATUS_OK",
    "KBO_SCHEDULER_LOCK_SKIP_TOTAL",
    "KST",
    "LAST_LIVE_POLL_INTERVAL",
    "LAST_LIVE_RUN_TIME",
    "LAST_PREGAME_RUN_TIME",
    "LIVE_LOCK",
    "LOCK_SKIP_ALERT_THRESHOLD",
    "MAINTENANCE_LOCK",
    "PROJECT_ROOT",
    "SCHEDULER_JOB_EXCEPTIONS",
    "SQLITE_WRITE_LOCK",
    "SQLITE_WRITE_LOCK_TIMEOUT_SECONDS",
    "_LAST_LOCK_SKIP",
    "_SCHEDULER_PID_FILE",
    "_SCHEDULER_REF",
    "BlockingScheduler",
    "CronTrigger",
    "LockAcquisitionError",
    "SessionLocal",
    "SlackWebhookClient",
    "_LockSkipped",
    "_analyze_game_rows",
    "_backfill_phase_detail",
    "_backfill_phase_pbp",
    "_backfill_phase_preview",
    "_backfill_phase_profiles",
    "_categorize_game_row",
    "_compact_date",
    "_crawl_team_info_history",
    "_dispatch_single_run",
    "_ensure_single_scheduler_instance",
    "_env_enabled",
    "_env_float",
    "_env_int",
    "_find_detail_gaps",
    "_find_pbp_gaps",
    "_find_player_profile_gaps",
    "_find_preview_gaps",
    "_from_compact_date",
    "_get_adaptive_polling_engine",
    "_get_live_poll_interval_seconds",
    "_interval_from_analysis",
    "_live_refresh_max_games_per_cycle",
    "_parse_start_time",
    "_parse_update_time",
    "_pregame_preview_detail_has_starters",
    "_pregame_refresh_summary",
    "_pregame_target_dates",
    "_previous_day_kst",
    "_process_pregame_date",
    "_release_scheduler_pid_file",
    "_scheduler_job_lock",
    "_scheduler_pid_alive",
    "_scheduler_uses_sqlite_database",
    "_should_skip_live_for_pregame",
    "_shutdown_handler",
    "_sqlite_writer_lock",
    "_start_scheduler",
    "_to_compact_date",
    "_with_lock_skip_guard",
    "_write_p1p2_run_marker",
    "aggregate_team_defense_job",
    "alert_failure",
    "alert_success",
    "alert_warning",
    "auto_heal_games_job",
    "backfill_missed_daily_crawls",
    "backup_db_job",
    "build_arg_parser",
    "compute_park_factor_job",
    "compute_rankings_job",
    "compute_standings_job",
    "crawl_congestion_job",
    "crawl_daily_games",
    "crawl_fan_culture_job",
    "crawl_futures_schedule_job",
    "crawl_kbo_press_releases_job",
    "crawl_live_refresh",
    "crawl_operation_notices_job",
    "crawl_operation_notices_naver_job",
    "crawl_p0_non_game_job",
    "crawl_p1p2_data_job",
    "crawl_phase1_extra_job",
    "crawl_pregame_refresh",
    "crawl_retired_players_job",
    "crawl_transit_time_job",
    "daily_gap_report_job",
    "data_integrity_check_job",
    "datetime",
    "format_stability_alert_summary",
    "heal_unverified_pbp_job",
    "job_lifecycle_listener",
    "job_start_times",
    "lock_health_check_job",
    "lock_skip_monitor_job",
    "log_path",
    "logger",
    "main",
    "recalc_milestones_and_rag_job",
    "requests",
    "run_daily_update_main",
    "run_live_crawler_cycle",
    "selector_drift_sentinel_job",
    "subprocess",
    "sync_rag_incremental_job",
    "weekly_sla_report_job",
]

if __name__ == "__main__":
    main()
