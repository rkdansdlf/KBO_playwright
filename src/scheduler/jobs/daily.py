"""Daily batch and backfill jobs for KBO scheduler - with enhanced task dependencies."""

from __future__ import annotations

import asyncio
import json
import logging
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

from tenacity import retry, stop_after_attempt, wait_exponential

from src.cli.daily_preview_batch import run_preview_batch
from src.cli.run_daily_update import format_stability_alert_summary
from src.cli.run_daily_update import main as run_daily_update_main
from src.db.engine import SessionLocal
from src.scheduler.alerting import alert_failure, alert_success, alert_warning
from src.scheduler.config import (
    KST,
    PROJECT_ROOT,
    SCHEDULER_JOB_EXCEPTIONS,
    _env_enabled,
    _env_float,
    _env_int,
)
from src.scheduler.jobs.live import _previous_day_kst
from src.scheduler.locks import DAILY_LOCK, MAINTENANCE_LOCK, _scheduler_job_lock, _with_lock_skip_guard

logger = logging.getLogger("src.scheduler.jobs.daily")

P1P2_RUN_MARKER = PROJECT_ROOT / "data" / "last_runs" / "p1p2_data.json"
COMPACT_DATE_LEN = 8
MIN_REAL_PLAYER_ID = 10000


class JobStatus(Enum):
    """Job execution status for dependency tracking."""

    SUCCESS = "success"
    FAILURE = "failure"
    SKIPPED = "skipped"
    RUNNING = "running"


@dataclass
class JobResult:
    """Result of a job execution with dependency tracking."""

    job_name: str
    status: JobStatus
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    dependencies: list[str] = field(default_factory=list)


# Global job state for tracking dependencies
_JOB_REGISTRY: dict[str, JobResult] = {}


def _register_job(job_name: str, dependencies: list[str] | None = None) -> None:
    """Register a job in the dependency registry."""
    _JOB_REGISTRY[job_name] = JobResult(job_name=job_name, status=JobStatus.RUNNING, dependencies=dependencies or [])


def _update_job_status(job_name: str, status: JobStatus, message: str = "", details: dict | None = None) -> None:
    """Update job status in registry."""
    if job_name in _JOB_REGISTRY:
        _JOB_REGISTRY[job_name].status = status
        _JOB_REGISTRY[job_name].message = message
        if details:
            _JOB_REGISTRY[job_name].details = details


def _can_run_job(job_name: str) -> tuple[bool, str]:
    """Check if a job can run based on its dependencies."""
    if job_name not in _JOB_REGISTRY:
        return True, ""

    job = _JOB_REGISTRY[job_name]
    for dep in job.dependencies:
        if dep not in _JOB_REGISTRY:
            return False, f"Dependency {dep} not registered"
        dep_status = _JOB_REGISTRY[dep].status
        if dep_status != JobStatus.SUCCESS:
            return False, f"Dependency {dep} has status {dep_status.value}"
    return True, ""


def _run_legacy_daily_update(
    target_date: str,
    run_main: Callable[..., object],
    fmt_fn: Callable[[object], str],
    alert_succ: Callable[[str, str], None],
    alert_warn: Callable[[str, str], None],
) -> None:
    """Fallback runner executing procedural daily update."""
    args = ["--date", target_date, "--seed-tomorrow-preview"]

    if _env_enabled("DAILY_SKIP_SEASON_STATS", "0"):
        logger.info("Skipping season stat crawl for daily update (DAILY_SKIP_SEASON_STATS=1)")
        args.append("--skip-season-stats")

    if _env_enabled("DAILY_AUTO_REMEDIATION", "1"):
        logger.info("Auto-remediation enabled for daily update (DAILY_AUTO_REMEDIATION=1)")
        args.append("--fix")
    else:
        logger.info("Auto-remediation disabled (DAILY_AUTO_REMEDIATION=0)")

    update_result = run_main(args, acquire_lock=False)

    logger.info("=== Daily Games Crawl Completed Successfully ===")
    alert_succ("crawl_daily_games", fmt_fn(update_result))

    if isinstance(update_result, dict):
        stability = update_result.get("stability", {})
        detail = stability.get("detail", {}) if isinstance(stability, dict) else {}
        detail_recovery = stability.get("detail_recovery", {}) if isinstance(stability, dict) else {}
        detail_counts = detail.get("failure_counts", {}) if isinstance(detail, dict) else {}
        repeated_failures = detail_recovery.get("escalation_game_ids") if isinstance(detail_recovery, dict) else []
        total_failures = sum(detail_counts.values()) if isinstance(detail_counts, dict) else 0
        if repeated_failures or total_failures > 0:
            alert_warn("crawl_daily_games", fmt_fn(update_result))


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=30, max=300),
    retry_error_callback=alert_failure,
)
@_with_lock_skip_guard
def crawl_daily_games() -> None:
    """Daily job: Run unified daily update entrypoint with dependency tracking.

    Runs at 03:00 KST daily to collect previous KST day's schedule+details.
    Uses exponential backoff retry on failures (3 attempts max).
    """
    _register_job("crawl_daily_games", dependencies=[])

    mod = sys.modules.get("scripts.scheduler") or sys.modules.get("src.scheduler")
    daily_lock = getattr(mod, "DAILY_LOCK", DAILY_LOCK) if mod else DAILY_LOCK
    prev_day_fn = getattr(mod, "_previous_day_kst", _previous_day_kst) if mod else _previous_day_kst
    run_main = getattr(mod, "run_daily_update_main", run_daily_update_main) if mod else run_daily_update_main
    fmt_fn = (
        getattr(mod, "format_stability_alert_summary", format_stability_alert_summary)
        if mod
        else format_stability_alert_summary
    )
    alert_succ = getattr(mod, "alert_success", alert_success) if mod else alert_success
    alert_warn = getattr(mod, "alert_warning", alert_warning) if mod else alert_warning

    can_run, reason = _can_run_job("crawl_daily_games")
    if not can_run:
        _update_job_status("crawl_daily_games", JobStatus.SKIPPED, f"Skipped due to dependency: {reason}")
        logger.warning("Skipping crawl_daily_games: %s", reason)
        return

    with _scheduler_job_lock(daily_lock):
        logger.info("=== Starting Daily Games Crawl ===")

        try:
            target_date = prev_day_fn()
            logger.info("Running daily sync pipeline for target_date=%s", target_date)

            if _env_enabled("DAILY_USE_DAG_ORCHESTRATOR", "1"):
                from src.orchestration.master import MasterWorkflowOrchestrator

                orch = MasterWorkflowOrchestrator.build_daily_sync_workflow()
                ctx = {
                    "date": target_date,
                    "skip_season_stats": _env_enabled("DAILY_SKIP_SEASON_STATS", "0"),
                    "auto_remediation": _env_enabled("DAILY_AUTO_REMEDIATION", "1"),
                    "run_main": run_main,
                }
                report = orch.execute_workflow(f"daily_sync_{target_date}", context=ctx)
                logger.info(
                    "=== Master DAG Daily Sync Completed: %s (%d/%d stages) ===",
                    report.overall_status,
                    report.completed_stages,
                    report.total_stages,
                )

                if report.overall_status == "SUCCESS":
                    msg = f"Daily DAG Sync completed ({report.completed_stages}/{report.total_stages} stages)"
                    alert_succ("crawl_daily_games", msg)
                    _update_job_status("crawl_daily_games", JobStatus.SUCCESS, msg)
                elif report.overall_status == "PARTIAL_FAILURE":
                    msg = (
                        f"Daily DAG Sync partial failure "
                        f"({report.failed_stages} failed, {report.skipped_stages} skipped)"
                    )
                    alert_warn("crawl_daily_games", msg)
                    _update_job_status("crawl_daily_games", JobStatus.FAILURE, msg)
                else:
                    err_msg = f"Master DAG Daily Sync failed: {report.overall_status}"
                    _update_job_status("crawl_daily_games", JobStatus.FAILURE, err_msg)
                    raise RuntimeError(err_msg)
                return

            _run_legacy_daily_update(target_date, run_main, fmt_fn, alert_succ, alert_warn)
            _update_job_status("crawl_daily_games", JobStatus.SUCCESS, "Legacy daily update completed")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Daily games crawl attempt failed")
            _update_job_status("crawl_daily_games", JobStatus.FAILURE, "Exception during execution")
            raise


def _compact_date(d: object) -> str:
    if hasattr(d, "strftime"):
        return d.strftime("%Y%m%d")  # type: ignore[union-attr]
    return str(d).replace("-", "")


def _to_compact_date(value: str) -> str:
    if isinstance(value, str) and len(value) == COMPACT_DATE_LEN and value.isdigit():
        return value
    msg = f"invalid compact date value: {value!r}"
    raise ValueError(msg)


def _from_compact_date(value: str) -> date:
    return datetime.strptime(_to_compact_date(value), "%Y%m%d").replace(tzinfo=KST).date()


def _find_detail_gaps(session: object, start_date: date) -> list[str]:
    """Find dates with COMPLETED/DRAW games missing batting or pitching stats."""
    from sqlalchemy import text as sa_text

    rows = (
        session.execute(  # type: ignore[attr-defined]
            sa_text("""
            SELECT DISTINCT g.game_date
            FROM game g
            LEFT JOIN game_batting_stats b ON g.game_id = b.game_id
            LEFT JOIN game_pitching_stats p ON g.game_id = p.game_id
            WHERE g.game_date >= :start
              AND g.game_status IN ('COMPLETED', 'DRAW')
              AND (b.game_id IS NULL OR p.game_id IS NULL)
            ORDER BY g.game_date
        """),
            {"start": start_date},
        )
        .scalars()
        .all()
    )
    return [_compact_date(d) for d in rows]


def _find_pbp_gaps(session: object, start_date: date) -> list[str]:
    """Find dates with COMPLETED/DRAW games missing game_play_by_play."""
    from sqlalchemy import text as sa_text

    rows = (
        session.execute(  # type: ignore[attr-defined]
            sa_text("""
            SELECT DISTINCT g.game_date
            FROM game g
            LEFT JOIN game_play_by_play p ON g.game_id = p.game_id
            WHERE g.game_date >= :start
              AND g.game_status IN ('COMPLETED', 'DRAW')
              AND p.game_id IS NULL
            ORDER BY g.game_date
        """),
            {"start": start_date},
        )
        .scalars()
        .all()
    )
    return [_compact_date(d) for d in rows]


def _find_preview_gaps(session: object, start_date: date) -> list[str]:
    """Find dates with SCHEDULED games missing pregame preview data."""
    from sqlalchemy import text as sa_text

    rows = (
        session.execute(  # type: ignore[attr-defined]
            sa_text("""
            SELECT DISTINCT g.game_date
            FROM game g
            LEFT JOIN game_summary s ON s.game_id = g.game_id AND s.summary_type = '프리뷰'
            WHERE g.game_date >= :start
              AND UPPER(g.game_status) = 'SCHEDULED'
              AND s.game_id IS NULL
            ORDER BY g.game_date
        """),
            {"start": start_date},
        )
        .scalars()
        .all()
    )
    return [_compact_date(d) for d in rows]


def _find_player_profile_gaps(session: object) -> list[int]:
    """Find player IDs missing photo_url (excludes pseudo/not-found status)."""
    from sqlalchemy import or_

    from src.models.player import PlayerBasic

    rows = (
        session.query(PlayerBasic.player_id)  # type: ignore[attr-defined]
        .filter(
            PlayerBasic.photo_url.is_(None),
            PlayerBasic.player_id >= MIN_REAL_PLAYER_ID,
            or_(PlayerBasic.status.is_(None), ~PlayerBasic.status.in_(["NOT_FOUND", "PSEUDO"])),
        )
        .all()
    )
    return [row.player_id for row in rows]


def _get_session_local() -> type[SessionLocal]:
    from src.db import engine

    return getattr(engine, "SessionLocal", SessionLocal)


def _get_run_daily_update() -> object:
    mod = sys.modules.get("scripts.scheduler")
    if mod and hasattr(mod, "run_daily_update_main"):
        return mod.run_daily_update_main
    return run_daily_update_main


def _get_run_preview_batch() -> object:
    mod = sys.modules.get("scripts.scheduler")
    if mod and hasattr(mod, "run_preview_batch"):
        return mod.run_preview_batch
    return run_preview_batch


def _dispatch_find_detail_gaps(session: object, start_date: date) -> list[str]:
    mod = sys.modules.get("scripts.scheduler")
    if mod and hasattr(mod, "_find_detail_gaps"):
        return mod._find_detail_gaps(session, start_date)  # noqa: SLF001
    return _find_detail_gaps(session, start_date)


def _dispatch_find_pbp_gaps(session: object, start_date: date) -> list[str]:
    mod = sys.modules.get("scripts.scheduler")
    if mod and hasattr(mod, "_find_pbp_gaps"):
        return mod._find_pbp_gaps(session, start_date)  # noqa: SLF001
    return _find_pbp_gaps(session, start_date)


def _dispatch_find_preview_gaps(session: object, start_date: date) -> list[str]:
    mod = sys.modules.get("scripts.scheduler")
    if mod and hasattr(mod, "_find_preview_gaps"):
        return mod._find_preview_gaps(session, start_date)  # noqa: SLF001
    return _find_preview_gaps(session, start_date)


def _dispatch_find_player_profile_gaps(session: object) -> list[int]:
    mod = sys.modules.get("scripts.scheduler")
    if mod and hasattr(mod, "_find_player_profile_gaps"):
        return mod._find_player_profile_gaps(session)  # noqa: SLF001
    return _find_player_profile_gaps(session)


def _backfill_phase_detail(start: date, backfilled: list[str]) -> list[str]:
    session_factory = _get_session_local()
    with session_factory() as session:
        detail_dates = _dispatch_find_detail_gaps(session, start)
    run_update = _get_run_daily_update()
    for date_compact in detail_dates:
        logger.warning("Phase 1 — Detail backfill needed for %s", date_compact)
        try:
            run_update(["--date", date_compact])
            with session_factory() as verify_session:
                if date_compact not in _dispatch_find_detail_gaps(verify_session, _from_compact_date(date_compact)):
                    backfilled.append(f"detail:{date_compact}")
                    logger.info("Phase 1 — Detail backfill completed for %s", date_compact)
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Phase 1 — Detail backfill failed for %s", date_compact)
    return detail_dates


def _backfill_phase_pbp(start: date, detail_dates: list[str], backfilled: list[str]) -> None:
    session_factory = _get_session_local()
    with session_factory() as session:
        all_dates = sorted(set(detail_dates + _dispatch_find_pbp_gaps(session, start)))
    run_update = _get_run_daily_update()
    for date_compact in all_dates:
        with session_factory() as verify_session:
            verify_date = _from_compact_date(date_compact)
            detail_still_missing = date_compact in _dispatch_find_detail_gaps(verify_session, verify_date)
            pbp_still_missing = date_compact in _dispatch_find_pbp_gaps(verify_session, verify_date)
        if not (detail_still_missing or pbp_still_missing):
            continue
        logger.warning("Phase 2 — PBP/relay backfill needed for %s", date_compact)
        try:
            run_update(["--date", date_compact])
            backfilled.append(f"pbp:{date_compact}")
            logger.info("Phase 2 — PBP/relay backfill completed for %s", date_compact)
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Phase 2 — PBP/relay backfill failed for %s", date_compact)


def _backfill_phase_preview(start: date, backfilled: list[str]) -> None:
    session_factory = _get_session_local()
    with session_factory() as session:
        preview_dates = _dispatch_find_preview_gaps(session, start)
    run_preview = _get_run_preview_batch()
    for date_compact in preview_dates:
        logger.warning("Phase 3 — Preview backfill needed for %s", date_compact)
        try:
            asyncio.run(run_preview(date_compact))
            backfilled.append(f"preview:{date_compact}")
            logger.info("Phase 3 — Preview backfill completed for %s", date_compact)
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Phase 3 — Preview backfill failed for %s", date_compact)


def _backfill_phase_profiles(backfilled: list[str]) -> None:
    session_factory = _get_session_local()
    with session_factory() as session:
        profile_gap_ids = _dispatch_find_player_profile_gaps(session)
    if not profile_gap_ids:
        return
    batch_size = max(0, _env_int("PROFILE_BACKFILL_BATCH_SIZE", 50))
    batch = profile_gap_ids[:batch_size]
    logger.warning(
        "Phase 4 — Profile backfill: %s players need profiles (processing %s)", len(profile_gap_ids), len(batch)
    )
    if not batch:
        return
    try:
        from scripts.backfill_player_profiles import backfill as backfill_player_profiles_fn

        asyncio.run(
            backfill_player_profiles_fn(
                limit=len(batch), delay=_env_float("PROFILE_BACKFILL_DELAY", 2.0), ids=[str(i) for i in batch]
            )
        )
        backfilled.append(f"profiles:{len(batch)}")
        logger.info("Phase 4 — Profile backfill completed for %s players", len(batch))
    except SCHEDULER_JOB_EXCEPTIONS:
        logger.exception("Phase 4 — Profile backfill failed")


def backfill_missed_daily_crawls(lookback_days: int = 14) -> list[str]:
    """Execute multi-phase backfill for missed details, PBP, preview, and player profiles."""
    _register_job("backfill_missed_daily_crawls", dependencies=["crawl_daily_games"])

    can_run, reason = _can_run_job("backfill_missed_daily_crawls")
    if not can_run:
        _update_job_status("backfill_missed_daily_crawls", JobStatus.SKIPPED, f"Skipped: {reason}")
        logger.warning("Skipping backfill_missed_daily_crawls: %s", reason)
        return []

    start = datetime.now(KST).date() - timedelta(days=lookback_days)
    backfilled: list[str] = []

    _register_job("backfill_phase_detail", dependencies=["crawl_daily_games"])
    detail_dates = _backfill_phase_detail(start, backfilled)

    _register_job("backfill_phase_pbp", dependencies=["backfill_phase_detail"])
    _backfill_phase_pbp(start, detail_dates, backfilled)

    _register_job("backfill_phase_preview", dependencies=["backfill_phase_pbp"])
    _backfill_phase_preview(start, backfilled)

    _register_job("backfill_phase_profiles", dependencies=["backfill_phase_preview"])
    _backfill_phase_profiles(backfilled)

    _update_job_status("backfill_missed_daily_crawls", JobStatus.SUCCESS, f"Backfilled: {backfilled}")
    return backfilled


@_with_lock_skip_guard
def crawl_phase1_extra_job() -> None:
    """Phase 1: Supplementary crawlers (broadcast, MVP, injury, foreign players, manager changes)."""
    _register_job("crawl_phase1_extra_job", dependencies=["crawl_daily_games"])

    can_run, reason = _can_run_job("crawl_phase1_extra_job")
    if not can_run:
        _update_job_status("crawl_phase1_extra_job", JobStatus.SKIPPED, f"Skipped: {reason}")
        logger.warning("Skipping crawl_phase1_extra_job: %s", reason)
        return

    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Phase 1 Extra Crawlers ===")
        try:
            from src.cli.crawl_phase1_extra import run_all_crawlers

            asyncio.run(run_all_crawlers(save=True))
            logger.info("=== Phase 1 Extra Crawlers Completed Successfully ===")
            _update_job_status("crawl_phase1_extra_job", JobStatus.SUCCESS, "Completed")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Phase 1 extra crawlers failed")
            _update_job_status("crawl_phase1_extra_job", JobStatus.FAILURE, "Exception")


def _write_p1p2_run_marker(status: str) -> None:
    """Record the last P1/P2 job run so the lock-health check can verify it ran today."""
    try:
        P1P2_RUN_MARKER.parent.mkdir(parents=True, exist_ok=True)
        P1P2_RUN_MARKER.write_text(
            json.dumps({"ts": datetime.now(KST).isoformat(), "status": status}, ensure_ascii=False),
            encoding="utf-8",
        )
    except OSError:
        logger.exception("Failed to write P1/P2 run marker")


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=300, max=1800),
    retry_error_callback=alert_failure,
)
@_with_lock_skip_guard
def crawl_p1p2_data_job() -> None:
    """P1/P2 Crawlers: seat sections, parking, stadium food."""
    _register_job("crawl_p1p2_data_job", dependencies=["crawl_daily_games"])

    can_run, reason = _can_run_job("crawl_p1p2_data_job")
    if not can_run:
        _update_job_status("crawl_p1p2_data_job", JobStatus.SKIPPED, f"Skipped: {reason}")
        logger.warning("Skipping crawl_p1p2_data_job: %s", reason)
        _write_p1p2_run_marker("skipped")
        return

    with _scheduler_job_lock(DAILY_LOCK):
        logger.info("=== Starting P1/P2 Data Crawlers ===")
        try:
            from src.cli.crawl_parking import main as parking_main
            from src.cli.crawl_seat_sections import main as seat_main
            from src.cli.crawl_stadium_food import main as food_main

            logger.info("Running seat sections crawler...")
            seat_main(["--save"])
            logger.info("Running parking crawler...")
            parking_main(["--save"])
            logger.info("Running stadium food crawler...")
            food_main(["--save"])
            logger.info("=== P1/P2 Data Crawlers Completed Successfully ===")
            alert_success("crawl_p1p2_data_job")
            _write_p1p2_run_marker("ok")
            _update_job_status("crawl_p1p2_data_job", JobStatus.SUCCESS, "Completed")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("P1/P2 data crawlers failed")
            _write_p1p2_run_marker("error")
            _update_job_status("crawl_p1p2_data_job", JobStatus.FAILURE, "Exception")


def lock_health_check_job() -> None:
    """Post-run lock-health verification for the 06:45 P1/P2 job."""
    _register_job("lock_health_check_job", dependencies=["crawl_p1p2_data_job"])

    can_run, reason = _can_run_job("lock_health_check_job")
    if not can_run:
        _update_job_status("lock_health_check_job", JobStatus.SKIPPED, f"Skipped: {reason}")
        logger.warning("Skipping lock_health_check_job: %s", reason)
        return

    target_mod = sys.modules.get("scheduler_under_test") or sys.modules.get("scripts.scheduler")
    sp = getattr(target_mod, "subprocess", subprocess) if target_mod else subprocess
    warn_fn = getattr(target_mod, "alert_warning", alert_warning) if target_mod else alert_warning

    logger.info("=== Starting Scheduler Lock Health Check ===")
    try:
        result = sp.run(
            [sys.executable, "scripts/check_p1p2_lock_health.py", "--require-run"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        logger.exception("Scheduler lock health check failed to launch")
        warn_fn("lock_health_check", details=f"Could not run check script: {exc}")
        _update_job_status("lock_health_check_job", JobStatus.FAILURE, f"Launch failed: {exc}")
        return

    output = f"{result.stdout or ''}{result.stderr or ''}"
    for line in output.splitlines():
        logger.info("[lock_health] %s", line)
    if result.returncode != 0:
        warn_fn("lock_health_check", details=output[-1500:])
        logger.warning("Scheduler lock health check reported problems (alert sent).")
        _update_job_status("lock_health_check_job", JobStatus.FAILURE, "Health check failed")
    else:
        logger.info("=== Scheduler Lock Health Check Passed ===")
        _update_job_status("lock_health_check_job", JobStatus.SUCCESS, "Passed")


@_with_lock_skip_guard
def crawl_p0_non_game_job() -> None:
    """P0 non-game job: crawl team events, roster transactions, and ticket info."""
    _register_job("crawl_p0_non_game_job", dependencies=["crawl_daily_games"])

    can_run, reason = _can_run_job("crawl_p0_non_game_job")
    if not can_run:
        _update_job_status("crawl_p0_non_game_job", JobStatus.SKIPPED, f"Skipped: {reason}")
        logger.warning("Skipping crawl_p0_non_game_job: %s", reason)
        return

    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("[P0NonGame] Starting P0 non-game crawl")
        try:
            from src.cli.crawl_p0_data import main as crawl_p0_data_main

            crawl_p0_data_main(
                [
                    "--type",
                    "all",
                    "--save",
                    "--days",
                    "3",
                    "--season",
                    str(datetime.now(KST).year),
                ],
            )
            logger.info("[P0NonGame] P0 non-game crawl completed")
            _update_job_status("crawl_p0_non_game_job", JobStatus.SUCCESS, "Completed")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("P0 non-game job failed")
            _update_job_status("crawl_p0_non_game_job", JobStatus.FAILURE, "Exception")


@_with_lock_skip_guard
def crawl_kbo_press_releases_job() -> None:
    """Crawl KBO official press releases and notices. Runs daily at 06:10 KST."""
    _register_job("crawl_kbo_press_releases_job", dependencies=["crawl_daily_games"])

    can_run, reason = _can_run_job("crawl_kbo_press_releases_job")
    if not can_run:
        _update_job_status("crawl_kbo_press_releases_job", JobStatus.SKIPPED, f"Skipped: {reason}")
        logger.warning("Skipping crawl_kbo_press_releases_job: %s", reason)
        return

    with _scheduler_job_lock(DAILY_LOCK):
        logger.info("=== Starting KBO Press Releases Crawl ===")
        try:
            from src.cli.crawl_press_releases import main as press_main

            press_main(["--save", "--max-pages", "1"])
            logger.info("=== KBO Press Releases Crawl Completed ===")
            _update_job_status("crawl_kbo_press_releases_job", JobStatus.SUCCESS, "Completed")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("KBO press releases crawl failed")
            _update_job_status("crawl_kbo_press_releases_job", JobStatus.FAILURE, "Exception")


@_with_lock_skip_guard
def crawl_futures_schedule_job() -> None:
    """Crawl Futures League game schedule and standings. Runs daily at 06:30 KST."""
    _register_job("crawl_futures_schedule_job", dependencies=["crawl_daily_games"])

    can_run, reason = _can_run_job("crawl_futures_schedule_job")
    if not can_run:
        _update_job_status("crawl_futures_schedule_job", JobStatus.SKIPPED, f"Skipped: {reason}")
        logger.warning("Skipping crawl_futures_schedule_job: %s", reason)
        return

    with _scheduler_job_lock(DAILY_LOCK):
        logger.info("=== Starting Futures League Schedule Crawl ===")
        try:
            from src.cli.crawl_futures_schedule import main as futures_main

            futures_main(["--save"])
            logger.info("=== Futures League Schedule Crawl Completed ===")
            _update_job_status("crawl_futures_schedule_job", JobStatus.SUCCESS, "Completed")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Futures League schedule crawl failed")
            _update_job_status("crawl_futures_schedule_job", JobStatus.FAILURE, "Exception")


@_with_lock_skip_guard
def daily_gap_report_job() -> None:
    """Daily Gap Report Summary: run gap report and send summary notification at 07:00 KST."""
    _register_job("daily_gap_report_job", dependencies=["crawl_daily_games", "crawl_p0_non_game_job"])

    can_run, reason = _can_run_job("daily_gap_report_job")
    if not can_run:
        _update_job_status("daily_gap_report_job", JobStatus.SKIPPED, f"Skipped: {reason}")
        logger.warning("Skipping daily_gap_report_job: %s", reason)
        return

    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Daily Gap Report Summary Notification ===")
        try:
            from src.cli.gap_report import run_gap_report

            run_gap_report(alert=True, send_summary=True)
            logger.info("=== Daily Gap Report Summary Completed Successfully ===")
            _update_job_status("daily_gap_report_job", JobStatus.SUCCESS, "Completed")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Daily Gap Report job failed")
            _update_job_status("daily_gap_report_job", JobStatus.FAILURE, "Exception")


def get_job_status_summary() -> dict[str, Any]:
    """Get summary of all registered jobs for monitoring."""
    return {
        name: {
            "status": result.status.value,
            "message": result.message,
            "details": result.details,
            "dependencies": result.dependencies,
        }
        for name, result in _JOB_REGISTRY.items()
    }


def clear_job_registry() -> None:
    """Clear the job registry (useful for testing)."""
    _JOB_REGISTRY.clear()
