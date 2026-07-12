"""APScheduler-based automation for KBO data collection.

Note: Daily post-processing (finalize, standings, defense, rankings, PBP healer,
batch parse, quality report, gap report, freshness monitor) and Tier 2 backfills
(SH/SF, advanced stats, player IDs, roster) are now handled by GitHub Actions
via .github/workflows/daily_kbo_sync.yml and backfill.yml (consolidated).

APScheduler focuses on real-time and local-only jobs:

=== Live / Game-Day Jobs ===
  - crawl_pregame_refresh: Every 15m, 10:00-23:45 KST (LIVE_LOCK)
  - crawl_live_refresh: Every 10s, 12:00-23:30 KST (LIVE_LOCK — 2 windows)

=== Stadium Real-Time Data ===
  - crawl_transit_time: Every 15m, 10:00-23:45 KST (LIVE_LOCK)
  - crawl_congestion: Every 5m, 10:00-23:55 KST (LIVE_LOCK)
  - crawl_operation_notices: 09:00 + 11:30 KST daily (DAILY_LOCK)
  - crawl_operation_notices_naver: 09:30 + 13:00 KST daily

=== Daily Jobs ===
  - crawl_phase1_extra: Daily at 06:00 KST (DAILY_LOCK)
  - crawl_p0_non_game: Daily at 06:20 KST (DAILY_LOCK)
  - crawl_p1p2_data: Daily at 06:30 KST (DAILY_LOCK)
  - startup backfill (run_daily_update) on first scheduler start

=== Weekly Jobs ===
  - weekly_sla_report: Monday 06:00 KST (MAINTENANCE_LOCK)
  - compute_park_factor: Sunday 05:30 KST (MAINTENANCE_LOCK)
  - crawl_fan_culture: Saturday 04:00 KST (MAINTENANCE_LOCK)

=== Monthly Jobs ===
  - crawl_retired_players: 1st at 02:00 KST (MAINTENANCE_LOCK)
  - crawl_monthly_unified_audit: 1st at 03:00 KST (MAINTENANCE_LOCK)

=== Locks ===
  - LIVE_LOCK: pregame, live_refresh, transit_time, congestion
  - DAILY_LOCK: phase1_extra, p0_non_game, p1p2_data, operation_notices
  - MAINTENANCE_LOCK: sla_report, park_factor, fan_culture, retire, audit
  - REALTIME_OCI_SYNC_LOCK: background OCI sync thread (separate from scheduler)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from threading import Thread
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

import time
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_SUBMITTED
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import sentry_sdk
from requests import RequestException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from tenacity import retry, stop_after_attempt, wait_exponential

from src.cli.crawl_retire import main as crawl_retire_main
from src.cli.daily_preview_batch import run_preview_batch
from src.cli.live_crawler import run_live_crawler_cycle
from src.cli.monthly_unified_audit import crawl_monthly_unified_audit_job
from src.cli.run_daily_update import format_stability_alert_summary
from src.cli.run_daily_update import main as run_daily_update_main
from src.db.engine import DATABASE_URL, SessionLocal
from src.sync.oci_sync import OCISync
from src.utils.alerting import SlackWebhookClient
from src.utils.lock import ForceProcessLock, ProcessLock
from src.utils.sentry import init_sentry
from src.utils.metrics import (
    start_metrics_server,
    KBO_SCHEDULER_JOB_TOTAL,
    KBO_SCHEDULER_JOB_DURATION_SECONDS,
    KBO_OCI_SYNC_LAG_SECONDS,
    KBO_OCI_LAST_SYNC_TIMESTAMP_SECONDS,
    KBO_OCI_SYNC_ERRORS_TOTAL,
)

# Stadium real-time data job functions (imported lazily inside job bodies to avoid startup overhead)

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

ALERT_EXCEPTIONS = (OSError, RuntimeError, ValueError, RequestException)
SCHEDULER_JOB_EXCEPTIONS = (
    RuntimeError,
    OSError,
    ValueError,
    TypeError,
    LookupError,
    SQLAlchemyError,
    RequestException,
    asyncio.TimeoutError,
    json.JSONDecodeError,
)

# Granular locking to prevent long-running batch jobs from blocking real-time updates
LIVE_LOCK = ProcessLock("live_refresh")
DAILY_LOCK = ProcessLock("daily_update")
MAINTENANCE_LOCK = ForceProcessLock("maintenance")
REALTIME_OCI_SYNC_LOCK = ProcessLock("realtime_oci_sync")
SQLITE_WRITE_LOCK = ForceProcessLock("sqlite_writer")

MISSING_PREGAME_ALERTED_DATES: set[str] = set()
LAST_LIVE_RUN_TIME: datetime | None = None
LAST_LIVE_POLL_INTERVAL: int | None = None
LAST_PREGAME_RUN_TIME: datetime | None = None


def _scheduler_uses_sqlite_database() -> bool:
    database_url = os.getenv("DATABASE_URL", DATABASE_URL)
    return database_url.startswith("sqlite:")


@contextmanager
def _sqlite_writer_lock() -> Iterator[None]:
    if not _scheduler_uses_sqlite_database():
        yield
        return

    with SQLITE_WRITE_LOCK:
        yield


@contextmanager
def _scheduler_job_lock(lock: ProcessLock) -> Iterator[None]:
    with lock, _sqlite_writer_lock():
        yield


def _live_refresh_max_games_per_cycle() -> int | None:
    raw_value = os.getenv("LIVE_REFRESH_MAX_GAMES_PER_CYCLE", "1").strip().lower()
    if raw_value in FALSE_ENV_VALUES or raw_value == "all":
        return None
    try:
        max_games = int(raw_value)
    except ValueError:
        logger.warning("Invalid LIVE_REFRESH_MAX_GAMES_PER_CYCLE=%r; defaulting to 1", raw_value)
        return 1
    return max_games if max_games > 0 else None


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
    except ALERT_EXCEPTIONS:
        logger.exception("Failed to send failure alert for job %s", func_name)

    # Do NOT re-raise — let retry_error_callback suppress so the scheduler survives.
    if exc:
        logger.warning(f"Job {func_name} permanently failed but scheduler continues: {error_text}")
    return


def alert_warning(func_name: str, details: str | None = None):
    """Send a warning alert for partial failures or non-critical issues."""
    try:
        message = f"⚠️ KBO Job {func_name} has warnings."
        if details:
            message = f"{message}\n{details}"
        SlackWebhookClient.send_alert(message)
    except ALERT_EXCEPTIONS:
        logger.exception("Failed to send warning alert for job %s", func_name)


def alert_success(func_name: str, details: str | None = None):
    """Send optional success notification."""
    if os.getenv("NOTIFY_SUCCESS", "0") == "1":
        try:
            message = f"✅ KBO Job {func_name} completed successfully."
            if details:
                message = f"{message}\n{details}"
            SlackWebhookClient.send_alert(message)
        except ALERT_EXCEPTIONS:
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


def _should_skip_live_for_pregame(now: datetime | None = None) -> bool:
    global LAST_PREGAME_RUN_TIME
    try:
        cooldown_seconds = int(os.getenv("LIVE_PREGAME_COOLDOWN_SECONDS", "30"))
    except ValueError:
        cooldown_seconds = 30
    if cooldown_seconds <= 0:
        return False

    current = now or datetime.now(KST)
    if LAST_PREGAME_RUN_TIME is None:
        return False
    elapsed = (current - LAST_PREGAME_RUN_TIME).total_seconds()
    return 0 <= elapsed < cooldown_seconds


def _pregame_refresh_summary(target_date: str) -> tuple[int, int, int]:
    """Return scheduled games, missing starters count, and preview-missing count for a date."""
    try:
        datetime.strptime(target_date, "%Y%m%d")
    except ValueError:
        return 0, 0, 0

    query = text(
        """
        SELECT
            g.game_id,
            g.away_pitcher,
            g.home_pitcher,
            p.detail_text AS preview_detail_text
        FROM game g
        LEFT JOIN (
            SELECT gs.game_id, gs.detail_text
            FROM game_summary gs
            JOIN (
                SELECT game_id, MAX(id) AS id
                FROM game_summary
                WHERE summary_type = '프리뷰'
                GROUP BY game_id
            ) latest ON latest.id = gs.id
        ) p ON p.game_id = g.game_id
        WHERE UPPER(g.game_status) = 'SCHEDULED'
          AND REPLACE(CAST(g.game_date AS TEXT), '-', '') = :target_date
        """,
    )

    with SessionLocal() as session:
        rows = session.execute(query, {"target_date": target_date}).all()

    if not rows:
        return 0, 0, 0

    scheduled_total = len(rows)
    starters_missing = 0
    preview_missing = 0
    for row in rows:
        if not str(row.away_pitcher or "").strip() or not str(row.home_pitcher or "").strip():
            starters_missing += 1
        if row.preview_detail_text is None or not _pregame_preview_detail_has_starters(row.preview_detail_text):
            preview_missing += 1

    return scheduled_total, starters_missing, preview_missing


def _pregame_preview_detail_has_starters(detail_text: str | None) -> bool:
    if not detail_text:
        return False
    try:
        payload = json.loads(detail_text)
    except (TypeError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict):
        return False
    return bool(str(payload.get("away_starter") or "").strip()) and bool(str(payload.get("home_starter") or "").strip())


def _env_enabled(name: str, default: str = "1") -> bool:
    return os.getenv(name, default).strip().lower() not in FALSE_ENV_VALUES


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid integer for %s=%r; using default=%d", name, raw, default)
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid float for %s=%r; using default=%s", name, raw, default)
        return default


def _pregame_sync_to_oci_enabled() -> bool:
    return _env_enabled("PREGAME_SYNC_TO_OCI") and bool(os.getenv("OCI_DB_URL"))


def _realtime_oci_sync_worker(sync_kind: str, oci_url: str, target_game_ids: list[str], method_name: str) -> None:
    started_at = datetime.now(KST)
    succeeded = 0
    failed_game_ids: list[str] = []
    try:
        logger.info("Starting background realtime OCI %s sync games=%s", sync_kind, ",".join(target_game_ids))
        for game_id in target_game_ids:
            syncer = None
            game_started_at = datetime.now(KST)
            try:
                with SessionLocal() as sync_session:
                    syncer = OCISync(oci_url, sync_session)
                    getattr(syncer, method_name)(game_id)
                succeeded += 1
                logger.info(
                    "Background realtime OCI %s sync succeeded game_id=%s elapsed=%.1fs",
                    sync_kind,
                    game_id,
                    (datetime.now(KST) - game_started_at).total_seconds(),
                )
            except SCHEDULER_JOB_EXCEPTIONS as e:
                failed_game_ids.append(game_id)
                KBO_OCI_SYNC_ERRORS_TOTAL.inc()
                sentry_sdk.capture_exception(e)
                logger.exception("Background realtime OCI %s sync failed game_id=%s", sync_kind, game_id)
            finally:
                if syncer is not None:
                    try:
                        syncer.close()
                    except SCHEDULER_JOB_EXCEPTIONS as e:
                        sentry_sdk.capture_exception(e)
                        logger.exception(
                            "Failed to close background realtime OCI %s syncer game_id=%s", sync_kind, game_id
                        )
        if succeeded > 0:
            KBO_OCI_LAST_SYNC_TIMESTAMP_SECONDS.set(time.time())
    except SCHEDULER_JOB_EXCEPTIONS as e:
        KBO_OCI_SYNC_ERRORS_TOTAL.inc()
        sentry_sdk.capture_exception(e)
        logger.exception("Background realtime OCI %s sync setup failed", sync_kind)
    finally:
        REALTIME_OCI_SYNC_LOCK.release()
        logger.info(
            "Background realtime OCI %s sync finished succeeded=%d failed=%d failed_game_ids=%s elapsed=%.1fs",
            sync_kind,
            succeeded,
            len(failed_game_ids),
            ",".join(failed_game_ids) or "-",
            (datetime.now(KST) - started_at).total_seconds(),
        )


def _submit_realtime_oci_sync(sync_kind: str, game_ids: Sequence[str]) -> bool:
    target_game_ids = sorted({str(game_id) for game_id in game_ids if game_id})
    if not target_game_ids:
        return False
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        logger.warning("Skipping realtime OCI %s sync because OCI_DB_URL is not set", sync_kind)
        return False
    method_by_kind = {"pregame": "sync_pregame_game", "live": "sync_specific_game"}
    method_name = method_by_kind.get(sync_kind)
    if method_name is None:
        msg = f"Unsupported realtime OCI sync kind: {sync_kind}"
        raise ValueError(msg)
    if not REALTIME_OCI_SYNC_LOCK.acquire(blocking=False):
        logger.warning(
            "Skipping realtime OCI %s sync because a prior realtime OCI sync is still running games=%s",
            sync_kind,
            ",".join(target_game_ids),
        )
        return False

    def worker() -> None:
        _realtime_oci_sync_worker(sync_kind, oci_url, target_game_ids, method_name)

    thread = Thread(
        target=worker,
        name=f"realtime-oci-{sync_kind}-sync",
        daemon=True,
    )
    try:
        thread.start()
    except RuntimeError:
        REALTIME_OCI_SYNC_LOCK.release()
        return False
    return True


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
    """Daily job: Run unified daily update entrypoint.

    Runs at 03:00 KST daily to collect previous KST day's schedule+details.
    Uses exponential backoff retry on failures (3 attempts max).
    """
    with _scheduler_job_lock(DAILY_LOCK):
        logger.info("=== Starting Daily Games Crawl ===")

        try:
            target_date = _previous_day_kst()
            logger.info("Running run_daily_update for target_date=%s", target_date)

            args = ["--date", target_date, "--seed-tomorrow-preview"]
            if bool(os.getenv("OCI_DB_URL")):
                logger.info("Enabling OCI sync for daily update")
                args.append("--sync")

            if _env_enabled("DAILY_SKIP_SEASON_STATS", "0"):
                logger.info("Skipping season stat crawl for daily update (DAILY_SKIP_SEASON_STATS=1)")
                args.append("--skip-season-stats")

            if _env_enabled("DAILY_SKIP_OCI_SUPPORTING_SYNC", "0"):
                logger.info("Skipping non-P0 OCI supporting sync for daily update (DAILY_SKIP_OCI_SUPPORTING_SYNC=1)")
                args.append("--skip-oci-supporting-sync")

            # Auto-remediation: fix stats discrepancies detected by the audit step
            if _env_enabled("DAILY_AUTO_REMEDIATION", "1"):
                logger.info("Auto-remediation enabled for daily update (DAILY_AUTO_REMEDIATION=1)")
                args.append("--fix")
            else:
                logger.info("Auto-remediation disabled (DAILY_AUTO_REMEDIATION=0)")

            update_result = run_daily_update_main(args)

            logger.info("=== Daily Games Crawl Completed Successfully ===")
            alert_success("crawl_daily_games", format_stability_alert_summary(update_result))

            # Check for partial failures (games that failed detail collection)
            if isinstance(update_result, dict):
                stability = update_result.get("stability", {})
                detail = stability.get("detail", {}) if isinstance(stability, dict) else {}
                detail_recovery = stability.get("detail_recovery", {}) if isinstance(stability, dict) else {}
                detail_counts = detail.get("failure_counts", {}) if isinstance(detail, dict) else {}
                repeated_failures = (
                    detail_recovery.get("escalation_game_ids") if isinstance(detail_recovery, dict) else []
                )
                total_failures = sum(detail_counts.values()) if isinstance(detail_counts, dict) else 0
                if repeated_failures or total_failures > 0:
                    alert_warning("crawl_daily_games", format_stability_alert_summary(update_result))
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Daily games crawl attempt failed")
            raise


def _find_detail_gaps(session, start_date: date) -> list[str]:
    """Find dates with COMPLETED/DRAW games missing batting or pitching stats."""
    from sqlalchemy import text as sa_text

    rows = (
        session.execute(
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


def _to_compact_date(value: str) -> str:
    if isinstance(value, str) and len(value) == 8 and value.isdigit():
        return value
    msg = f"invalid compact date value: {value!r}"
    raise ValueError(msg)


def _from_compact_date(value: str) -> date:
    return datetime.strptime(_to_compact_date(value), "%Y%m%d").date()


def _find_pbp_gaps(session, start_date: date) -> list[str]:
    """Find dates with COMPLETED/DRAW games missing game_play_by_play."""
    from sqlalchemy import text as sa_text

    rows = (
        session.execute(
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


def _find_preview_gaps(session, start_date: date) -> list[str]:
    """Find dates with SCHEDULED games missing pregame preview data."""
    from sqlalchemy import text as sa_text

    rows = (
        session.execute(
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


def _find_player_profile_gaps(session) -> list[int]:
    """Find player IDs missing photo_url (excludes pseudo/not-found status)."""
    from sqlalchemy import or_

    from src.models.player import PlayerBasic

    rows = (
        session.query(PlayerBasic.player_id)
        .filter(
            PlayerBasic.photo_url.is_(None),
            PlayerBasic.player_id >= 10000,
            or_(PlayerBasic.status.is_(None), ~PlayerBasic.status.in_(["NOT_FOUND", "PSEUDO"])),
        )
        .all()
    )
    return [row.player_id for row in rows]


def _compact_date(d) -> str:
    if hasattr(d, "strftime"):
        return d.strftime("%Y%m%d")
    return str(d).replace("-", "")


def _backfill_phase_detail(start, backfilled: list[str]) -> list[str]:
    with SessionLocal() as session:
        detail_dates = _find_detail_gaps(session, start)
    for date_compact in detail_dates:
        logger.warning("Phase 1 — Detail backfill needed for %s", date_compact)
        try:
            run_daily_update_main(["--date", date_compact])
            with SessionLocal() as verify_session:
                if date_compact not in _find_detail_gaps(verify_session, _from_compact_date(date_compact)):
                    backfilled.append(f"detail:{date_compact}")
                    logger.info("Phase 1 — Detail backfill completed for %s", date_compact)
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Phase 1 — Detail backfill failed for %s", date_compact)
    return detail_dates


def _backfill_phase_pbp(start, detail_dates: list[str], backfilled: list[str]) -> None:
    with SessionLocal() as session:
        all_dates = sorted(set(detail_dates + _find_pbp_gaps(session, start)))
    for date_compact in all_dates:
        with SessionLocal() as verify_session:
            verify_date = _from_compact_date(date_compact)
            detail_still_missing = date_compact in _find_detail_gaps(verify_session, verify_date)
            pbp_still_missing = date_compact in _find_pbp_gaps(verify_session, verify_date)
        if not (detail_still_missing or pbp_still_missing):
            continue
        logger.warning("Phase 2 — PBP/relay backfill needed for %s", date_compact)
        try:
            run_daily_update_main(["--date", date_compact])
            backfilled.append(f"pbp:{date_compact}")
            logger.info("Phase 2 — PBP/relay backfill completed for %s", date_compact)
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Phase 2 — PBP/relay backfill failed for %s", date_compact)


def _backfill_phase_preview(start, backfilled: list[str]) -> None:
    with SessionLocal() as session:
        preview_dates = _find_preview_gaps(session, start)
    for date_compact in preview_dates:
        logger.warning("Phase 3 — Preview backfill needed for %s", date_compact)
        try:
            asyncio.run(run_preview_batch(date_compact))
            backfilled.append(f"preview:{date_compact}")
            logger.info("Phase 3 — Preview backfill completed for %s", date_compact)
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Phase 3 — Preview backfill failed for %s", date_compact)


def _backfill_phase_profiles(backfilled: list[str]) -> None:
    with SessionLocal() as session:
        profile_gap_ids = _find_player_profile_gaps(session)
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
        oci_url = os.getenv("OCI_DB_URL")
        if oci_url:
            with SessionLocal() as sync_session:
                syncer = OCISync(oci_url, sync_session)
                try:
                    synced_basic = syncer.sync_player_basic_by_ids(batch)
                    synced_players = syncer.sync_players()
                    logger.info(
                        "Phase 4 — Profile OCI sync completed (player_basic=%s, players=%s)",
                        synced_basic,
                        synced_players,
                    )
                finally:
                    syncer.close()
    except SCHEDULER_JOB_EXCEPTIONS:
        logger.exception("Phase 4 — Profile backfill failed")


def backfill_missed_daily_crawls(lookback_days: int = 14) -> list[str]:
    start = datetime.now(KST).date() - timedelta(days=lookback_days)
    backfilled: list[str] = []
    detail_dates = _backfill_phase_detail(start, backfilled)
    _backfill_phase_pbp(start, detail_dates, backfilled)
    _backfill_phase_preview(start, backfilled)
    _backfill_phase_profiles(backfilled)
    if not backfilled:
        logger.info("No backfill needed — all data types are current.")
    else:
        logger.info("Backfill summary: %s action(s) — %s", len(backfilled), backfilled)
    return backfilled


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=30, max=120),
    retry_error_callback=alert_failure,
)
def _process_pregame_date(
    target_date: str,
    refresh_only_missing: bool,
    alert_on_missing: bool,
    sync_to_oci: bool,
    pregame_sync_game_ids: list[str],
) -> int:
    scheduled_count, starters_missing, preview_missing = _pregame_refresh_summary(target_date)
    if scheduled_count == 0:
        return 0
    if refresh_only_missing and starters_missing == 0 and preview_missing == 0:
        MISSING_PREGAME_ALERTED_DATES.discard(target_date)
        logger.info("Skipping pregame refresh for target_date=%s (all present)", target_date)
        return 0
    saved_ids = asyncio.run(run_preview_batch(target_date, sync_to_oci=False))
    if saved_ids and sync_to_oci:
        pregame_sync_game_ids.extend(saved_ids)
        logger.info("Pregame OCI sync queued for target_date=%s games=%s", target_date, len(saved_ids))
    post = _pregame_refresh_summary(target_date)
    if post[0] and (post[1] > 0 or post[2] > 0):
        if alert_on_missing and target_date not in MISSING_PREGAME_ALERTED_DATES:
            try:
                SlackWebhookClient.send_alert(
                    f"Pregame missing remains for {target_date}: starters_missing={post[1]}, preview_missing={post[2]}"
                )
            except ALERT_EXCEPTIONS:
                logger.exception("Failed to send pregame missing alert for target_date=%s", target_date)
            MISSING_PREGAME_ALERTED_DATES.add(target_date)
    else:
        MISSING_PREGAME_ALERTED_DATES.discard(target_date)
    if scheduled_count and not saved_ids:
        logger.warning(
            "Pregame refresh saved no preview rows for %s: scheduled=%d, saved=0.", target_date, scheduled_count
        )
    return len(saved_ids or [])


def crawl_pregame_refresh():
    pregame_sync_game_ids: list[str] = []
    if not LIVE_LOCK.acquire(blocking=False):
        logger.info("Skipping pregame refresh because LIVE_LOCK is already held")
        return
    try:
        refresh_only_missing = _env_enabled("PREGAME_REFRESH_ONLY_MISSING", "1")
        alert_on_missing = _env_enabled("PREGAME_MISSING_ALERT", "0")
        sync_to_oci = _pregame_sync_to_oci_enabled()
        for target_date in _pregame_target_dates():
            _process_pregame_date(
                target_date, refresh_only_missing, alert_on_missing, sync_to_oci, pregame_sync_game_ids
            )
        alert_success("crawl_pregame_refresh")
        global LAST_PREGAME_RUN_TIME
        LAST_PREGAME_RUN_TIME = datetime.now(KST)
    finally:
        LIVE_LOCK.release()
    if sync_to_oci and pregame_sync_game_ids:
        _submit_realtime_oci_sync("pregame", pregame_sync_game_ids)


def _parse_start_time(
    start_time_raw,
    now: datetime,
    earliest_start_time: datetime | None,
) -> datetime | None:
    try:
        if isinstance(start_time_raw, str):
            parts = list(map(int, start_time_raw.split(":")[:2]))
            start_time = now.replace(hour=parts[0], minute=parts[1], second=0, microsecond=0)
        else:
            start_time = now.replace(
                hour=start_time_raw.hour,
                minute=start_time_raw.minute,
                second=0,
                microsecond=0,
            )
        if start_time < now:
            start_time += timedelta(days=1)
        if earliest_start_time is None or start_time < earliest_start_time:
            return start_time
    except (ValueError, TypeError, IndexError):
        logger.warning("[LiveInterval] Failed to parse start_time: %s", start_time_raw)
    return earliest_start_time


def _parse_update_time(
    updated_at_raw,
    latest_update_time: datetime | None,
) -> datetime | None:
    try:
        if isinstance(updated_at_raw, str):
            updated_dt = datetime.fromisoformat(updated_at_raw)
        else:
            updated_dt = updated_at_raw
        if updated_dt.tzinfo is None:
            updated_kst = updated_dt.replace(tzinfo=KST)
        else:
            updated_kst = updated_dt.astimezone(KST)
        if latest_update_time is None or updated_kst > latest_update_time:
            return updated_kst
    except (ValueError, TypeError, OSError):
        logger.debug("Skipping unparsable live game update timestamp", exc_info=True)
    return latest_update_time


def _categorize_game_row(
    row,
    terminal_statuses: set[str],
    terminal_lifecycles: set[str],
    active_statuses: set[str],
    active_lifecycles: set[str],
) -> tuple[bool, bool, bool]:
    status = str(row[0] or "").upper()
    lifecycle = str(row[1] or "").lower()
    not_terminal = status not in terminal_statuses and lifecycle not in terminal_lifecycles
    is_active = status in active_statuses or lifecycle in active_lifecycles
    is_suspended = is_active and (status in {"DELAYED", "SUSPENDED"} or lifecycle == "suspended")
    return not_terminal, is_active, is_suspended


def _analyze_game_rows(rows, now: datetime) -> tuple[bool, bool, bool, datetime | None, datetime | None]:
    terminal_statuses = {"COMPLETED", "CANCELLED", "POSTPONED", "DRAW"}
    terminal_lifecycles = {"cancelled", "final", "result_pending_stabilization"}
    active_statuses = {"LIVE", "DELAYED", "SUSPENDED", "RUNNING"}
    active_lifecycles = {"running", "delayed", "suspended"}
    has_active = False
    has_suspended = False
    all_terminal = True
    earliest_start_time: datetime | None = None
    latest_update_time: datetime | None = None
    for row in rows:
        not_terminal, is_active, is_suspended = _categorize_game_row(
            row, terminal_statuses, terminal_lifecycles, active_statuses, active_lifecycles
        )
        if not_terminal:
            all_terminal = False
        if is_active:
            has_active = True
        if is_suspended:
            has_suspended = True
        if str(row[0] or "").upper() == "SCHEDULED" and row[2]:
            earliest_start_time = _parse_start_time(row[2], now, earliest_start_time)
        if row[3]:
            latest_update_time = _parse_update_time(row[3], latest_update_time)
    return has_active, has_suspended, all_terminal, earliest_start_time, latest_update_time


def _interval_from_analysis(
    now: datetime,
    has_active: bool,
    has_suspended: bool,
    all_terminal: bool,
    earliest_start_time: datetime | None,
    latest_update_time: datetime | None,
) -> int:
    if has_active:
        return 60 if has_suspended else 10
    if all_terminal:
        if latest_update_time:
            elapsed = (now - latest_update_time).total_seconds()
            if 0 <= elapsed < 600:
                return 60
        return 1800
    if earliest_start_time:
        time_to_start = (earliest_start_time - now).total_seconds()
        if 0 <= time_to_start <= 900 or time_to_start < 0:
            return 30
    return 120


def _get_live_poll_interval_seconds() -> int:
    now = datetime.now(KST)
    try:
        with SessionLocal() as session:
            rows = session.execute(
                text(
                    "SELECT g.game_status, g.game_lifecycle_state, m.start_time, g.updated_at FROM game g LEFT JOIN game_metadata m ON g.game_id = m.game_id WHERE g.game_date = :today"
                ),
                {"today": now.strftime("%Y-%m-%d")},
            ).all()
    except SCHEDULER_JOB_EXCEPTIONS:
        logger.exception("[LiveInterval] Failed to query game states; defaulting to 120s")
        return 120
    if not rows:
        return 1800
    has_active, has_suspended, all_terminal, earliest_start_time, latest_update_time = _analyze_game_rows(rows, now)
    return _interval_from_analysis(
        now, has_active, has_suspended, all_terminal, earliest_start_time, latest_update_time
    )


def crawl_live_refresh():
    global LAST_LIVE_RUN_TIME, LAST_LIVE_POLL_INTERVAL

    if _should_skip_live_for_pregame():
        logger.info("Skipping live refresh because pregame refresh is due soon")
        return

    now = datetime.now(KST)
    interval = _get_live_poll_interval_seconds()

    if interval != LAST_LIVE_POLL_INTERVAL:
        logger.info(
            "[LiveInterval] Polling interval changed: %s -> %ds",
            f"{LAST_LIVE_POLL_INTERVAL}s" if LAST_LIVE_POLL_INTERVAL else "None",
            interval,
        )
        LAST_LIVE_POLL_INTERVAL = interval

    if LAST_LIVE_RUN_TIME is not None:
        elapsed = (now - LAST_LIVE_RUN_TIME).total_seconds()
        if elapsed < interval:
            # Fast exit without acquiring LIVE_LOCK
            return

    live_sync_game_ids: list[str] = []
    if not LIVE_LOCK.acquire(blocking=False):
        logger.info("Skipping live refresh because LIVE_LOCK is already held")
        return

    LAST_LIVE_RUN_TIME = now
    try:
        logger.info("Running live refresh cycle")
        with _sqlite_writer_lock():
            result = asyncio.run(
                run_live_crawler_cycle(
                    sync_to_oci=False,
                    max_active_games=_live_refresh_max_games_per_cycle(),
                    detail_snapshot_background=True,
                ),
            )

        if isinstance(result, dict):
            live_sync_game_ids.extend(str(game_id) for game_id in result.get("game_ids_playing") or [] if game_id)
            failed_ids = result.get("oci_sync_failed_game_ids") or []
            failure_count = int(result.get("oci_sync_failure_count") or len(failed_ids) or 0)
            if failure_count:
                logger.warning(
                    "Live refresh completed with OCI partial failures phase=sync_specific_game failed=%d game_ids=%s",
                    failure_count,
                    ",".join(str(game_id) for game_id in failed_ids),
                )
    except SCHEDULER_JOB_EXCEPTIONS:
        LAST_LIVE_RUN_TIME = None
        raise
    finally:
        LIVE_LOCK.release()

    if os.getenv("OCI_DB_URL") and live_sync_game_ids:
        _submit_realtime_oci_sync("live", live_sync_game_ids)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=120, max=600),
    retry_error_callback=alert_failure,
)
def crawl_retired_players_job(limit: int | None = None):
    """Monthly job: Crawl retired/inactive player statistics.

    Runs on the 1st of every month at 02:00 KST.
    Uses exponential backoff retry on failures (3 attempts max).
    """
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
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=1, min=60, max=300),
    retry_error_callback=alert_failure,
)
def crawl_p1p2_data_job():
    """P1/P2 Crawlers: seat sections, parking, stadium food.
    Runs daily at 06:30 KST (after Phase 1 extra crawlers).
    """
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
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("P1/P2 data crawlers failed")


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
        "--run-retire-once",
        action="store_true",
        help="Run only one retired player crawl job immediately and exit.",
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


def sync_from_oci_job():
    """Sync job: Hydrate local DB from OCI after GitHub Actions run window.
    Runs daily at 05:00 KST.
    """
    with _scheduler_job_lock(MAINTENANCE_LOCK):
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
def _crawl_team_info_history():
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


def weekly_sla_report_job():
    """Weekly SLA report job: computes past 7 days SLA and alerts.
    Runs weekly on Monday at 06:00 KST.
    """
    from src.monitoring.sla_tracker import SlaTracker

    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Weekly SLA Report Generation ===")
        with SessionLocal() as session:
            tracker = SlaTracker(session)
            tracker.send_weekly_sla_report()
        logger.info("=== Weekly SLA Report Generation Completed ===")


def crawl_phase1_extra_job():
    """Phase 1: Supplementary crawlers (broadcast, MVP, injury, foreign players, manager changes).
    Runs daily at 06:00 KST (after daily game crawl and standings compute).
    """
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Phase 1 Extra Crawlers ===")
        try:
            from src.cli.crawl_phase1_extra import run_all_crawlers

            asyncio.run(run_all_crawlers(save=True))
            logger.info("=== Phase 1 Extra Crawlers Completed Successfully ===")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Phase 1 extra crawlers failed")


def compute_standings_job():
    """Compute daily standings with home/away splits, recent 10, weekly trends.
    Runs daily at 03:30 KST (after game crawl at 03:00).
    """
    with _scheduler_job_lock(DAILY_LOCK):
        logger.info("=== Starting Standings Computation ===")
        try:
            from src.cli.calculate_standings import main as standings_main

            current_year = datetime.now(KST).year
            standings_main(["--year", str(current_year)])
            logger.info("=== Standings Computation Completed ===")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Standings computation failed")


def aggregate_team_defense_job():
    """Aggregate team-level fielding and baserunning stats.
    Runs daily at 03:45 KST (after standings).
    """
    with _scheduler_job_lock(MAINTENANCE_LOCK):
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
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Team defense aggregation failed")


def compute_rankings_job():
    """Compute sabermetric rankings (wOBA, wRC+, WAR, OPS+).
    Runs daily at 04:00 KST.
    """
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Rankings Computation ===")
        try:
            from src.cli.calculate_rankings import rebuild_rankings

            current_year = datetime.now(KST).year
            rebuild_rankings(current_year)
            logger.info("=== Rankings Computation Completed ===")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Rankings computation failed")


def heal_unverified_pbp_job():
    """PBP Healer: scan for unverified PBP games and re-crawl from KBO official site.
    Runs daily at 04:30 KST (after rankings, before OCI sync).
    Uses MAINTENANCE_LOCK to avoid overlapping with other heavy jobs.
    """
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


def compute_park_factor_job():
    """Compute park factor for all stadiums.
    Runs weekly Sunday at 05:30 KST.
    """
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Park Factor Computation ===")
        try:
            from src.aggregators.park_factor_calculator import ParkFactorCalculator
            from src.db.engine import SessionLocal

            current_year = datetime.now(KST).year
            with SessionLocal() as session:
                calc = ParkFactorCalculator(session)
                results = calc.calculate(current_year)
                logger.info(f"Park Factor computed for {len(results)} stadiums")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Park Factor computation failed")


# ─────────────────────────────────────────────────────────────────────────────
# Tier 2 Maintenance Backfill Jobs
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# Tier 3 — Unified Gap Report
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# Stadium Real-Time Data Jobs (이동 시간 · �잡도 · 운영 공지)
# ─────────────────────────────────────────────────────────────────────────────


def crawl_transit_time_job():
    """Transit time measurement job: measure travel times from nearby stations to
    Jamsil Stadium every 15 minutes on game days (D-2h ~ D+1h window).
    Uses LIVE_LOCK to prevent conflicts with live crawlers.
    """
    if not LIVE_LOCK.acquire(blocking=False):
        logger.info("Skipping transit time because LIVE_LOCK is already held")
        return
    try:
        logger.info("[Transit] Starting transit time measurement")
        from src.crawlers.transit_time_crawler import TransitTimeCrawler

        with _sqlite_writer_lock():
            asyncio.run(TransitTimeCrawler().run(save=True))
        logger.info("[Transit] Transit time measurement completed")
    except SCHEDULER_JOB_EXCEPTIONS:
        logger.exception("Transit time job failed")
    finally:
        LIVE_LOCK.release()


def crawl_congestion_job():
    """Congestion data job: collect real-time congestion for Jamsil area
    every 5 minutes on game days (D-3h ~ D+2h window).
    Uses LIVE_LOCK to stay in the same priority tier as live crawlers.
    """
    if not LIVE_LOCK.acquire(blocking=False):
        logger.info("Skipping congestion because LIVE_LOCK is already held")
        return
    try:
        logger.info("[Congestion] Starting congestion data collection")
        from src.crawlers.congestion_crawler import CongestionCrawler

        with _sqlite_writer_lock():
            asyncio.run(CongestionCrawler().run(save=True))
        logger.info("[Congestion] Congestion data collection completed")
    except SCHEDULER_JOB_EXCEPTIONS:
        logger.exception("Congestion job failed")
    finally:
        LIVE_LOCK.release()


def crawl_operation_notices_job():
    """Operation notice job: crawl LG Twins and Doosan Bears official notices
    once daily at 09:00 KST (before gates open for evening games).
    Also triggered at 11:30 KST for day-of-game notices.
    Uses DAILY_LOCK.
    """
    with _scheduler_job_lock(DAILY_LOCK):
        logger.info("[Notice] Starting operation notice crawl")
        try:
            from src.cli.crawl_operation_notices import main as notices_main

            notices_main(["--save", "--incremental"])
            logger.info("[Notice] Operation notice crawl completed")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Operation notices job failed")


def crawl_operation_notices_naver_job():
    """Naver Search-based operation notice job.
    Queries Naver News API for real-time KBO/stadium notices.
    Runs at 09:30 and 13:00 KST. Uses DAILY_LOCK.
    """
    with _scheduler_job_lock(DAILY_LOCK):
        logger.info("[NaverNotice] Starting Naver search notice crawl")
        try:
            from src.cli.crawl_operation_notices import main as notices_main

            notices_main(["--source", "naver", "--days", "1", "--save"])
            logger.info("[NaverNotice] Naver notice crawl completed")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Naver operation notices job failed")


def crawl_fan_culture_job():
    """Fan culture data job: crawl cheer songs, chants, and rivalries from
    Namuwiki. Runs weekly on Saturday 04:00 KST. Uses MAINTENANCE_LOCK.
    """
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("[FanCulture] Starting fan culture data crawl")
        try:
            from src.crawlers.fan_culture_crawler import FanCultureCrawler

            asyncio.run(FanCultureCrawler().run(save=True))
            logger.info("[FanCulture] Fan culture data crawl completed")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Fan culture job failed")


def crawl_p0_non_game_job():
    """P0 non-game job: crawl team events, roster transactions, and ticket info.
    Runs daily before the freshness monitor. Uses MAINTENANCE_LOCK.
    """
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("[P0NonGame] Starting P0 non-game crawl")
        try:
            from src.cli.crawl_p0_data import main as crawl_p0_data_main

            current_year = datetime.now(KST).year
            result = crawl_p0_data_main(["--type", "all", "--save", "--days", "3", "--season", str(current_year)])
            logger.info("[P0NonGame] P0 non-game crawl completed: %s", result)
            alert_success("crawl_p0_non_game_job", str(result))
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("P0 non-game crawl failed")


job_start_times: dict[str, float] = {}


def job_lifecycle_listener(event: object) -> None:
    """Listener for APScheduler job lifecycle events to collect metrics and capture errors."""
    event_code = getattr(event, "code", None)
    job_id = getattr(event, "job_id", "unknown")

    if event_code == EVENT_JOB_SUBMITTED:
        job_start_times[job_id] = time.time()

    elif event_code == EVENT_JOB_EXECUTED:
        start_time = job_start_times.pop(job_id, None)
        duration = time.time() - start_time if start_time else 0.0

        KBO_SCHEDULER_JOB_TOTAL.labels(job_id=job_id, status="success").inc()
        KBO_SCHEDULER_JOB_DURATION_SECONDS.labels(job_id=job_id).observe(duration)

    elif event_code == EVENT_JOB_ERROR:
        start_time = job_start_times.pop(job_id, None)
        duration = time.time() - start_time if start_time else 0.0

        KBO_SCHEDULER_JOB_TOTAL.labels(job_id=job_id, status="failure").inc()
        KBO_SCHEDULER_JOB_DURATION_SECONDS.labels(job_id=job_id).observe(duration)

        exc = getattr(event, "exception", None)
        if exc:
            import traceback

            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            logger.error("Job %s failed: %s", job_id, exc)

            sentry_sdk.capture_exception(exc)

            try:
                SlackWebhookClient.send_error_alert(f"🚨 <b>Scheduler Job Failed: {job_id}</b>\nError: {exc}\n\n{tb}")
            except ALERT_EXCEPTIONS:
                logger.exception("Failed to send Slack alert for failed job %s", job_id)


def _query_max_updated_at(url: str | None) -> datetime | None:
    if not url:
        return None
    try:
        from sqlalchemy import create_engine

        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            row = conn.execute(text("SELECT MAX(updated_at) FROM game")).scalar()
            return datetime.fromisoformat(row) if isinstance(row, str) else row
    except Exception as e:
        logger.exception("Failed to query OCI max updated_at")
        sentry_sdk.capture_exception(e)
        KBO_OCI_SYNC_ERRORS_TOTAL.inc()
        return None


def update_oci_sync_lag_metrics() -> None:
    try:
        with SessionLocal() as sqlite_session:
            row = sqlite_session.execute(text("SELECT MAX(updated_at) FROM game")).scalar()
            sqlite_max = datetime.fromisoformat(row) if isinstance(row, str) else row
    except Exception as e:
        logger.exception("Failed to query SQLite max updated_at")
        sentry_sdk.capture_exception(e)
        return
    if sqlite_max is None:
        return
    oci_max = _query_max_updated_at(os.getenv("OCI_DB_URL"))
    if oci_max is None:
        return
    sqlite_max = sqlite_max if sqlite_max.tzinfo else sqlite_max.replace(tzinfo=KST)
    oci_max = oci_max if oci_max.tzinfo else oci_max.replace(tzinfo=KST)
    lag_seconds = max(0.0, (sqlite_max - oci_max).total_seconds())
    KBO_OCI_SYNC_LAG_SECONDS.set(lag_seconds)
    logger.info("Updated OCI sync lag metric: %.1f seconds", lag_seconds)


_SCHEDULER_REF: BlockingScheduler | None = None


def _shutdown_handler(signum: int, frame: object) -> None:
    logger.info("Received signal %s. Stopping scheduler gracefully...", signum)
    if _SCHEDULER_REF is not None:
        try:
            _SCHEDULER_REF.shutdown(wait=False)
        except Exception as e:
            logger.warning("Error during scheduler shutdown: %s", e)
    for lock in [LIVE_LOCK, DAILY_LOCK, MAINTENANCE_LOCK, REALTIME_OCI_SYNC_LOCK, SQLITE_WRITE_LOCK]:
        try:
            lock.release()
        except Exception as e:
            logger.warning("Error releasing lock %s: %s", lock.name, e)
    sys.exit(0)


def _dispatch_single_run(args) -> bool:
    if args.run_once:
        crawl_daily_games()
        return True
    if args.run_pregame_once:
        crawl_pregame_refresh()
        return True
    if args.run_retire_once:
        crawl_retired_players_job(limit=args.limit)
        return True
    return False


def _start_scheduler(args):
    global _SCHEDULER_REF
    scheduler = BlockingScheduler(timezone="Asia/Seoul")
    _SCHEDULER_REF = scheduler
    scheduler.add_listener(job_lifecycle_listener, EVENT_JOB_SUBMITTED | EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    jobs = [
        (
            crawl_daily_games,
            CronTrigger(hour=3, minute=0),
            "crawl_daily_games",
            "Daily Games Crawl (Schedule + Details)",
            7200,
        ),
        (crawl_phase1_extra_job, CronTrigger(hour=6, minute=0), "crawl_phase1_extra", "Phase 1 Extra Crawlers", 7200),
        (
            crawl_p1p2_data_job,
            CronTrigger(hour=6, minute=30),
            "crawl_p1p2_data",
            "P1/P2 Seat/Parking/Food Crawlers",
            7200,
        ),
        (
            crawl_p0_non_game_job,
            CronTrigger(hour=6, minute=20),
            "crawl_p0_non_game",
            "P0 Non-Game Data Crawl (Events/Roster/Tickets)",
            3600,
        ),
        (
            weekly_sla_report_job,
            CronTrigger(day_of_week="mon", hour=6, minute=0),
            "weekly_sla_report",
            "Weekly SLA Report Generation",
            3600,
        ),
        (
            compute_park_factor_job,
            CronTrigger(day_of_week="sun", hour=5, minute=30),
            "compute_park_factor",
            "Weekly Park Factor Computation",
            7200,
        ),
        (
            crawl_retired_players_job,
            CronTrigger(day=1, hour=2, minute=0),
            "crawl_retired_players",
            "Monthly Retired Player Crawl",
            3600,
        ),
        (
            crawl_monthly_unified_audit_job,
            CronTrigger(day=1, hour=3, minute=0),
            "crawl_monthly_unified_audit",
            "Monthly Unified Audit (PA + Team Stats)",
            3600,
        ),
    ]
    for fn, trigger, job_id, name, grace in jobs:
        scheduler.add_job(fn, trigger=trigger, id=job_id, name=name, misfire_grace_time=grace, max_instances=1)
        logger.info("Registered job: %s", job_id)

    scheduler.add_job(
        crawl_pregame_refresh,
        trigger=CronTrigger(hour="10-23", minute="*/15"),
        id="crawl_pregame_refresh",
        name="Pregame Refresh",
        misfire_grace_time=900,
        max_instances=1,
    )
    scheduler.add_job(
        crawl_live_refresh,
        trigger=CronTrigger(hour="12-22", second="*/10"),
        id="crawl_live_refresh_day",
        name="Live Refresh Day Window",
        misfire_grace_time=5,
        max_instances=1,
    )
    scheduler.add_job(
        crawl_live_refresh,
        trigger=CronTrigger(hour=23, minute="0-30", second="*/10"),
        id="crawl_live_refresh_night",
        name="Live Refresh Night Window",
        misfire_grace_time=5,
        max_instances=1,
    )
    logger.info("Registered job: crawl_live_refresh (Every 10s, 12:00-23:30 KST)")

    tier2_jobs = [
        (compute_standings_job, CronTrigger(hour=3, minute=30), "compute_standings", 7200),
        (aggregate_team_defense_job, CronTrigger(hour=3, minute=45), "aggregate_team_defense", 7200),
        (compute_rankings_job, CronTrigger(hour=4, minute=0), "compute_rankings", 7200),
        (heal_unverified_pbp_job, CronTrigger(hour=4, minute=30), "heal_pbp", 7200),
    ]
    for fn, trigger, job_id, grace in tier2_jobs:
        scheduler.add_job(fn, trigger=trigger, id=job_id, name=job_id, misfire_grace_time=grace, max_instances=1)
        logger.info("Registered job: %s (Daily)", job_id)

    scheduler.add_job(
        crawl_transit_time_job,
        trigger=CronTrigger(hour="10-23", minute="*/15"),
        id="crawl_transit_time",
        name="Stadium Transit Time Measurement (JAMSIL)",
        misfire_grace_time=600,
        max_instances=1,
    )
    scheduler.add_job(
        crawl_congestion_job,
        trigger=CronTrigger(hour="10-23", minute="*/5"),
        id="crawl_congestion",
        name="Stadium Congestion Data (JAMSIL)",
        misfire_grace_time=300,
        max_instances=1,
    )
    scheduler.add_job(
        crawl_operation_notices_job,
        trigger=CronTrigger(hour=9, minute=0),
        id="crawl_operation_notices_morning",
        name="Operation Notices — Morning",
        misfire_grace_time=3600,
        max_instances=1,
    )
    scheduler.add_job(
        crawl_operation_notices_job,
        trigger=CronTrigger(hour=11, minute=30),
        id="crawl_operation_notices_daygame",
        name="Operation Notices — Day-of-Game",
        misfire_grace_time=3600,
        max_instances=1,
    )
    scheduler.add_job(
        crawl_operation_notices_naver_job,
        trigger=CronTrigger(hour=9, minute=30),
        id="crawl_naver_notices_morning",
        name="Naver Notice — Morning",
        misfire_grace_time=3600,
        max_instances=1,
    )
    scheduler.add_job(
        crawl_operation_notices_naver_job,
        trigger=CronTrigger(hour=13, minute=0),
        id="crawl_naver_notices_afternoon",
        name="Naver Notice — Afternoon",
        misfire_grace_time=3600,
        max_instances=1,
    )
    scheduler.add_job(
        crawl_fan_culture_job,
        trigger=CronTrigger(day_of_week="sat", hour=4, minute=0),
        id="crawl_fan_culture",
        name="Fan Culture Data Crawl",
        misfire_grace_time=7200,
        max_instances=1,
    )
    scheduler.add_job(
        _crawl_team_info_history,
        trigger=CronTrigger(day_of_week="sun", hour=6, minute=0),
        id="crawl_team_info_history",
        name="Team Info/History Refresh",
        misfire_grace_time=7200,
        max_instances=1,
    )
    scheduler.add_job(
        update_oci_sync_lag_metrics,
        trigger=CronTrigger(minute="*/5"),
        id="update_oci_sync_lag",
        name="Update OCI Sync Lag Metrics",
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
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "=" * 60,
    )
    job_names = [
        "Daily Games Crawl (03:00)",
        "Phase 1 Extra (06:00)",
        "P0 Non-Game (06:20)",
        "P1/P2 Seat/Parking/Food (06:30)",
        "Pregame Refresh (Every 15m, 10:00-23:45)",
        "Live Refresh (Every 10s, 12:00-23:30)",
        "Park Factor (Sunday 05:30)",
        "Retired Players (Month 1st 02:00)",
        "SLA Report (Monday 06:00)",
        "Transit Time (Every 15m, 10:00-23:45)",
        "Congestion (Every 5m, 10:00-23:55)",
        "Operation Notices (09:00 + 11:30)",
        "Naver Notices (09:30 + 13:00)",
        "Fan Culture (Saturday 04:00)",
        "Standings (03:30)",
        "Team Defense (03:45)",
        "Rankings (04:00)",
        "PBP Healer (04:30)",
        "OCI Hydration (05:00)",
        "Team Info/History (Sunday 06:00)",
    ]
    logger.info("\nScheduled Jobs:\n%s\n", "\n".join(f"  {i + 1}. {name}" for i, name in enumerate(job_names)))
    logger.info("%s\n", "=" * 60)

    signal.signal(signal.SIGTERM, _shutdown_handler)
    signal.signal(signal.SIGINT, _shutdown_handler)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user")


def main(argv: Sequence[str] | None = None):
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if _dispatch_single_run(args):
        return
    init_sentry()
    start_metrics_server(_env_int("PROMETHEUS_PORT", 8000))
    _start_scheduler(args)


if __name__ == "__main__":
    main()
