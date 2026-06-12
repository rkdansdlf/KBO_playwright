"""
APScheduler-based automation for KBO data collection.

Note: Daily post-processing (finalize, standings, defense, rankings, PBP healer,
batch parse, quality report, gap report, freshness monitor) and Tier 2 backfills
(SH/SF, advanced stats, player IDs, roster) are now handled by GitHub Actions
via .github/workflows/daily_kbo_sync.yml and backfill.yml (consolidated).

APScheduler focuses on real-time and local-only jobs:

Jobs:
  1. crawl_phase1_extra: Daily at 06:00 KST (broadcast, MVP, injury, etc.)
  2. crawl_p0_non_game: Daily at 06:20 KST (P0 events/roster/tickets)
  3. crawl_p1p2_data: Daily at 06:30 KST (seat + parking + food crawlers)
  4. crawl_pregame_refresh: Every 15m, 10:00-23:45 KST
  5. crawl_live_refresh: Every 2m, 12:00-23:30 KST
  6. compute_park_factor: Weekly Sunday at 05:30 KST
  7. crawl_retired_players: Monthly 1st at 02:00 KST (crawl_retire)
  8. weekly_sla_report: Weekly Monday at 06:00 KST
  9. crawl_transit_time: Every 15m, 10:00-00:00 KST on game days (LIVE_LOCK)
  10. crawl_congestion: Every 5m, 10:00-00:00 KST on game days (LIVE_LOCK)
  11. crawl_operation_notices: Daily at 09:00 KST (DAILY_LOCK)
  12. crawl_operation_notices_naver: Daily at 09:30 + 13:00 KST
  13. crawl_fan_culture: Weekly Saturday at 04:00 KST
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from threading import Lock, Thread
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
from src.cli.crawl_retire import main as crawl_retire_main
from src.cli.daily_preview_batch import run_preview_batch
from src.cli.live_crawler import run_live_crawler_cycle
from src.cli.monthly_unified_audit import crawl_monthly_unified_audit_job
from src.cli.run_daily_update import format_stability_alert_summary
from src.cli.run_daily_update import main as run_daily_update_main
from src.db.engine import SessionLocal
from src.sync.oci_sync import OCISync
from src.utils.alerting import SlackWebhookClient

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

# Granular locking to prevent long-running batch jobs from blocking real-time updates
LIVE_LOCK = Lock()  # For live refresh and pregame refresh
DAILY_LOCK = Lock()  # For daily update/finalize
MAINTENANCE_LOCK = Lock()  # For weekly futures sync and OCI hydration/reports
REALTIME_OCI_SYNC_LOCK = Lock()  # Best-effort live/pregame OCI sync, never blocks realtime jobs

MISSING_PREGAME_ALERTED_DATES: set[str] = set()
LAST_LIVE_RUN_TIME: datetime | None = None
LAST_LIVE_POLL_INTERVAL: int | None = None


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
    except Exception:
        logger.exception("Failed to send failure alert for job %s", func_name)

    # Do NOT re-raise — let retry_error_callback suppress so the scheduler survives.
    if exc:
        logger.warning(f"Job {func_name} permanently failed but scheduler continues: {error_text}")
    return None


def alert_warning(func_name: str, details: str | None = None):
    """Send a warning alert for partial failures or non-critical issues."""
    try:
        message = f"⚠️ KBO Job {func_name} has warnings."
        if details:
            message = f"{message}\n{details}"
        SlackWebhookClient.send_alert(message)
    except Exception:
        logger.exception("Failed to send warning alert for job %s", func_name)


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
        """
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


def _pregame_sync_to_oci_enabled() -> bool:
    return _env_enabled("PREGAME_SYNC_TO_OCI") and bool(os.getenv("OCI_DB_URL"))


def _submit_realtime_oci_sync(sync_kind: str, game_ids: Sequence[str]) -> bool:
    """Submit best-effort realtime OCI sync without blocking live/pregame jobs."""
    target_game_ids = sorted({str(game_id) for game_id in game_ids if game_id})
    if not target_game_ids:
        return False

    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        logger.warning("Skipping realtime OCI %s sync because OCI_DB_URL is not set", sync_kind)
        return False

    method_by_kind = {
        "pregame": "sync_pregame_game",
        "live": "sync_specific_game",
    }
    method_name = method_by_kind.get(sync_kind)
    if method_name is None:
        raise ValueError(f"Unsupported realtime OCI sync kind: {sync_kind}")

    if not REALTIME_OCI_SYNC_LOCK.acquire(blocking=False):
        logger.warning(
            "Skipping realtime OCI %s sync because a prior realtime OCI sync is still running games=%s",
            sync_kind,
            ",".join(target_game_ids),
        )
        return False

    def _worker() -> None:
        syncer = None
        succeeded = 0
        failed = 0
        try:
            logger.info(
                "Starting background realtime OCI %s sync games=%s",
                sync_kind,
                ",".join(target_game_ids),
            )
            with SessionLocal() as sync_session:
                syncer = OCISync(oci_url, sync_session)
                sync_method = getattr(syncer, method_name)
                for game_id in target_game_ids:
                    try:
                        sync_method(game_id)
                        succeeded += 1
                    except Exception:
                        failed += 1
                        logger.exception(
                            "Background realtime OCI %s sync failed game_id=%s",
                            sync_kind,
                            game_id,
                        )
        except Exception:
            failed += len(target_game_ids) - succeeded - failed
            logger.exception("Background realtime OCI %s sync setup failed", sync_kind)
        finally:
            if syncer is not None:
                try:
                    syncer.close()
                except Exception:
                    logger.exception("Failed to close background realtime OCI %s syncer", sync_kind)
            REALTIME_OCI_SYNC_LOCK.release()
            logger.info(
                "Background realtime OCI %s sync finished succeeded=%d failed=%d",
                sync_kind,
                succeeded,
                failed,
            )

    thread = Thread(
        target=_worker,
        name=f"realtime-oci-{sync_kind}-sync",
        daemon=True,
    )
    try:
        thread.start()
    except Exception:
        REALTIME_OCI_SYNC_LOCK.release()
        logger.exception("Failed to start background realtime OCI %s sync", sync_kind)
        return False

    logger.info("Queued background realtime OCI %s sync games=%s", sync_kind, ",".join(target_game_ids))
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
        except Exception as e:
            logger.error(f"Daily games crawl attempt failed: {e}")
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
    raise ValueError(f"invalid compact date value: {value!r}")


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


def backfill_missed_daily_crawls(lookback_days: int = 14) -> list[str]:
    """
    Multi-phase backfill orchestrator for the last N days:

    Phase 1 — Game detail (batting/pitching) via run_daily_update_main
    Phase 2 — PBP / relay via run_daily_update_main
    Phase 3 — Pregame previews via run_preview_batch
    Phase 4 — Player profiles via backfill_player_profiles.backfill
    """

    from src.db.engine import SessionLocal

    start = datetime.now(KST).date() - timedelta(days=lookback_days)
    backfilled: list[str] = []

    # ── Phase 1: Game detail gaps ──────────────────────────────────────────
    with SessionLocal() as session:
        detail_dates = _find_detail_gaps(session, start)

    for date_compact in detail_dates:
        logger.warning("Phase 1 — Detail backfill needed for %s", date_compact)
        try:
            run_daily_update_main(["--date", date_compact])
            with SessionLocal() as verify_session:
                verify_date = _from_compact_date(date_compact)
                if date_compact in _find_detail_gaps(verify_session, verify_date):
                    logger.warning("Phase 1 completed but %s still has detail gaps", date_compact)
                else:
                    backfilled.append(f"detail:{date_compact}")
                    logger.info("Phase 1 — Detail backfill completed for %s", date_compact)
        except Exception:
            logger.exception("Phase 1 — Detail backfill failed for %s", date_compact)

    # ── Phase 2: PBP/relay gaps (skip dates already handled in Phase 1) ────
    with SessionLocal() as session:
        all_dates = sorted(set(detail_dates + _find_pbp_gaps(session, start)))

    for date_compact in all_dates:
        with SessionLocal() as verify_session:
            verify_date = _from_compact_date(date_compact)
            detail_still_missing = date_compact in _find_detail_gaps(verify_session, verify_date)
            pbp_still_missing = date_compact in _find_pbp_gaps(verify_session, verify_date)

        if not (detail_still_missing or pbp_still_missing):
            continue

        if detail_still_missing and f"detail:{date_compact}" in backfilled:
            logger.info("Phase 1 still incomplete for %s; retrying via phase-2 workflow", date_compact)

        logger.warning("Phase 2 — PBP/relay backfill needed for %s", date_compact)
        try:
            run_daily_update_main(["--date", date_compact])
            backfilled.append(f"pbp:{date_compact}")
            logger.info("Phase 2 — PBP/relay backfill completed for %s", date_compact)
        except Exception:
            logger.exception("Phase 2 — PBP/relay backfill failed for %s", date_compact)

    # ── Phase 3: Pregame preview gaps ──────────────────────────────────────
    with SessionLocal() as session:
        preview_dates = _find_preview_gaps(session, start)

    for date_compact in preview_dates:
        logger.warning("Phase 3 — Preview backfill needed for %s", date_compact)
        try:
            asyncio.run(run_preview_batch(date_compact))
            backfilled.append(f"preview:{date_compact}")
            logger.info("Phase 3 — Preview backfill completed for %s", date_compact)
        except Exception:
            logger.exception("Phase 3 — Preview backfill failed for %s", date_compact)

    # ── Phase 4: Player profile gaps (rate-limited, max 5 per cycle) ───────
    with SessionLocal() as session:
        profile_gap_ids = _find_player_profile_gaps(session)

    if profile_gap_ids:
        batch = profile_gap_ids[:5]
        logger.warning(
            "Phase 4 — Profile backfill: %d players need profiles (processing %d)",
            len(profile_gap_ids),
            len(batch),
        )
        try:
            from scripts.backfill_player_profiles import backfill as backfill_player_profiles_fn

            awaitable = backfill_player_profiles_fn(limit=len(batch), delay=2.0, ids=batch)
            asyncio.run(awaitable)
            backfilled.append(f"profiles:{len(batch)}")
            logger.info("Phase 4 — Profile backfill completed for %d players", len(batch))
        except Exception:
            logger.exception("Phase 4 — Profile backfill failed")

    # ── Summary ────────────────────────────────────────────────────────────
    if not backfilled:
        logger.info("No backfill needed — all data types are current.")
    else:
        logger.info("Backfill summary: %d action(s) — %s", len(backfilled), backfilled)
    return backfilled


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=30, max=120),
    retry_error_callback=alert_failure,
)
def crawl_pregame_refresh():
    sync_to_oci = False
    pregame_sync_game_ids: list[str] = []

    with LIVE_LOCK:
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
            saved_ids = asyncio.run(run_preview_batch(target_date, sync_to_oci=False))
            if saved_ids and sync_to_oci:
                pregame_sync_game_ids.extend(saved_ids)
                logger.info("Pregame OCI sync queued for target_date=%s games=%s", target_date, len(saved_ids))
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
                logger.warning(
                    "Pregame refresh saved no preview rows for %s: scheduled=%d, saved=0. "
                    "This is expected if games are postponed or not yet available.",
                    target_date,
                    scheduled_count,
                )

        alert_success("crawl_pregame_refresh")

    if sync_to_oci and pregame_sync_game_ids:
        _submit_realtime_oci_sync("pregame", pregame_sync_game_ids)


def _get_live_poll_interval_seconds() -> int:
    """
    Calculate the appropriate polling interval in seconds by querying the
    local SQLite database for today's KBO games.

    Database state rules:
    1. If no games scheduled for today: return 1800 (30 minutes).
    2. If all games are terminal (COMPLETED, CANCELLED, POSTPONED, etc.):
       - If the last game updated within the last 10 minutes (cooldown): return 60.
       - Otherwise: return 1800 (30 minutes).
    3. If there are any live/active games:
       - If any game is 'LIVE' (running): return 10.
       - If any game is 'DELAYED' or 'SUSPENDED' (rain delay/stoppage): return 60.
       - Otherwise: return 10 (safe default).
    4. If there are games today but none have started yet (all are SCHEDULED):
       - Find the earliest start time.
       - If current KST time is within 15 minutes of the earliest start time: return 30.
       - Otherwise: return 120 (2 minutes).
    """
    now = datetime.now(KST)
    today_str = now.strftime("%Y-%m-%d")

    try:
        with SessionLocal() as session:
            # Query today's games and metadata from SQLite
            query = text("""
                SELECT g.game_status, g.game_id, m.start_time, g.updated_at
                FROM game g
                LEFT JOIN game_metadata m ON g.game_id = m.game_id
                WHERE g.game_date = :today
            """)
            rows = session.execute(query, {"today": today_str}).all()
    except Exception:
        logger.exception("[LiveInterval] Failed to query game states; defaulting to 120s")
        return 120

    if not rows:
        # No games scheduled today
        return 1800

    # Categorize game states
    terminal_statuses = {"COMPLETED", "CANCELLED", "POSTPONED", "DRAW"}
    active_statuses = {"LIVE", "DELAYED", "SUSPENDED", "RUNNING"}

    has_active = False
    has_suspended = False
    all_terminal = True
    earliest_start_time = None
    latest_update_time = None

    for row in rows:
        # row: (game_status, game_id, start_time, updated_at)
        status = str(row[0] or "").upper()
        start_time_raw = row[2]  # datetime.time object
        updated_at_raw = row[3]  # datetime.datetime object

        if status not in terminal_statuses:
            all_terminal = False

        if status in active_statuses:
            has_active = True
            if status in {"DELAYED", "SUSPENDED"}:
                has_suspended = True

        # Track earliest start time for non-started games
        if status == "SCHEDULED" and start_time_raw:
            try:
                if isinstance(start_time_raw, str):
                    parts = list(map(int, start_time_raw.split(":")[:2]))
                    start_time = now.replace(hour=parts[0], minute=parts[1], second=0, microsecond=0)
                else:
                    start_time = now.replace(
                        hour=start_time_raw.hour, minute=start_time_raw.minute, second=0, microsecond=0
                    )

                if start_time < now:
                    start_time += timedelta(days=1)

                if earliest_start_time is None or start_time < earliest_start_time:
                    earliest_start_time = start_time
            except Exception:  # noqa: BLE001
                logger.warning("[LiveInterval] Failed to parse start_time: %s", start_time_raw)

        # Track latest update time for terminal/finished games
        if updated_at_raw:
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
                    latest_update_time = updated_kst
            except Exception:  # noqa: BLE001
                logger.debug("Skipping unparsable live game update timestamp", exc_info=True)

    # Apply interval rules
    if has_active:
        if has_suspended:
            return 60
        return 10

    if all_terminal:
        if latest_update_time:
            elapsed_since_finish = (now - latest_update_time).total_seconds()
            if 0 <= elapsed_since_finish < 600:
                return 60
        return 1800

    if earliest_start_time:
        time_to_start = (earliest_start_time - now).total_seconds()
        if 0 <= time_to_start <= 900 or time_to_start < 0:
            return 30

    return 120


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
    with LIVE_LOCK:
        logger.info("Running live refresh cycle")
        result = asyncio.run(
            run_live_crawler_cycle(
                sync_to_oci=False,
                max_active_games=_live_refresh_max_games_per_cycle(),
            )
        )

        # Only update run time when the cycle actually executes
        LAST_LIVE_RUN_TIME = datetime.now(KST)

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

    if os.getenv("OCI_DB_URL") and live_sync_game_ids:
        _submit_realtime_oci_sync("live", live_sync_game_ids)


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
            summary = crawl_futures_main(
                [
                    "--season",
                    str(current_year),
                    "--concurrency",
                    "2",  # Low concurrency to respect rate limits
                    "--delay",
                    "2.0",  # 2-second delay between requests
                ]
            )
            if not isinstance(summary, dict):
                raise RuntimeError("Futures crawl did not return a summary")
            if not summary.get("ok", False):
                raise RuntimeError(
                    "Futures crawl failed: "
                    f"processed={summary.get('processed')} "
                    f"success_count={summary.get('success_count')} "
                    f"total_saved={summary.get('total_saved')} "
                    f"failure_counts={summary.get('failure_counts')}"
                )

            logger.info("=== Weekly Futures Profile Crawl Completed Successfully ===")
            alert_success("crawl_all_futures_profiles")

        except Exception as e:
            logger.error(f"Futures profile crawl attempt failed: {e}")
            raise


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=120, max=600),
    retry_error_callback=alert_failure,
)
def crawl_retired_players_job(limit: int | None = None):
    """
    Monthly job: Crawl retired/inactive player statistics.

    Runs on the 1st of every month at 02:00 KST.
    Uses exponential backoff retry on failures (3 attempts max).
    """
    with MAINTENANCE_LOCK:
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

        except Exception as e:
            logger.error(f"Retired player crawl attempt failed: {e}")
            raise


@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=1, min=60, max=300),
    retry_error_callback=alert_failure,
)
def crawl_p1p2_data_job():
    """
    P1/P2 Crawlers: seat sections, parking, stadium food.
    Runs daily at 06:30 KST (after Phase 1 extra crawlers).
    """
    with DAILY_LOCK:
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
        except Exception:
            logger.exception("P1/P2 data crawlers failed")


def monitor_data_freshness_job():
    """
    Data freshness monitor: check for stale DataSources and empty tables.
    Runs daily at 07:00 KST (after all crawlers).
    """
    with MAINTENANCE_LOCK:
        logger.info("=== Starting Data Freshness Monitor ===")
        try:
            from src.cli.monitor_data_freshness import run_monitor

            result = run_monitor(alert=True)
            stale = result.get("stale", [])
            table_issues = result.get("table_issues", [])
            p0_issues = result.get("p0_issues", [])
            if stale or table_issues or p0_issues:
                logger.warning(
                    "Freshness issues: %d stale sources, %d table issues, %d P0 issues",
                    len(stale),
                    len(table_issues),
                    len(p0_issues),
                )
            else:
                logger.info("All data sources and tables healthy")
        except Exception:
            logger.exception("Data freshness monitor failed")


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
        report_main(["--date", target_date, "--force-notify"])
        logger.info("=== Daily Quality Report Generation Completed ===")


def weekly_sla_report_job():
    """
    Weekly SLA report job: computes past 7 days SLA and alerts.
    Runs weekly on Monday at 06:00 KST.
    """
    from src.monitoring.sla_tracker import SlaTracker

    with MAINTENANCE_LOCK:
        logger.info("=== Starting Weekly SLA Report Generation ===")
        with SessionLocal() as session:
            tracker = SlaTracker(session)
            tracker.send_weekly_sla_report()
        logger.info("=== Weekly SLA Report Generation Completed ===")


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


@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=1, min=60, max=300),
    retry_error_callback=alert_failure,
)
def batch_parse_snapshots_job():
    """
    Batch parser: process pending RawSourceSnapshot records.
    Runs daily at 04:45 KST (after PBP healer, before OCI sync).
    """
    with MAINTENANCE_LOCK:
        logger.info("=== Starting Batch Parse Snapshots ===")
        try:
            from scripts.batch_parse_snapshots import run_batch_parse

            stats = run_batch_parse(limit=200)
            logger.info(
                "Batch parse completed: processed=%d, done=%d, failed=%d, skipped=%d",
                stats["processed"],
                stats["done"],
                stats["failed"],
                stats["skipped"],
            )
            if stats.get("failed", 0) > 0:
                logger.warning("Batch parse had %d failures", stats["failed"])
            alert_success(
                "batch_parse_snapshots_job",
                f"processed={stats['processed']}, done={stats['done']}, failed={stats['failed']}",
            )
        except Exception:
            logger.exception("Batch parse snapshots job failed")


def heal_unverified_pbp_job():
    """
    PBP Healer: scan for unverified PBP games and re-crawl from KBO official site.
    Runs daily at 04:30 KST (after rankings, before OCI sync).
    Uses MAINTENANCE_LOCK to avoid overlapping with other heavy jobs.
    """
    with MAINTENANCE_LOCK:
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
        except Exception:
            logger.exception("PBP Auto-Healer job failed")


# ─────────────────────────────────────────────────────────────────────────────
# Tier 2 Maintenance Backfill Jobs
# ─────────────────────────────────────────────────────────────────────────────


def backfill_sh_sf_job():
    """Derive missing SH/SF from PBP events for current-season games."""
    with MAINTENANCE_LOCK:
        logger.info("=== Starting SH/SF Backfill ===")
        try:
            from scripts.maintenance.backfill_sh_sf_from_pbp import (
                backfill_game,
                find_candidate_games,
            )
            from src.db.engine import SessionLocal

            year = datetime.now(KST).year
            with SessionLocal() as session:
                game_ids = find_candidate_games(session, year=year)
                if not game_ids:
                    logger.info("SH/SF backfill: no candidate games found")
                    return
                total = 0
                for gid in game_ids:
                    updated = backfill_game(session, gid)
                    if updated:
                        session.commit()
                        total += updated
                logger.info("SH/SF backfill: %d games updated (%d rows)", len(game_ids), total)
        except Exception:
            logger.exception("SH/SF backfill job failed")


def backfill_player_ids_job():
    """Resolve NULL player_ids in game stats tables for the current year."""
    with MAINTENANCE_LOCK:
        logger.info("=== Starting Player ID Backfill ===")
        try:
            from scripts.maintenance.resolve_null_player_ids_conservative import (
                DEFAULT_OUTPUT_DIR,
                DEFAULT_OVERRIDES_CSV,
                DEFAULT_ROW_OVERRIDES_CSV,
                DEFAULT_TABLES,
                resolve_null_player_ids,
            )

            year = datetime.now(KST).year
            result = resolve_null_player_ids(
                years=(year,),
                tables=DEFAULT_TABLES,
                overrides_csv=DEFAULT_OVERRIDES_CSV,
                row_overrides_csv=DEFAULT_ROW_OVERRIDES_CSV,
                output_dir=DEFAULT_OUTPUT_DIR,
                apply=True,
                backup=True,
                delete_duplicates=True,
            )
            logger.info(
                "Player ID backfill: resolved_groups=%d unresolved_groups=%d updated_rows=%d duplicate_null_rows=%d",
                result.get("resolved_groups", 0),
                result.get("unresolved_groups", 0),
                result.get("updated_rows", 0),
                result.get("duplicate_null_rows", 0),
            )
        except Exception:
            logger.exception("Player ID backfill job failed")


def backfill_advanced_stats_job():
    """Recalculate advanced season stats (batting/pitching/baserunning/fielding)."""
    with MAINTENANCE_LOCK:
        logger.info("=== Starting Advanced Stats Backfill ===")
        try:
            from src.cli.backfill_advanced_stats import backfill_stats

            year = datetime.now(KST).year
            backfill_stats([year], "regular")
            logger.info("Advanced stats backfill completed for %d", year)
        except Exception:
            logger.exception("Advanced stats backfill job failed")


def backfill_roster_job():
    """Monthly full backfill of roster movements and daily rosters."""
    with MAINTENANCE_LOCK:
        logger.info("=== Starting Roster Backfill ===")
        try:
            from scripts.maintenance.backfill_roster_movements import (
                backfill_daily_rosters,
                backfill_player_movements,
            )

            year = datetime.now(KST).year
            asyncio.run(backfill_player_movements([year]))
            end = datetime.now(KST).strftime("%Y-%m-%d")
            start = (datetime.now(KST) - timedelta(days=7)).strftime("%Y-%m-%d")
            asyncio.run(backfill_daily_rosters(start, end))
            logger.info("Roster backfill completed for %d (rosters: %s ~ %s)", year, start, end)
        except Exception:
            logger.exception("Roster backfill job failed")


# ─────────────────────────────────────────────────────────────────────────────
# Tier 3 — Unified Gap Report
# ─────────────────────────────────────────────────────────────────────────────


def gap_report_job():
    """Run the unified gap analysis and send per-category alerts."""
    with MAINTENANCE_LOCK:
        logger.info("=== Starting Gap Report ===")
        try:
            from src.cli.gap_report import run_gap_report

            report = run_gap_report(alert=True, dry_run=False)
            total_gaps = sum(1 for g in report.get("gaps", {}).values() if not g.get("ok", True) and not g.get("error"))
            error_gaps = sum(1 for g in report.get("gaps", {}).values() if g.get("error"))
            logger.info(
                "Gap report complete: %d gap(s), %d error(s)",
                total_gaps,
                error_gaps,
            )
        except Exception:
            logger.exception("Gap report job failed")


# ─────────────────────────────────────────────────────────────────────────────
# Stadium Real-Time Data Jobs (이동 시간 · �잡도 · 운영 공지)
# ─────────────────────────────────────────────────────────────────────────────


def crawl_transit_time_job():
    """
    Transit time measurement job: measure travel times from nearby stations to
    Jamsil Stadium every 15 minutes on game days (D-2h ~ D+1h window).
    Uses LIVE_LOCK to prevent conflicts with live crawlers.
    """
    with LIVE_LOCK:
        logger.info("[Transit] Starting transit time measurement")
        try:
            from src.crawlers.transit_time_crawler import TransitTimeCrawler

            asyncio.run(TransitTimeCrawler().run(save=True))
            logger.info("[Transit] Transit time measurement completed")
        except Exception:
            logger.exception("Transit time job failed")


def crawl_congestion_job():
    """
    Congestion data job: collect real-time congestion for Jamsil area
    every 5 minutes on game days (D-3h ~ D+2h window).
    Uses LIVE_LOCK to stay in the same priority tier as live crawlers.
    """
    with LIVE_LOCK:
        logger.info("[Congestion] Starting congestion data collection")
        try:
            from src.crawlers.congestion_crawler import CongestionCrawler

            asyncio.run(CongestionCrawler().run(save=True))
            logger.info("[Congestion] Congestion data collection completed")
        except Exception:
            logger.exception("Congestion job failed")


def crawl_operation_notices_job():
    """
    Operation notice job: crawl LG Twins and Doosan Bears official notices
    once daily at 09:00 KST (before gates open for evening games).
    Also triggered at 11:30 KST for day-of-game notices.
    Uses DAILY_LOCK.
    """
    with DAILY_LOCK:
        logger.info("[Notice] Starting operation notice crawl")
        try:
            from src.cli.crawl_operation_notices import main as notices_main

            notices_main(["--save", "--incremental"])
            logger.info("[Notice] Operation notice crawl completed")
        except Exception:
            logger.exception("Operation notices job failed")


def crawl_operation_notices_naver_job():
    """
    Naver Search-based operation notice job.
    Queries Naver News API for real-time KBO/stadium notices.
    Runs at 09:30 and 13:00 KST. Uses DAILY_LOCK.
    """
    with DAILY_LOCK:
        logger.info("[NaverNotice] Starting Naver search notice crawl")
        try:
            from src.cli.crawl_operation_notices import main as notices_main

            notices_main(["--source", "naver", "--days", "1", "--save"])
            logger.info("[NaverNotice] Naver notice crawl completed")
        except Exception:
            logger.exception("Naver operation notices job failed")


def crawl_fan_culture_job():
    """
    Fan culture data job: crawl cheer songs, chants, and rivalries from
    Namuwiki. Runs weekly on Saturday 04:00 KST. Uses MAINTENANCE_LOCK.
    """
    with MAINTENANCE_LOCK:
        logger.info("[FanCulture] Starting fan culture data crawl")
        try:
            from src.crawlers.fan_culture_crawler import FanCultureCrawler

            asyncio.run(FanCultureCrawler().run(save=True))
            logger.info("[FanCulture] Fan culture data crawl completed")
        except Exception:
            logger.exception("Fan culture job failed")


def crawl_p0_non_game_job():
    """
    P0 non-game job: crawl team events, roster transactions, and ticket info.
    Runs daily before the freshness monitor. Uses MAINTENANCE_LOCK.
    """
    with MAINTENANCE_LOCK:
        logger.info("[P0NonGame] Starting P0 non-game crawl")
        try:
            from src.cli.crawl_p0_data import main as crawl_p0_data_main

            current_year = datetime.now(KST).year
            result = crawl_p0_data_main(["--type", "all", "--save", "--days", "3", "--season", str(current_year)])
            logger.info("[P0NonGame] P0 non-game crawl completed: %s", result)
            alert_success("crawl_p0_non_game_job", str(result))
        except Exception:
            logger.exception("P0 non-game crawl failed")


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
    if args.run_retire_once:
        crawl_retired_players_job(limit=args.limit)
        return

    scheduler = BlockingScheduler(timezone="Asia/Seoul")
    scheduler.add_listener(job_error_listener, EVENT_JOB_ERROR)

    # Job 1.9: Phase 1 Extra Crawlers (06:00 KST)
    scheduler.add_job(
        crawl_phase1_extra_job,
        trigger=CronTrigger(hour=6, minute=0),
        id="crawl_phase1_extra",
        name="Phase 1 Extra Crawlers",
        misfire_grace_time=7200,
        max_instances=1,
    )
    logger.info("Registered job: crawl_phase1_extra (Daily 06:00 KST)")

    # Job 1.10: P1/P2 Data Crawlers (06:30 KST) — after Phase 1 extra
    scheduler.add_job(
        crawl_p1p2_data_job,
        trigger=CronTrigger(hour=6, minute=30),
        id="crawl_p1p2_data",
        name="P1/P2 Seat/Parking/Food Crawlers",
        misfire_grace_time=7200,
        max_instances=1,
    )
    logger.info("Registered job: crawl_p1p2_data (Daily 06:30 KST)")

    # Job 1.13: Weekly SLA Report (Monday 06:00 KST)
    scheduler.add_job(
        weekly_sla_report_job,
        trigger=CronTrigger(day_of_week="mon", hour=6, minute=0),
        id="weekly_sla_report",
        name="Weekly SLA Report Generation",
        misfire_grace_time=3600,
        max_instances=1,
    )
    logger.info("Registered job: weekly_sla_report (Weekly Monday 06:00 KST)")

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

    # Job 2.6: Monthly Retired Player Crawl (1st of every month at 02:00 KST)
    scheduler.add_job(
        crawl_retired_players_job,
        trigger=CronTrigger(day=1, hour=2, minute=0),
        id="crawl_retired_players",
        name="Monthly Retired Player Crawl",
        misfire_grace_time=3600,
        max_instances=1,
    )
    logger.info("Registered job: crawl_retired_players (Monthly 1st 02:00 KST)")

    # Job 2.7: Monthly Unified Audit (1st of every month at 03:00 KST)
    scheduler.add_job(
        crawl_monthly_unified_audit_job,
        trigger=CronTrigger(day=1, hour=3, minute=0),
        id="crawl_monthly_unified_audit",
        name="Monthly Unified Audit (PA + Team Stats)",
        misfire_grace_time=3600,
        max_instances=1,
    )
    logger.info("Registered job: crawl_monthly_unified_audit (Monthly 1st 03:00 KST)")

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

    # ─────────────────────────────────────────────────────────────────────────
    # Stadium Real-Time Data Jobs
    # ─────────────────────────────────────────────────────────────────────────

    # Job R1: Transit time — every 15 minutes, 10:00 ~ 00:00 KST on game days
    scheduler.add_job(
        crawl_transit_time_job,
        trigger=CronTrigger(hour="10-23", minute="*/15"),
        id="crawl_transit_time",
        name="Stadium Transit Time Measurement (JAMSIL)",
        misfire_grace_time=600,
        max_instances=1,
    )
    logger.info("Registered job: crawl_transit_time (Every 15m, 10:00-23:45 KST)")

    # Job R2: Congestion — every 5 minutes, 10:00 ~ 00:00 KST on game days
    scheduler.add_job(
        crawl_congestion_job,
        trigger=CronTrigger(hour="10-23", minute="*/5"),
        id="crawl_congestion",
        name="Stadium Congestion Data (JAMSIL)",
        misfire_grace_time=300,
        max_instances=1,
    )
    logger.info("Registered job: crawl_congestion (Every 5m, 10:00-23:55 KST)")

    # Job R3a: Operation notices — daily 09:00 KST (morning sweep)
    scheduler.add_job(
        crawl_operation_notices_job,
        trigger=CronTrigger(hour=9, minute=0),
        id="crawl_operation_notices_morning",
        name="Operation Notices Crawl — Morning (LG + Doosan)",
        misfire_grace_time=3600,
        max_instances=1,
    )
    # Job R3b: Operation notices — 11:30 KST (day-of-game sweep)
    scheduler.add_job(
        crawl_operation_notices_job,
        trigger=CronTrigger(hour=11, minute=30),
        id="crawl_operation_notices_daygame",
        name="Operation Notices Crawl — Day-of-Game (LG + Doosan)",
        misfire_grace_time=3600,
        max_instances=1,
    )
    logger.info("Registered job: crawl_operation_notices (09:00 + 11:30 KST daily)")

    # Job R3c: Naver Search-based notices — 09:30 + 13:00 KST
    scheduler.add_job(
        crawl_operation_notices_naver_job,
        trigger=CronTrigger(hour=9, minute=30),
        id="crawl_naver_notices_morning",
        name="Naver Notice Crawl — Morning",
        misfire_grace_time=3600,
        max_instances=1,
    )
    scheduler.add_job(
        crawl_operation_notices_naver_job,
        trigger=CronTrigger(hour=13, minute=0),
        id="crawl_naver_notices_afternoon",
        name="Naver Notice Crawl — Afternoon",
        misfire_grace_time=3600,
        max_instances=1,
    )
    logger.info("Registered job: crawl_naver_notices (09:30 + 13:00 KST daily)")

    # Job M1: Fan culture (cheer songs/chants/rivalries) — Weekly Saturday 04:00 KST
    scheduler.add_job(
        crawl_fan_culture_job,
        trigger=CronTrigger(day_of_week="sat", hour=4, minute=0),
        id="crawl_fan_culture",
        name="Fan Culture Data Crawl (Cheer Songs/Chants)",
        misfire_grace_time=7200,
        max_instances=1,
    )
    logger.info("Registered job: crawl_fan_culture (Weekly Saturday 04:00 KST)")

    # Job M2: P0 non-game data — Daily 06:20 KST
    scheduler.add_job(
        crawl_p0_non_game_job,
        trigger=CronTrigger(hour=6, minute=20),
        id="crawl_p0_non_game",
        name="P0 Non-Game Data Crawl (Events/Roster/Tickets)",
        misfire_grace_time=3600,
        max_instances=1,
    )
    logger.info("Registered job: crawl_p0_non_game (Daily 06:20 KST)")

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

    logger.info("\n" + "=" * 60)
    logger.info(" KBO Crawler Scheduler Started")
    logger.info("=" * 60)
    logger.info(" Timezone: Asia/Seoul")
    logger.info(f" Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    logger.info("\nScheduled Jobs:")
    logger.info("  1. Phase 1 Extra Crawlers: Every day at 06:00 KST")
    logger.info("  2. P0 Non-Game Data: Every day at 06:20 KST")
    logger.info("  3. P1/P2 Seat/Parking/Food: Every day at 06:30 KST")
    logger.info("  4. Pregame Refresh: Every 15 minutes, 10:00-23:45 KST, today + lookahead")
    logger.info("  5. Live Refresh: Every 2 minutes, 12:00-23:30 KST")
    logger.info("  6. Park Factor Computation: Every Sunday at 05:30 KST")
    logger.info("  7. Retired Player Crawl: 1st of every month at 02:00 KST")
    logger.info("  8. Weekly SLA Report: Every Monday at 06:00 KST")
    logger.info("  9. Transit Time (JAMSIL): Every 15m, 10:00-23:45 KST")
    logger.info(" 10. Congestion (JAMSIL): Every 5m, 10:00-23:55 KST")
    logger.info(" 11. Operation Notices (Official): Daily 09:00 + 11:30 KST")
    logger.info(" 12. Operation Notices (Naver): Daily 09:30 + 13:00 KST")
    logger.info(" 13. Fan Culture (Cheer Songs/Chants): Weekly Saturday 04:00 KST")
    logger.info("=" * 60 + "\n")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user")
        logger.info("\nScheduler stopped")


if __name__ == "__main__":
    main()
