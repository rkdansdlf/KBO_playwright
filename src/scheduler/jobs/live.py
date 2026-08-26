"""Live match crawling and pregame refresh jobs for KBO scheduler."""

from __future__ import annotations

import asyncio
import datetime as dt_module
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_exponential

from src.cli.daily_preview_batch import run_preview_batch
from src.cli.live_crawler import run_live_crawler_cycle
from src.db.engine import DATABASE_URL, SessionLocal
from src.db.sqlite_integrity import check_sqlite_database, is_sqlite_corruption_error
from src.scheduler.alerting import alert_failure, alert_success
from src.scheduler.config import (
    ALERT_EXCEPTIONS,
    FALSE_ENV_VALUES,
    KST,
    SCHEDULER_JOB_EXCEPTIONS,
    _env_enabled,
)
from src.scheduler.locks import LIVE_LOCK, _sqlite_writer_lock
from src.utils.alerting import SlackWebhookClient

if TYPE_CHECKING:
    from src.utils.polling_policy import AdaptivePollingEngine

logger = logging.getLogger("src.scheduler.jobs.live")

RECENT_UPDATE_WINDOW_SECONDS = 600
SOON_START_WINDOW_SECONDS = 900


@dataclass
class _LiveJobState:
    missing_pregame_alerted_dates: set[str]
    last_live_run_time: datetime | None = None
    last_live_poll_interval: int | None = None
    last_pregame_run_time: datetime | None = None
    adaptive_polling_engine: AdaptivePollingEngine | None = None


_STATE = _LiveJobState(missing_pregame_alerted_dates=set())

# Exported module-level aliases for backward compatibility
MISSING_PREGAME_ALERTED_DATES = _STATE.missing_pregame_alerted_dates
LAST_LIVE_RUN_TIME = _STATE.last_live_run_time
LAST_LIVE_POLL_INTERVAL = _STATE.last_live_poll_interval
LAST_PREGAME_RUN_TIME = _STATE.last_pregame_run_time


def _previous_day_kst() -> str:
    """Return yesterday in KST as YYYYMMDD."""
    return (datetime.now(KST) - timedelta(days=1)).strftime("%Y%m%d")


def _pregame_target_dates(now: datetime | None = None) -> list[str]:
    """Return pregame dates to refresh for the current scheduler tick."""
    current = now or datetime.now(KST)
    current = current.replace(tzinfo=KST) if current.tzinfo is None else current.astimezone(KST)

    try:
        lookahead_days = int(os.getenv("PREGAME_LOOKAHEAD_DAYS", "2"))
    except ValueError:
        lookahead_days = 2
    lookahead_days = max(0, min(lookahead_days, 7))

    return [(current + timedelta(days=offset)).strftime("%Y%m%d") for offset in range(lookahead_days + 1)]


def _should_skip_live_for_pregame(now: datetime | None = None) -> bool:
    try:
        cooldown_seconds = int(os.getenv("LIVE_PREGAME_COOLDOWN_SECONDS", "30"))
    except ValueError:
        cooldown_seconds = 30
    if cooldown_seconds <= 0:
        return False

    mod = sys.modules.get("scripts.scheduler") or sys.modules.get("src.scheduler")
    last_pregame = (
        getattr(mod, "LAST_PREGAME_RUN_TIME", _STATE.last_pregame_run_time) if mod else _STATE.last_pregame_run_time
    )
    if last_pregame is None:
        return False

    current = now or datetime.now(KST)
    elapsed = (current - last_pregame).total_seconds()
    return 0 <= elapsed < cooldown_seconds


def _pregame_refresh_summary(target_date: str) -> tuple[int, int, int]:
    """Return scheduled games, missing starters count, and preview-missing count for a date."""
    try:
        datetime.strptime(target_date, "%Y%m%d").replace(tzinfo=KST)
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

    mod = sys.modules.get("scripts.scheduler") or sys.modules.get("src.scheduler")
    session_factory = getattr(mod, "SessionLocal", SessionLocal) if mod else SessionLocal

    try:
        with session_factory() as session:
            rows = session.execute(query, {"target_date": target_date}).all()
    except SCHEDULER_JOB_EXCEPTIONS as exc:
        if is_sqlite_corruption_error(exc):
            logger.critical("[PregameSummary] SQLite corruption error encountered: %s", exc)
            try:
                report = check_sqlite_database(DATABASE_URL)
                logger.warning("[PregameSummary] SQLite integrity report: %s", report)
            except Exception:
                logger.exception("[PregameSummary] Failed running sqlite_integrity_guard check")
        else:
            logger.exception("[PregameSummary] Failed to query pregame refresh summary for %s", target_date)
        return 0, 0, 0

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


def _live_refresh_max_games_per_cycle() -> int | None:
    raw = os.getenv("LIVE_REFRESH_MAX_GAMES_PER_CYCLE", "1").strip().lower()
    if raw in FALSE_ENV_VALUES or raw == "all":
        return None
    try:
        val = int(raw)
    except ValueError:
        logger.warning(
            "Invalid integer for LIVE_REFRESH_MAX_GAMES_PER_CYCLE=%r; using default=1",
            raw,
        )
        return 1
    return val if val > 0 else None


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=30, max=120),
    retry_error_callback=alert_failure,
)
def _process_pregame_date(
    target_date: str,
    *,
    refresh_only_missing: bool = True,
    alert_on_missing: bool = False,
) -> int:
    scheduled_count, starters_missing, preview_missing = _pregame_refresh_summary(target_date)
    if scheduled_count == 0:
        return 0
    if refresh_only_missing and starters_missing == 0 and preview_missing == 0:
        _STATE.missing_pregame_alerted_dates.discard(target_date)
        logger.info("Skipping pregame refresh for target_date=%s (all present)", target_date)
        return 0
    saved_ids = asyncio.run(run_preview_batch(target_date))
    post = _pregame_refresh_summary(target_date)
    if post[0] and (post[1] > 0 or post[2] > 0):
        if alert_on_missing and target_date not in _STATE.missing_pregame_alerted_dates:
            try:
                SlackWebhookClient.send_alert(
                    f"Pregame missing remains for {target_date}: starters_missing={post[1]}, preview_missing={post[2]}"
                )
            except ALERT_EXCEPTIONS:
                logger.exception("Failed to send pregame missing alert for target_date=%s", target_date)
            _STATE.missing_pregame_alerted_dates.add(target_date)
    else:
        _STATE.missing_pregame_alerted_dates.discard(target_date)
    if scheduled_count and not saved_ids:
        logger.warning(
            "Pregame refresh saved no preview rows for %s: scheduled=%d, saved=0.", target_date, scheduled_count
        )
    return len(saved_ids or [])


def crawl_pregame_refresh() -> None:
    """Run pregame lineup and preview refresh across target dates."""
    mod = sys.modules.get("scripts.scheduler") or sys.modules.get("src.scheduler")
    live_lock = getattr(mod, "LIVE_LOCK", LIVE_LOCK) if mod else LIVE_LOCK

    if not live_lock.acquire(blocking=False):
        logger.info("Skipping pregame refresh because LIVE_LOCK is already held")
        return
    try:
        refresh_only_missing = _env_enabled("PREGAME_REFRESH_ONLY_MISSING", "1")
        alert_on_missing = _env_enabled("PREGAME_MISSING_ALERT", "0")
        for target_date in _pregame_target_dates():
            _process_pregame_date(
                target_date,
                refresh_only_missing=refresh_only_missing,
                alert_on_missing=alert_on_missing,
            )
        alert_success("crawl_pregame_refresh")
        now_val = datetime.now(KST)
        _STATE.last_pregame_run_time = now_val
        if mod and hasattr(mod, "LAST_PREGAME_RUN_TIME"):
            mod.LAST_PREGAME_RUN_TIME = now_val
    finally:
        live_lock.release()


def _parse_start_time(
    start_time_raw: object,
    now: datetime,
    earliest_start_time: datetime | None,
) -> datetime | None:
    try:
        if isinstance(start_time_raw, str):
            parts = list(map(int, start_time_raw.split(":")[:2]))
            start_time = now.replace(hour=parts[0], minute=parts[1], second=0, microsecond=0)
        else:
            start_time = now.replace(
                hour=start_time_raw.hour,  # type: ignore[attr-defined]
                minute=start_time_raw.minute,  # type: ignore[attr-defined]
                second=0,
                microsecond=0,
            )
        if start_time < now:
            start_time += timedelta(days=1)
        if earliest_start_time is None or start_time < earliest_start_time:
            return start_time
    except (ValueError, TypeError, IndexError, AttributeError):
        logger.warning("[LiveInterval] Failed to parse start_time: %s", start_time_raw)
    return earliest_start_time


def _parse_update_time(
    updated_at_raw: object,
    latest_update_time: datetime | None,
) -> datetime | None:
    try:
        updated_dt = datetime.fromisoformat(updated_at_raw) if isinstance(updated_at_raw, str) else updated_at_raw
        if getattr(updated_dt, "tzinfo", None) is None:
            updated_kst = updated_dt.replace(tzinfo=KST)  # type: ignore[union-attr]
        else:
            updated_kst = updated_dt.astimezone(KST)  # type: ignore[union-attr]
        if latest_update_time is None or updated_kst > latest_update_time:
            return updated_kst
    except (ValueError, TypeError, OSError, AttributeError):
        logger.debug("Skipping unparsable live game update timestamp", exc_info=True)
    return latest_update_time


def _categorize_game_row(
    row: object,
    terminal_statuses: set[str],
    terminal_lifecycles: set[str],
    active_statuses: set[str],
    active_lifecycles: set[str],
) -> tuple[bool, bool, bool]:
    status = str(row[0] or "").upper()  # type: ignore[index]
    lifecycle = str(row[1] or "").lower()  # type: ignore[index]
    not_terminal = status not in terminal_statuses and lifecycle not in terminal_lifecycles
    is_active = status in active_statuses or lifecycle in active_lifecycles
    is_suspended = is_active and (status in {"DELAYED", "SUSPENDED"} or lifecycle == "suspended")
    return not_terminal, is_active, is_suspended


def _analyze_game_rows(rows: list[object], now: datetime) -> tuple[bool, bool, bool, datetime | None, datetime | None]:
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
        if str(row[0] or "").upper() == "SCHEDULED" and row[2]:  # type: ignore[index]
            earliest_start_time = _parse_start_time(row[2], now, earliest_start_time)  # type: ignore[index]
        if row[3]:  # type: ignore[index]
            latest_update_time = _parse_update_time(row[3], latest_update_time)  # type: ignore[index]
    return has_active, has_suspended, all_terminal, earliest_start_time, latest_update_time


def _get_adaptive_polling_engine() -> AdaptivePollingEngine:
    """Return the shared AdaptivePollingEngine configured for scheduler cadence."""
    if _STATE.adaptive_polling_engine is None:
        from src.utils.polling_policy import AdaptivePollingEngine, PollingPolicyConfig

        _STATE.adaptive_polling_engine = AdaptivePollingEngine(
            PollingPolicyConfig(
                interval_running=10,
                interval_high_leverage=5,
                interval_delayed=60,
                interval_stabilization=60,
                interval_terminal=1800,
                interval_before=120,
            ),
        )
    return _STATE.adaptive_polling_engine


def _interval_from_analysis(  # noqa: PLR0913
    now: datetime,
    has_active: bool,  # noqa: FBT001
    has_suspended: bool,  # noqa: FBT001
    all_terminal: bool,  # noqa: FBT001
    earliest_start_time: datetime | None,
    latest_update_time: datetime | None,
) -> int:
    if has_active:
        engine = _get_adaptive_polling_engine()
        decision = engine.evaluate("delayed" if has_suspended else "running")
        return decision.interval_seconds
    if all_terminal:
        if latest_update_time:
            elapsed = (now - latest_update_time).total_seconds()
            if 0 <= elapsed < RECENT_UPDATE_WINDOW_SECONDS:
                return 60
        return 1800
    if earliest_start_time:
        time_to_start = (earliest_start_time - now).total_seconds()
        if 0 <= time_to_start <= SOON_START_WINDOW_SECONDS or time_to_start < 0:
            return 30
    return 120


def _get_live_poll_interval_seconds() -> int:
    mod = sys.modules.get("scripts.scheduler") or sys.modules.get("src.scheduler")
    dt_cls = getattr(mod, "datetime", dt_module.datetime) if mod else dt_module.datetime
    session_factory = getattr(mod, "SessionLocal", SessionLocal) if mod else SessionLocal

    now = dt_cls.now(KST) if hasattr(dt_cls, "now") else datetime.now(KST)
    query = text(
        """
        SELECT g.game_status, g.game_lifecycle_state, m.start_time, g.updated_at
        FROM game g
        LEFT JOIN game_metadata m ON g.game_id = m.game_id
        WHERE g.game_date = :today
        """,
    )
    try:
        with session_factory() as session:
            rows = session.execute(query, {"today": now.date()}).all()
    except SCHEDULER_JOB_EXCEPTIONS:
        logger.exception("[LiveInterval] Failed to query game states; defaulting to 120s")
        return 120
    if not rows:
        return 1800
    has_active, has_suspended, all_terminal, earliest_start_time, latest_update_time = _analyze_game_rows(rows, now)
    return _interval_from_analysis(
        now, has_active, has_suspended, all_terminal, earliest_start_time, latest_update_time
    )


def _sync_live_poll_interval(mod: object | None, interval: int) -> None:

    if interval != _STATE.last_live_poll_interval:
        logger.info(
            "[LiveInterval] Polling interval changed: %s -> %ds",
            f"{_STATE.last_live_poll_interval}s" if _STATE.last_live_poll_interval else "None",
            interval,
        )
        _STATE.last_live_poll_interval = interval
        if mod and hasattr(mod, "LAST_LIVE_POLL_INTERVAL"):
            mod.LAST_LIVE_POLL_INTERVAL = interval


def crawl_live_refresh() -> None:
    """Execute live polling crawler cycle if due."""
    mod = sys.modules.get("scripts.scheduler") or sys.modules.get("src.scheduler")
    live_lock = getattr(mod, "LIVE_LOCK", LIVE_LOCK) if mod else LIVE_LOCK

    skip_fn = getattr(mod, "_should_skip_live_for_pregame", None) or _should_skip_live_for_pregame
    if skip_fn():
        logger.info("Skipping live refresh because pregame refresh is due soon")
        return

    now = datetime.now(KST)
    interval_fn = getattr(mod, "_get_live_poll_interval_seconds", None) or _get_live_poll_interval_seconds
    interval = interval_fn()
    _sync_live_poll_interval(mod, interval)

    last_live_run = getattr(mod, "LAST_LIVE_RUN_TIME", _STATE.last_live_run_time) if mod else _STATE.last_live_run_time

    if last_live_run is not None:
        elapsed = (now - last_live_run).total_seconds()
        if elapsed < interval:
            # Fast exit without acquiring LIVE_LOCK
            return

    if not live_lock.acquire(blocking=False):
        logger.info("Skipping live refresh because LIVE_LOCK is already held")
        return

    _STATE.last_live_run_time = now
    if mod and hasattr(mod, "LAST_LIVE_RUN_TIME"):
        mod.LAST_LIVE_RUN_TIME = now
    try:
        logger.info("Running live refresh cycle")
        cycle_fn = getattr(mod, "run_live_crawler_cycle", None) or run_live_crawler_cycle
        max_games_fn = getattr(mod, "_live_refresh_max_games_per_cycle", None) or _live_refresh_max_games_per_cycle
        with _sqlite_writer_lock():
            asyncio.run(
                cycle_fn(
                    max_active_games=max_games_fn(),
                    detail_snapshot_background=True,
                ),
            )

    except SCHEDULER_JOB_EXCEPTIONS:
        _STATE.last_live_run_time = now
        _STATE.last_live_poll_interval = max(interval, 60)
        if mod and hasattr(mod, "LAST_LIVE_RUN_TIME"):
            mod.LAST_LIVE_RUN_TIME = now
        if mod and hasattr(mod, "LAST_LIVE_POLL_INTERVAL"):
            mod.LAST_LIVE_POLL_INTERVAL = _STATE.last_live_poll_interval
        raise
    finally:
        live_lock.release()
