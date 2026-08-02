"""OCI Data Sync Lag Monitor and Auto Re-Sync Trigger.

Detects tables where SQLite and OCI sync lag exceeds 24 hours (86,400 seconds)
and automatically triggers incremental re-synchronization. Updates Prometheus metrics
instantly.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from src.constants import KST
from src.db.engine import SessionLocal, create_engine_for_url, get_oci_url
from src.utils.metrics import (
    KBO_OCI_SYNC_ERRORS_TOTAL,
    KBO_OCI_SYNC_LAG_SECONDS,
    KBO_OCI_TABLE_SYNC_LAG_SECONDS,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

DEFAULT_LAG_THRESHOLD_SECONDS = 86400.0  # 24 hours

# Table definitions for lag monitoring: table_name -> (timestamp_column, resync_category)
MONITORED_TABLE_SPECS: dict[str, tuple[str, str]] = {
    "game": ("updated_at", "games"),
    "game_batting_stats": ("updated_at", "game_details"),
    "game_pitching_stats": ("updated_at", "game_details"),
    "game_lineups": ("updated_at", "game_details"),
    "game_inning_scores": ("updated_at", "game_details"),
    "player_season_batting": ("updated_at", "season_stats"),
    "player_season_pitching": ("updated_at", "season_stats"),
    "team_standings_daily": ("updated_at", "standings"),
    "team_daily_roster": ("updated_at", "daily_roster"),
    "player_movements": ("created_at", "player_movements"),
    "rag_chunks": ("updated_at", "rag_chunks"),
    "stadium_transit_times": ("created_at", "transit_times"),
    "stadium_congestion": ("created_at", "congestion"),
    "stadium_operation_notices": ("created_at", "operation_notices"),
    "team_events": ("created_at", "team_events"),
}


def _parse_timestamp(val: Any) -> datetime | None:  # noqa: ANN401
    """Safely parse a timestamp value to datetime."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val)
        except ValueError:
            return None
    return None


def get_table_max_timestamp(session: Session, table_name: str, col_name: str = "updated_at") -> datetime | None:
    """Query MAX(col_name) for a given table in session."""
    try:
        query = text(f"SELECT MAX({col_name}) FROM {table_name}")  # noqa: S608
        row = session.execute(query).scalar()
        return _parse_timestamp(row)
    except (SQLAlchemyError, RuntimeError, OSError, ValueError) as exc:
        logger.debug("Failed to query MAX(%s) for table %s: %s", col_name, table_name, exc)
        return None


def check_and_resync_lagging_tables(
    target_url: str | None = None,
    threshold_seconds: float = DEFAULT_LAG_THRESHOLD_SECONDS,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Detect tables lagging > threshold_seconds and trigger incremental re-sync.

    Args:
        target_url: Target OCI database URL. Defaults to get_oci_url().
        threshold_seconds: Lag threshold in seconds (default: 86400s = 24h).
        dry_run: If True, detect and update metrics but skip actual re-sync execution.

    Returns:
        Summary dict containing lagging tables, max lag, and resync outcomes.

    """
    oci_url = target_url or get_oci_url()
    if not oci_url:
        logger.warning("OCI_DB_URL not set. Skipping OCI sync lag check.")
        return {"status": "skipped", "reason": "missing_target_url"}

    oci_engine = create_engine_for_url(oci_url)
    oci_session_factory = sessionmaker(bind=oci_engine)

    lag_results: dict[str, float] = {}
    lagging_tables: list[str] = []
    max_overall_lag = 0.0

    with SessionLocal() as sqlite_session, oci_session_factory() as oci_session:
        for table_name, (col_name, _category) in MONITORED_TABLE_SPECS.items():
            sqlite_max = get_table_max_timestamp(sqlite_session, table_name, col_name)
            if sqlite_max is None:
                continue

            oci_max = get_table_max_timestamp(oci_session, table_name, col_name)

            if oci_max is None:
                # OCI table is empty or missing data while SQLite has records
                lag_seconds = threshold_seconds + 3600.0  # Treat as lagging (> threshold)
            else:
                sq_dt = sqlite_max if sqlite_max.tzinfo else sqlite_max.replace(tzinfo=KST)
                oci_dt = oci_max if oci_max.tzinfo else oci_max.replace(tzinfo=KST)
                lag_seconds = max(0.0, (sq_dt - oci_dt).total_seconds())

            lag_results[table_name] = lag_seconds
            max_overall_lag = max(max_overall_lag, lag_seconds)

            # Update Prometheus metric per table
            metric_val = lag_seconds if math.isfinite(lag_seconds) else 999999.0
            KBO_OCI_TABLE_SYNC_LAG_SECONDS.labels(table=table_name).set(metric_val)

            if lag_seconds >= threshold_seconds:
                lagging_tables.append(table_name)
                logger.warning(
                    "⚠️ OCI sync lag >= 24h detected for table '%s': %.1fs (threshold=%.1fs)",
                    table_name,
                    lag_seconds,
                    threshold_seconds,
                )

    oci_engine.dispose()

    # Update overall lag gauge
    KBO_OCI_SYNC_LAG_SECONDS.set(max_overall_lag)

    resynced_counts: dict[str, Any] = {}
    if lagging_tables and not dry_run:
        from src.sync.oci_sync import OCISync

        # Collect unique resync categories for lagging tables
        categories_to_sync = dict.fromkeys(
            MONITORED_TABLE_SPECS[tbl][1] for tbl in lagging_tables if tbl in MONITORED_TABLE_SPECS
        )
        logger.info(
            "🔄 Triggering incremental re-sync for %d lagging tables (categories: %s)...",
            len(lagging_tables),
            list(categories_to_sync.keys()),
        )

        with SessionLocal() as sqlite_session:
            syncer = OCISync(oci_url, sqlite_session)
            try:
                for cat in categories_to_sync:
                    try:
                        count = _execute_category_resync(syncer, cat)
                        resynced_counts[cat] = count
                        logger.info("✅ Incremental re-sync finished for category '%s': %s records", cat, count)
                    except (SQLAlchemyError, RuntimeError, OSError, ValueError) as e:
                        KBO_OCI_SYNC_ERRORS_TOTAL.inc()
                        logger.exception("❌ Failed incremental re-sync for category '%s'", cat)
                        resynced_counts[cat] = f"error: {e}"
            finally:
                syncer.close()

    return {
        "status": "completed",
        "monitored_tables_count": len(lag_results),
        "lagging_tables": lagging_tables,
        "overall_max_lag_seconds": max_overall_lag,
        "resynced_counts": resynced_counts,
        "dry_run": dry_run,
    }


def _execute_category_resync(syncer: Any, category: str) -> Any:  # noqa: ANN401, C901, PLR0911
    """Execute incremental re-sync for a specific category on OCISync instance."""
    if category == "games":
        return syncer.sync_games()
    if category == "game_details":
        return syncer.sync_game_details(unsynced_only=True)
    if category == "season_stats":
        return syncer.sync_season_stats(unsynced_only=True)
    if category == "standings":
        return syncer.sync_standings()
    if category == "daily_roster":
        return syncer.sync_daily_rosters()
    if category == "player_movements":
        return syncer.sync_player_movements()
    if category == "rag_chunks":
        return syncer.sync_rag_chunks()
    if category == "transit_times":
        return syncer.sync_transit_times()
    if category == "congestion":
        return syncer.sync_congestion()
    if category == "operation_notices":
        return syncer.sync_operation_notices()
    if category == "team_events":
        return syncer.sync_team_events()
    logger.warning("Unknown resync category: %s", category)
    return 0
