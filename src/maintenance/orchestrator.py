"""Unified Maintenance Orchestrator for database repair and scheduled tasks."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import Engine, get_db_session
from src.maintenance.dto import (
    MaintenanceRunReport,
    MaintenanceTaskMeta,
    MaintenanceTaskResult,
    MaintenanceTaskType,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine as SqlEngine
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class MaintenanceOrchestrator:
    """Orchestrates routine database repairs, integrity fixes, and maintenance tasks."""

    def __init__(self, engine: SqlEngine | None = None) -> None:
        """Initialize maintenance orchestrator."""
        self.engine = engine or Engine

    def list_tasks(self) -> list[MaintenanceTaskMeta]:
        """Return metadata for all available maintenance tasks."""
        return [
            MaintenanceTaskMeta(
                task_name="pa_formula_audit",
                task_type=MaintenanceTaskType.PA_AUDIT,
                description="Audit and fix PA formula violations (PA = AB + BB + HBP + SH + SF)",
                safe_mode_supported=True,
            ),
            MaintenanceTaskMeta(
                task_name="null_player_ids",
                task_type=MaintenanceTaskType.NULL_PLAYER_IDS,
                description="Resolve and backfill NULL player_ids in game stats and lineups",
                safe_mode_supported=True,
            ),
            MaintenanceTaskMeta(
                task_name="data_cleanup",
                task_type=MaintenanceTaskType.DATA_CLEANUP,
                description="Clean up orphan records and temporary staging data",
                safe_mode_supported=True,
            ),
            MaintenanceTaskMeta(
                task_name="wal_checkpoint",
                task_type=MaintenanceTaskType.WAL_CHECKPOINT,
                description="Execute database WAL checkpoint for SQLite maintenance",
                safe_mode_supported=False,
            ),
        ]

    def run_pa_formula_audit(
        self,
        year: int | None = None,
        *,
        apply: bool = False,
        session: Session | None = None,
    ) -> MaintenanceTaskResult:
        """Audit and conservatively fix PA formula mismatches."""
        start_mono = time.monotonic()

        def _execute(s: Session) -> int:
            if year:
                sql_check = text("""
                SELECT id FROM game_batting_stats
                WHERE plate_appearances != (
                    at_bats + walks + COALESCE(hbp, 0) + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
                ) AND game_id LIKE :year_prefix
                """)
                rows = s.execute(sql_check, {"year_prefix": f"{year}%"}).fetchall()
            else:
                sql_check = text("""
                SELECT id FROM game_batting_stats
                WHERE plate_appearances != (
                    at_bats + walks + COALESCE(hbp, 0) + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
                )
                """)
                rows = s.execute(sql_check).fetchall()
            affected = len(rows)

            if apply and affected > 0:
                if year:
                    sql_fix = text("""
                    UPDATE game_batting_stats
                    SET plate_appearances = at_bats + walks + COALESCE(hbp, 0)
                        + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
                    WHERE plate_appearances != (
                        at_bats + walks + COALESCE(hbp, 0) + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
                    ) AND game_id LIKE :year_prefix
                    """)
                    s.execute(sql_fix, {"year_prefix": f"{year}%"})
                else:
                    sql_fix = text("""
                    UPDATE game_batting_stats
                    SET plate_appearances = at_bats + walks + COALESCE(hbp, 0)
                        + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
                    WHERE plate_appearances != (
                        at_bats + walks + COALESCE(hbp, 0) + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
                    )
                    """)
                    s.execute(sql_fix)
                s.flush()
                logger.info("Fixed PA formula for %d rows in game_batting_stats", affected)

            return affected

        try:
            if session is not None:
                rows_count = _execute(session)
            else:
                with get_db_session() as s:
                    rows_count = _execute(s)

            duration = time.monotonic() - start_mono
            status = "SUCCESS" if apply else "DRY_RUN"
            return MaintenanceTaskResult(
                task_name="pa_formula_audit",
                status=status,
                rows_affected=rows_count,
                duration_seconds=duration,
            )
        except SQLAlchemyError as exc:
            duration = time.monotonic() - start_mono
            logger.exception("PA formula audit failed")
            return MaintenanceTaskResult(
                task_name="pa_formula_audit",
                status="FAILED",
                duration_seconds=duration,
                error_message=str(exc),
            )

    def run_null_player_ids_audit(
        self,
        year: int | None = None,
        *,
        apply: bool = False,
        session: Session | None = None,
    ) -> MaintenanceTaskResult:
        """Resolve and backfill NULL player_ids using canonical player registry."""
        start_mono = time.monotonic()

        def _execute(s: Session) -> int:
            if year:
                sql_check = text("""
                SELECT g.id, g.player_name, g.team_code, p.player_id AS resolved_id
                FROM game_batting_stats g
                JOIN player_basic p ON g.player_name = p.name AND g.team_code = p.team
                WHERE g.player_id IS NULL AND g.game_id LIKE :year_prefix
                """)
                rows = s.execute(sql_check, {"year_prefix": f"{year}%"}).fetchall()
            else:
                sql_check = text("""
                SELECT g.id, g.player_name, g.team_code, p.player_id AS resolved_id
                FROM game_batting_stats g
                JOIN player_basic p ON g.player_name = p.name AND g.team_code = p.team
                WHERE g.player_id IS NULL
                """)
                rows = s.execute(sql_check).fetchall()
            affected = len(rows)

            if apply and affected > 0:
                for row in rows:
                    row_id, _, _, resolved_id = row
                    s.execute(
                        text("UPDATE game_batting_stats SET player_id = :p_id WHERE id = :r_id"),
                        {"p_id": resolved_id, "r_id": row_id},
                    )
                s.flush()
                logger.info("Resolved %d NULL player_ids in game_batting_stats", affected)

            return affected

        try:
            if session is not None:
                rows_count = _execute(session)
            else:
                with get_db_session() as s:
                    rows_count = _execute(s)

            duration = time.monotonic() - start_mono
            status = "SUCCESS" if apply else "DRY_RUN"
            return MaintenanceTaskResult(
                task_name="null_player_ids",
                status=status,
                rows_affected=rows_count,
                duration_seconds=duration,
            )
        except SQLAlchemyError as exc:
            duration = time.monotonic() - start_mono
            logger.exception("NULL player_ids audit failed")
            return MaintenanceTaskResult(
                task_name="null_player_ids",
                status="FAILED",
                duration_seconds=duration,
                error_message=str(exc),
            )

    def run_data_cleanup(
        self,
        *,
        dry_run: bool = True,
        session: Session | None = None,
    ) -> MaintenanceTaskResult:
        """Clean up orphan child rows without valid game parent."""
        start_mono = time.monotonic()

        def _execute(s: Session) -> int:
            sql_check = text("""
            SELECT id FROM game_batting_stats
            WHERE game_id NOT IN (SELECT game_id FROM game)
            """)
            rows = s.execute(sql_check).fetchall()
            affected = len(rows)

            if not dry_run and affected > 0:
                sql_delete = text("""
                DELETE FROM game_batting_stats
                WHERE game_id NOT IN (SELECT game_id FROM game)
                """)
                s.execute(sql_delete)
                s.flush()
                logger.info("Deleted %d orphan batting stats rows", affected)

            return affected

        try:
            if session is not None:
                rows_count = _execute(session)
            else:
                with get_db_session() as s:
                    rows_count = _execute(s)

            duration = time.monotonic() - start_mono
            status = "DRY_RUN" if dry_run else "SUCCESS"
            return MaintenanceTaskResult(
                task_name="data_cleanup",
                status=status,
                rows_affected=rows_count,
                duration_seconds=duration,
            )
        except SQLAlchemyError as exc:
            duration = time.monotonic() - start_mono
            logger.exception("Data cleanup failed")
            return MaintenanceTaskResult(
                task_name="data_cleanup",
                status="FAILED",
                duration_seconds=duration,
                error_message=str(exc),
            )

    def run_wal_checkpoint(self, engine: SqlEngine | None = None) -> MaintenanceTaskResult:
        """Execute SQLite WAL checkpoint truncate."""
        start_mono = time.monotonic()
        target_engine = engine or self.engine

        if "sqlite" not in target_engine.dialect.name.lower():
            return MaintenanceTaskResult(
                task_name="wal_checkpoint",
                status="SKIPPED",
                rows_affected=0,
                duration_seconds=0.0,
            )

        try:
            with target_engine.connect() as conn:
                conn.execute(text("PRAGMA wal_checkpoint(TRUNCATE)"))
            duration = time.monotonic() - start_mono
            return MaintenanceTaskResult(
                task_name="wal_checkpoint",
                status="SUCCESS",
                rows_affected=0,
                duration_seconds=duration,
            )
        except SQLAlchemyError as exc:
            duration = time.monotonic() - start_mono
            logger.exception("WAL checkpoint failed")
            return MaintenanceTaskResult(
                task_name="wal_checkpoint",
                status="FAILED",
                duration_seconds=duration,
                error_message=str(exc),
            )

    def run_all(
        self,
        year: int | None = None,
        *,
        apply: bool = False,
        session: Session | None = None,
    ) -> MaintenanceRunReport:
        """Execute all standard maintenance tasks in order and return a batch run report."""
        started_at = datetime.now(UTC).isoformat()
        start_mono = time.monotonic()
        results: list[MaintenanceTaskResult] = []

        results.append(self.run_pa_formula_audit(year=year, apply=apply, session=session))
        results.append(self.run_null_player_ids_audit(year=year, apply=apply, session=session))
        results.append(self.run_data_cleanup(dry_run=not apply, session=session))
        results.append(self.run_wal_checkpoint())

        duration = time.monotonic() - start_mono
        successful = sum(1 for r in results if r.status in ("SUCCESS", "DRY_RUN", "SKIPPED"))
        failed = sum(1 for r in results if r.status == "FAILED")
        total_rows = sum(r.rows_affected for r in results)

        return MaintenanceRunReport(
            total_tasks=len(results),
            successful_tasks=successful,
            failed_tasks=failed,
            total_rows_affected=total_rows,
            duration_seconds=duration,
            results=results,
            started_at=started_at,
            completed_at=datetime.now(UTC).isoformat(),
        )
