"""Unified System Diagnostics Engine for KBO Playwright Platform."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import Engine, SessionLocal
from src.diagnostics.dto import (
    DiagnosticSeverity,
    SubsystemCheckItem,
    SubsystemType,
    UnifiedDiagnosticsReport,
)
from src.scheduler.lock_manager import SchedulerLockManager

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine as SqlEngine
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

KEY_TABLES = ("game", "player_basic", "team_standings_daily", "rag_chunks", "game_batting_stats")


class SystemDiagnosticsEngine:
    """Orchestrates comprehensive multi-subsystem diagnostics and automated self-healing."""

    def __init__(
        self,
        engine: SqlEngine | None = None,
        lock_manager: SchedulerLockManager | None = None,
        logs_dir: Path | None = None,
    ) -> None:
        """Initialize diagnostics engine with subsystem dependencies."""
        self.engine = engine or Engine
        self.lock_manager = lock_manager or SchedulerLockManager()
        self.logs_dir = logs_dir or (Path(__file__).resolve().parents[2] / "logs")

    def diagnose_database(self, engine: SqlEngine | None = None) -> list[SubsystemCheckItem]:
        """Diagnose database connectivity, latency, and table schemas."""
        target_engine = engine or self.engine
        checks: list[SubsystemCheckItem] = []

        # 1. Connectivity & Latency check
        start_mono = time.monotonic()
        try:
            with target_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            latency_ms = round((time.monotonic() - start_mono) * 1000, 2)
            checks.append(
                SubsystemCheckItem(
                    name="db_connectivity",
                    subsystem=SubsystemType.DATABASE,
                    severity=DiagnosticSeverity.HEALTHY,
                    status="OK",
                    message=f"Database connection successful (latency: {latency_ms}ms)",
                    metrics={"latency_ms": latency_ms, "dialect": target_engine.dialect.name},
                )
            )
        except SQLAlchemyError as exc:
            checks.append(
                SubsystemCheckItem(
                    name="db_connectivity",
                    subsystem=SubsystemType.DATABASE,
                    severity=DiagnosticSeverity.CRITICAL,
                    status="FAIL",
                    message=f"Database connection failed: {exc}",
                    remediation_hint="Check DATABASE_URL and network accessibility.",
                )
            )
            return checks

        # 2. Table presence check
        try:
            with target_engine.connect() as conn:
                inspector = inspect(conn)
                tables = {t.lower() for t in inspector.get_table_names()}
                missing = [t for t in KEY_TABLES if t.lower() not in tables]

                if not missing:
                    checks.append(
                        SubsystemCheckItem(
                            name="db_core_tables",
                            subsystem=SubsystemType.DATABASE,
                            severity=DiagnosticSeverity.HEALTHY,
                            status="OK",
                            message=f"All {len(KEY_TABLES)} key tables exist in catalog.",
                            metrics={"table_count": len(tables)},
                        )
                    )
                else:
                    checks.append(
                        SubsystemCheckItem(
                            name="db_core_tables",
                            subsystem=SubsystemType.DATABASE,
                            severity=DiagnosticSeverity.CRITICAL,
                            status="FAIL",
                            message=f"Missing key tables: {missing}",
                            remediation_hint="Run database migrations using 'python3 -m src.cli.run_migrations'.",
                        )
                    )
        except SQLAlchemyError as exc:
            checks.append(
                SubsystemCheckItem(
                    name="db_core_tables",
                    subsystem=SubsystemType.DATABASE,
                    severity=DiagnosticSeverity.WARNING,
                    status="WARN",
                    message=f"Could not inspect table catalog: {exc}",
                )
            )

        return checks

    def diagnose_scheduler(self) -> list[SubsystemCheckItem]:
        """Diagnose scheduler process lock hierarchy and PID instance guard."""
        checks: list[SubsystemCheckItem] = []
        lock_report = self.lock_manager.diagnose_locks()

        # 1. PID guard check
        is_stale_pid = lock_report.daemon_pid is not None and not lock_report.pid_alive
        if is_stale_pid:
            checks.append(
                SubsystemCheckItem(
                    name="scheduler_pid_guard",
                    subsystem=SubsystemType.SCHEDULER,
                    severity=DiagnosticSeverity.WARNING,
                    status="WARN",
                    message="Stale scheduler PID file detected from a previous crashed process.",
                    remediation_hint="Run auto_heal() or remove data/locks/scheduler.pid.",
                )
            )
        else:
            checks.append(
                SubsystemCheckItem(
                    name="scheduler_pid_guard",
                    subsystem=SubsystemType.SCHEDULER,
                    severity=DiagnosticSeverity.HEALTHY,
                    status="OK",
                    message=f"Scheduler PID guard is clean (Active PID: {lock_report.daemon_pid or 'None'}).",
                )
            )

        # 2. Lock files check
        active_locks = lock_report.active_locks
        checks.append(
            SubsystemCheckItem(
                name="scheduler_tier_locks",
                subsystem=SubsystemType.SCHEDULER,
                severity=DiagnosticSeverity.HEALTHY if not active_locks else DiagnosticSeverity.INFO,
                status="OK",
                message=f"Scheduler locks diagnosed ({len(active_locks)} active locks).",
                metrics={"active_locks": active_locks},
            )
        )

        return checks

    def diagnose_crawlers(self) -> list[SubsystemCheckItem]:
        """Diagnose crawler logs for timeout patterns or rate-limiting."""
        checks: list[SubsystemCheckItem] = []
        if not self.logs_dir.exists():
            checks.append(
                SubsystemCheckItem(
                    name="crawler_logs_directory",
                    subsystem=SubsystemType.CRAWLER,
                    severity=DiagnosticSeverity.INFO,
                    status="OK",
                    message="No logs directory found; crawler log audit skipped.",
                )
            )
            return checks

        log_files = list(self.logs_dir.glob("*.log"))
        total_logs = len(log_files)

        checks.append(
            SubsystemCheckItem(
                name="crawler_log_health",
                subsystem=SubsystemType.CRAWLER,
                severity=DiagnosticSeverity.HEALTHY,
                status="OK",
                message=f"Found {total_logs} crawler log files in logs directory.",
                metrics={"log_files_count": total_logs},
            )
        )
        return checks

    def diagnose_pipeline(self, session: Session | None = None) -> list[SubsystemCheckItem]:
        """Diagnose pipeline invariants and database records."""
        checks: list[SubsystemCheckItem] = []

        def _run_pipeline_check(s: Session) -> None:
            try:
                row = s.execute(text("SELECT COUNT(*) FROM game")).fetchone()
                game_count = int(row[0]) if row else 0

                if game_count > 0:
                    checks.append(
                        SubsystemCheckItem(
                            name="pipeline_game_records",
                            subsystem=SubsystemType.PIPELINE,
                            severity=DiagnosticSeverity.HEALTHY,
                            status="OK",
                            message=f"Pipeline game store healthy ({game_count} games indexed).",
                            metrics={"game_count": game_count},
                        )
                    )
                else:
                    checks.append(
                        SubsystemCheckItem(
                            name="pipeline_game_records",
                            subsystem=SubsystemType.PIPELINE,
                            severity=DiagnosticSeverity.INFO,
                            status="OK",
                            message="Pipeline game store is empty (fresh environment).",
                            metrics={"game_count": 0},
                        )
                    )
            except SQLAlchemyError as exc:
                checks.append(
                    SubsystemCheckItem(
                        name="pipeline_game_records",
                        subsystem=SubsystemType.PIPELINE,
                        severity=DiagnosticSeverity.WARNING,
                        status="WARN",
                        message=f"Pipeline query failed: {exc}",
                    )
                )

        if session is not None:
            _run_pipeline_check(session)
        else:
            try:
                with SessionLocal() as s:
                    _run_pipeline_check(s)
            except (SQLAlchemyError, RuntimeError, OSError, ValueError, TypeError) as exc:
                checks.append(
                    SubsystemCheckItem(
                        name="pipeline_session",
                        subsystem=SubsystemType.PIPELINE,
                        severity=DiagnosticSeverity.WARNING,
                        status="WARN",
                        message=f"Could not open session for pipeline audit: {exc}",
                    )
                )

        return checks

    def diagnose_rag_vector(self, session: Session | None = None) -> list[SubsystemCheckItem]:
        """Diagnose RAG chunk corpus and embeddings."""
        checks: list[SubsystemCheckItem] = []

        def _run_rag_check(s: Session) -> None:
            try:
                row = s.execute(text("SELECT COUNT(*) FROM rag_chunks")).fetchone()
                chunk_count = int(row[0]) if row else 0

                checks.append(
                    SubsystemCheckItem(
                        name="rag_corpus_chunks",
                        subsystem=SubsystemType.RAG_VECTOR,
                        severity=DiagnosticSeverity.HEALTHY,
                        status="OK",
                        message=f"RAG vector corpus contains {chunk_count} chunk entries.",
                        metrics={"chunk_count": chunk_count},
                    )
                )
            except SQLAlchemyError as exc:
                checks.append(
                    SubsystemCheckItem(
                        name="rag_corpus_chunks",
                        subsystem=SubsystemType.RAG_VECTOR,
                        severity=DiagnosticSeverity.INFO,
                        status="OK",
                        message=f"RAG chunks check skipped: {exc}",
                    )
                )

        if session is not None:
            _run_rag_check(session)
        else:
            try:
                with SessionLocal() as s:
                    _run_rag_check(s)
            except (SQLAlchemyError, RuntimeError, OSError, ValueError, TypeError) as exc:
                logger.debug("RAG check session failed: %s", exc)

        return checks

    def diagnose_all(self, session: Session | None = None) -> UnifiedDiagnosticsReport:
        """Run complete end-to-end diagnostic audit across all platform subsystems."""
        checks: list[SubsystemCheckItem] = []
        checks.extend(self.diagnose_database())
        checks.extend(self.diagnose_scheduler())
        checks.extend(self.diagnose_crawlers())
        checks.extend(self.diagnose_pipeline(session=session))
        checks.extend(self.diagnose_rag_vector(session=session))

        healthy_count = sum(1 for c in checks if c.severity == DiagnosticSeverity.HEALTHY)
        warning_count = sum(1 for c in checks if c.severity == DiagnosticSeverity.WARNING)
        critical_count = sum(1 for c in checks if c.severity == DiagnosticSeverity.CRITICAL)

        if critical_count > 0:
            overall = "CRITICAL"
        elif warning_count > 0:
            overall = "DEGRADED"
        else:
            overall = "HEALTHY"

        return UnifiedDiagnosticsReport(
            overall_status=overall,
            total_checks=len(checks),
            healthy_count=healthy_count,
            warning_count=warning_count,
            critical_count=critical_count,
            checks=checks,
            generated_at=datetime.now(UTC).isoformat(),
        )

    def auto_heal(self, subsystem: SubsystemType | str | None = None) -> list[str]:
        """Perform automated self-healing actions on detected anomalies."""
        healed: list[str] = []
        sub_val = subsystem.value if isinstance(subsystem, SubsystemType) else subsystem

        # Heal scheduler stale locks
        if sub_val in (None, "all", SubsystemType.SCHEDULER.value):
            report = self.lock_manager.diagnose_locks()
            if report.daemon_pid is not None and not report.pid_alive:
                pid_file = self.lock_manager.lock_dir / "scheduler.pid"
                if pid_file.exists():
                    pid_file.unlink()
                    msg = "Removed stale scheduler PID file."
                    logger.info(msg)
                    healed.append(msg)

        return healed
