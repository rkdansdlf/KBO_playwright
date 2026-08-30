"""G01: Schema & Migration Parity Certification Gate."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, ClassVar

from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError

from src.certification.models import GateResult, GateStatus
from src.db.drift_detector import SchemaDriftDetector
from src.db.engine import Engine, get_db_session
from src.db.migration_engine import MigrationEngine

if TYPE_CHECKING:
    from src.certification.context import CertificationContext


class SchemaMigrationGate:
    """G01: Verifies schema parity across ORM and database, ensuring 0 pending migrations and 0 drift."""

    gate_id: str = "schema_migration"
    name: str = "Schema & Migration Parity"
    blocking: bool = True
    dependencies: ClassVar[list[str]] = []

    def run(self, context: CertificationContext) -> GateResult:
        """Run schema parity and migration inspection."""
        start = time.perf_counter()
        metrics: dict[str, Any] = {}
        evidence: dict[str, Any] = {}

        try:
            try:
                with get_db_session() as session:
                    target_engine = session.bind or Engine
                    inspector = inspect(target_engine)
                    db_tables = inspector.get_table_names()
            except (SQLAlchemyError, OSError):
                if context.target == "local":
                    # Fallback to local SQLite file
                    target_engine = create_engine("sqlite:///./data/kbo_dev.db")
                    inspector = inspect(target_engine)
                    db_tables = inspector.get_table_names()
                else:
                    raise

            dialect_name = target_engine.dialect.name.lower()
            if "oracle" in dialect_name:
                target_dialect = "oracle"
            elif "postgres" in dialect_name:
                target_dialect = "postgres"
            else:
                target_dialect = "sqlite"

            # 1. Inspect table counts
            metrics["total_db_tables"] = len(db_tables)
            evidence["tables_present"] = sorted(db_tables)[:10]

            # 2. Check pending migrations
            mig_engine = MigrationEngine()
            mig_status = mig_engine.get_status(target_engine, dialect=target_dialect)
            metrics["pending_migrations"] = mig_status.pending_count
            metrics["applied_migrations"] = mig_status.applied_count

            # 3. Check schema drift
            drift_detector = SchemaDriftDetector(target_engine, dialect=target_dialect)
            report = drift_detector.detect_drift()
            metrics["schema_drift_count"] = report.drift_count
            evidence["is_synced"] = report.is_synced

            duration_ms = (time.perf_counter() - start) * 1000.0

            if mig_status.pending_count > 0:
                status = GateStatus.WARN if context.target == "local" else GateStatus.FAIL
                return GateResult(
                    gate_id=self.gate_id,
                    name=self.name,
                    status=status,
                    duration_ms=duration_ms,
                    blocking=self.blocking,
                    metrics=metrics,
                    evidence=evidence,
                    message=f"{mig_status.pending_count} unapplied migration(s) detected",
                )

            if not report.is_synced and report.drift_count > 0:
                return GateResult(
                    gate_id=self.gate_id,
                    name=self.name,
                    status=GateStatus.FAIL,
                    duration_ms=duration_ms,
                    blocking=self.blocking,
                    metrics=metrics,
                    evidence=evidence,
                    message=f"{report.drift_count} schema drift item(s) detected",
                )

            return GateResult(
                gate_id=self.gate_id,
                name=self.name,
                status=GateStatus.PASS,
                duration_ms=duration_ms,
                blocking=self.blocking,
                metrics=metrics,
                evidence=evidence,
                message="Schema and migrations fully aligned (0 pending, 0 drift)",
            )

        except (SQLAlchemyError, RuntimeError, OSError, ValueError) as exc:
            duration_ms = (time.perf_counter() - start) * 1000.0
            err = context.redact(str(exc))
            return GateResult(
                gate_id=self.gate_id,
                name=self.name,
                status=GateStatus.FAIL,
                duration_ms=duration_ms,
                blocking=self.blocking,
                metrics=metrics,
                evidence={"error": err},
                message=f"Schema inspection failed: {err}",
            )


__all__ = [
    "SchemaMigrationGate",
]
