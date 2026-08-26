"""Unit tests for src.diagnostics.engine."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.diagnostics.dto import DiagnosticSeverity, SubsystemType
from src.diagnostics.engine import SystemDiagnosticsEngine
from src.models.base import Base
from src.scheduler.lock_manager import SchedulerLockManager

if TYPE_CHECKING:
    from pathlib import Path


def test_diagnose_database_success() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    diag_engine = SystemDiagnosticsEngine(engine=engine)
    checks = diag_engine.diagnose_database(engine)

    assert len(checks) >= 2
    assert any(c.name == "db_connectivity" and c.severity == DiagnosticSeverity.HEALTHY for c in checks)
    assert any(c.name == "db_core_tables" and c.severity == DiagnosticSeverity.HEALTHY for c in checks)


def test_diagnose_scheduler_and_auto_heal(tmp_path: Path) -> None:
    lock_mgr = SchedulerLockManager(lock_dir=tmp_path)
    pid_file = tmp_path / "scheduler.pid"
    # Write a dead PID (99999999)
    pid_file.write_text("99999999")

    diag_engine = SystemDiagnosticsEngine(lock_manager=lock_mgr)
    checks = diag_engine.diagnose_scheduler()

    assert any(c.name == "scheduler_pid_guard" and c.severity == DiagnosticSeverity.WARNING for c in checks)

    # Run auto-heal
    healed = diag_engine.auto_heal(SubsystemType.SCHEDULER)
    assert len(healed) == 1
    assert "Removed stale scheduler PID" in healed[0]
    assert not pid_file.exists()


def test_diagnose_all_report() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    diag_engine = SystemDiagnosticsEngine(engine=engine)
    with Session(engine) as session:
        report = diag_engine.diagnose_all(session=session)

        assert report.total_checks > 0
        assert report.overall_status in ("HEALTHY", "DEGRADED", "CRITICAL")
        assert report.healthy_count >= 1
