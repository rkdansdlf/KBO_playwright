"""Unit tests for src.diagnostics.engine."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.diagnostics import engine as diag_engine_module
from src.diagnostics.dto import AutoHealResult, DiagnosticSeverity, SubsystemType
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
    pid_file.write_text("99999999", encoding="utf-8")

    diag_engine = SystemDiagnosticsEngine(lock_manager=lock_mgr)
    checks = diag_engine.diagnose_scheduler()

    assert any(c.name == "scheduler_pid_guard" and c.severity == DiagnosticSeverity.WARNING for c in checks)

    # Run auto-heal
    result = diag_engine.auto_heal(SubsystemType.SCHEDULER)
    assert len(result.actions) == 1
    assert "Removed stale scheduler PID" in result.actions[0]
    assert result.addressed == ("scheduler",)
    assert result.unsupported == ()
    assert not pid_file.exists()


def test_a_subsystem_without_a_remediation_is_reported_as_unsupported() -> None:
    """`auto_heal('database')` must not look like 'nothing needed healing'."""
    engine = SystemDiagnosticsEngine()

    result = engine.auto_heal(SubsystemType.DATABASE)

    assert result.actions == ()
    assert result.addressed == ()
    assert result.unsupported == (SubsystemType.DATABASE.value,)


def test_all_names_every_subsystem_that_cannot_be_healed(tmp_path: Path, monkeypatch) -> None:
    """`all` addresses what it can repair and names the rest explicitly."""
    monkeypatch.setattr(diag_engine_module, "circuit_registry", MagicMock(get_all_stats=MagicMock(return_value=[])))
    engine = SystemDiagnosticsEngine(lock_manager=SchedulerLockManager(lock_dir=tmp_path))

    result = engine.auto_heal("all")

    assert set(result.addressed) == {"scheduler", "crawler"}
    assert set(result.unsupported) == {
        SubsystemType.DATABASE.value,
        SubsystemType.PIPELINE.value,
        SubsystemType.API_GATEWAY.value,
        SubsystemType.RAG_VECTOR.value,
    }
    assert set(result.addressed).isdisjoint(result.unsupported)


def test_an_unknown_subsystem_name_is_reported_rather_than_ignored() -> None:
    """A name that is not a subsystem is surfaced, not silently skipped."""
    result = SystemDiagnosticsEngine().auto_heal("not-a-subsystem")

    assert result.addressed == ()
    assert result.unsupported == ("not-a-subsystem",)


def test_auto_heal_result_is_serializable() -> None:
    """The heal outcome survives JSON serialization for automation consumers."""
    payload = AutoHealResult(actions=("a",), addressed=("crawler",), unsupported=("database",)).to_dict()

    assert payload == {"actions": ["a"], "addressed": ["crawler"], "unsupported": ["database"]}


def test_diagnose_all_report() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    diag_engine = SystemDiagnosticsEngine(engine=engine)
    with Session(engine) as session:
        report = diag_engine.diagnose_all(session=session)

        assert report.total_checks > 0
        assert report.overall_status in ("HEALTHY", "DEGRADED", "CRITICAL")
        assert report.healthy_count >= 1
