"""Synthetic fault-injection tests to prove that failures in any gate properly block certification."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from src.certification.context import CertificationContext
from src.certification.gates.data_integrity import DataIntegrityGate
from src.certification.gates.idempotency import UpsertIdempotencyGate
from src.certification.gates.locks import SchedulerLocksGate
from src.certification.gates.schema import SchemaMigrationGate
from src.certification.gates.transaction import TransactionAtomicityGate
from src.certification.models import GateResult, GateStatus
from src.certification.registry import GateRegistry
from src.certification.runner import CertificationRunner

if TYPE_CHECKING:
    from pathlib import Path


def test_synthetic_schema_drift_failure(tmp_path: Path) -> None:
    """Inject synthetic schema drift and verify G01 returns FAIL."""
    ctx = CertificationContext(artifact_dir=tmp_path)
    gate = SchemaMigrationGate()

    mock_report = MagicMock()
    mock_report.is_synced = False
    mock_report.drift_count = 3

    mock_mig = MagicMock()
    mock_mig.pending_count = 0
    mock_mig.applied_count = 10

    with (
        patch("src.db.migration_engine.MigrationEngine.get_status", return_value=mock_mig),
        patch("src.db.drift_detector.SchemaDriftDetector.detect_drift", return_value=mock_report),
    ):
        res = gate.run(ctx)
        assert res.status == GateStatus.FAIL
        assert "3 schema drift" in (res.message or "")


def test_synthetic_rollback_leak_failure(tmp_path: Path) -> None:
    """Inject synthetic transaction rollback leak and verify G02 returns FAIL."""
    ctx = CertificationContext(artifact_dir=tmp_path)
    gate = TransactionAtomicityGate()

    # Simulate query returning leaked rows after rollback
    with patch("sqlalchemy.orm.query.Query.count", return_value=1):
        res = gate.run(ctx)
        assert res.status == GateStatus.FAIL
        assert "Rollback failed" in (res.message or "")


def test_synthetic_idempotency_delta_failure(tmp_path: Path) -> None:
    """Inject synthetic idempotency violation and verify G03 returns FAIL."""
    ctx = CertificationContext(artifact_dir=tmp_path)
    gate = UpsertIdempotencyGate()

    with patch("src.repositories.player_basic_repository.PlayerBasicRepository.upsert_players"):
        with patch("sqlalchemy.orm.query.Query.count", side_effect=[2, 4]):
            res = gate.run(ctx)
            assert res.status == GateStatus.FAIL
            assert "Idempotency violation" in (res.message or "")


def test_synthetic_data_invariant_negative_stat_failure(tmp_path: Path) -> None:
    """Inject synthetic negative batting records and verify G05 returns FAIL."""
    ctx = CertificationContext(artifact_dir=tmp_path)
    gate = DataIntegrityGate()

    mock_invalid_record = MagicMock()
    with patch("sqlalchemy.orm.Session.scalars") as mock_scalars:
        mock_scalars.return_value.all.side_effect = [[mock_invalid_record], []]
        res = gate.run(ctx)
        assert res.status == GateStatus.FAIL
        assert "critical data invariant" in (res.message or "")


def test_synthetic_concurrent_lock_double_acquire_failure(tmp_path: Path) -> None:
    """Inject synthetic failure where double lock acquire does not raise an error."""
    ctx = CertificationContext(artifact_dir=tmp_path)
    gate = SchedulerLocksGate()

    # Simulate ProcessLock allowing double acquire
    with patch("src.utils.lock.ProcessLock.acquire", return_value=True):
        res = gate.run(ctx)
        assert res.status == GateStatus.FAIL
        assert "Mutual exclusion failed" in (res.message or "")


def test_runner_blocks_release_on_single_gate_failure(tmp_path: Path) -> None:
    """Verify that a single failing gate causes overall certification status to be NOT_CERTIFIED."""
    ctx = CertificationContext(artifact_dir=tmp_path, target="production")

    failing_gate = MagicMock()
    failing_gate.gate_id = "test_failing_gate"
    failing_gate.name = "Test Failing Gate"
    failing_gate.blocking = True
    failing_gate.dependencies = []
    failing_gate.run.return_value = GateResult(
        gate_id="test_failing_gate",
        name="Test Failing Gate",
        status=GateStatus.FAIL,
        duration_ms=10.0,
        blocking=True,
        message="Synthetic critical failure",
    )

    registry = GateRegistry()
    registry.register(failing_gate)

    runner = CertificationRunner(registry=registry)
    report = runner.run_certification(ctx)

    assert report.status == "NOT_CERTIFIED"
    assert report.blocking_failures == 1
