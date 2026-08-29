"""Unit tests for all 9 individual KBO Production Certification gates."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.certification.context import CertificationContext
from src.certification.gates.api import ApiGatewayGate
from src.certification.gates.data_integrity import DataIntegrityGate
from src.certification.gates.idempotency import UpsertIdempotencyGate
from src.certification.gates.locks import SchedulerLocksGate
from src.certification.gates.schema import SchemaMigrationGate
from src.certification.gates.security_drift import SecurityDriftGate
from src.certification.gates.transaction import TransactionAtomicityGate
from src.certification.gates.vector_rag import VectorRagGate
from src.certification.gates.websocket import WebSocketStreamGate
from src.certification.models import GateStatus
from src.models.base import Base

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path


@pytest.fixture(name="cert_db_factory", scope="module")
def _cert_db_factory(tmp_path_factory: pytest.TempPathFactory) -> Generator[Any, None, None]:
    """Provide a dedicated test SQLite database with all tables created."""
    db_dir = tmp_path_factory.mktemp("cert_test_db")
    db_file = db_dir / "cert_test.db"

    test_engine = create_engine(f"sqlite:///{db_file}")
    Base.metadata.create_all(bind=test_engine)
    session_factory = sessionmaker(bind=test_engine, autoflush=False, autocommit=False, expire_on_commit=False)

    @contextmanager
    def mock_get_db_session() -> Generator[Session, None, None]:
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    yield mock_get_db_session, test_engine


@pytest.fixture(autouse=True)
def _setup_mock_db(cert_db_factory: tuple[Any, Any]) -> Generator[None, None, None]:
    """Patch get_db_session and Engine to use isolated test DB."""
    mock_session_fn, test_engine = cert_db_factory
    with (
        patch("src.certification.gates.schema.get_db_session", mock_session_fn),
        patch("src.certification.gates.schema.Engine", test_engine),
        patch("src.certification.gates.vector_rag.get_db_session", mock_session_fn),
        patch("src.certification.gates.data_integrity.get_db_session", mock_session_fn),
        patch("src.db.engine.get_db_session", mock_session_fn),
    ):
        yield


def test_g01_schema_migration_gate(tmp_path: Path) -> None:
    """Test G01 Schema & Migration Parity Gate."""
    ctx = CertificationContext(artifact_dir=tmp_path, target="local")
    gate = SchemaMigrationGate()
    res = gate.run(ctx)
    assert res.gate_id == "schema_migration"
    assert res.status in {GateStatus.PASS, GateStatus.WARN}
    assert "total_db_tables" in res.metrics


def test_g02_transaction_atomicity_gate(tmp_path: Path) -> None:
    """Test G02 Transaction Atomicity Gate."""
    ctx = CertificationContext(artifact_dir=tmp_path)
    gate = TransactionAtomicityGate()
    res = gate.run(ctx)
    assert res.gate_id == "transaction_atomicity"
    assert res.status == GateStatus.PASS
    assert res.metrics["post_rollback_leaked_rows"] == 0


def test_g03_upsert_idempotency_gate(tmp_path: Path) -> None:
    """Test G03 Upsert Idempotency Gate."""
    ctx = CertificationContext(artifact_dir=tmp_path)
    gate = UpsertIdempotencyGate()
    res = gate.run(ctx)
    assert res.gate_id == "upsert_idempotency"
    assert res.status == GateStatus.PASS
    assert res.metrics["row_delta_after_repeat"] == 0


def test_g04_vector_rag_gate(tmp_path: Path) -> None:
    """Test G04 Oracle Native Vector & RAG Gate."""
    ctx = CertificationContext(artifact_dir=tmp_path)
    gate = VectorRagGate()
    res = gate.run(ctx)
    assert res.gate_id == "oracle_vector_rag"
    assert res.status in {GateStatus.PASS, GateStatus.WARN}
    assert "expected_dimension" in res.metrics


def test_g05_data_integrity_gate(tmp_path: Path) -> None:
    """Test G05 Critical Data Invariants Gate."""
    ctx = CertificationContext(artifact_dir=tmp_path)
    gate = DataIntegrityGate()
    res = gate.run(ctx)
    assert res.gate_id == "data_invariants"
    assert res.status == GateStatus.PASS
    assert res.metrics["total_critical_violations"] == 0


def test_g06_api_gateway_gate(tmp_path: Path) -> None:
    """Test G06 API Gateway & Auth Contract Gate."""
    ctx = CertificationContext(artifact_dir=tmp_path)
    gate = ApiGatewayGate()
    res = gate.run(ctx)
    assert res.gate_id == "api_gateway"
    assert res.status == GateStatus.PASS
    assert res.metrics["health_status_code"] == 200


def test_g07_websocket_stream_gate(tmp_path: Path) -> None:
    """Test G07 Real-time WebSocket Stream Gate."""
    ctx = CertificationContext(artifact_dir=tmp_path)
    gate = WebSocketStreamGate()
    res = gate.run(ctx)
    assert res.gate_id == "websocket_stream"
    assert res.status == GateStatus.PASS
    assert "observed_latency_ms" in res.metrics


def test_g08_scheduler_locks_gate(tmp_path: Path) -> None:
    """Test G08 Scheduler Lock Safety Gate."""
    ctx = CertificationContext(artifact_dir=tmp_path)
    gate = SchedulerLocksGate()
    res = gate.run(ctx)
    assert res.gate_id == "scheduler_locks"
    assert res.status == GateStatus.PASS
    assert res.metrics["double_acquire_rejected"] is True


def test_g09_security_drift_gate(tmp_path: Path) -> None:
    """Test G09 Security Scanner & Schema Drift Gate."""
    ctx = CertificationContext(artifact_dir=tmp_path, target="local")
    gate = SecurityDriftGate()
    res = gate.run(ctx)
    assert res.gate_id == "security_drift"
    assert res.status == GateStatus.PASS
    assert res.metrics["critical_security_findings"] == 0
