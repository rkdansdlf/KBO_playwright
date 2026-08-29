"""Unit tests for certification engine, runner, context, secret redaction, and reporter."""

from __future__ import annotations

from contextlib import contextmanager
import json
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.certification.context import CertificationContext, redact_secrets
from src.certification.models import CertificationReport, GateResult, GateStatus
from src.certification.registry import GateRegistry
from src.certification.reporter import CertificationReporter
from src.cli.certify import (
    EXIT_CERTIFIED,
    main as certify_cli_main,
)
import src.models
from src.models.base import Base

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path


@pytest.fixture(name="cert_db_factory", scope="module")
def _cert_db_factory(tmp_path_factory: pytest.TempPathFactory) -> Generator[Any, None, None]:
    """Provide a dedicated test SQLite database with all tables created."""
    db_dir = tmp_path_factory.mktemp("cert_engine_test_db")
    db_file = db_dir / "cert_engine_test.db"

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
        patch("src.certification.historical.runner.get_db_session", mock_session_fn),
        patch("src.certification.historical.runner.Engine", test_engine),
        patch("src.db.engine.get_db_session", mock_session_fn),
    ):
        yield


def test_secret_redaction() -> None:
    """Test credential and token masking in logs and output."""
    raw_db = "oracle+oracledb://kbo_admin:mySecretPassword123@kbo_high"
    redacted = redact_secrets(raw_db)
    assert "mySecretPassword123" not in redacted
    assert "://***:***@" in redacted

    bot_token = "https://api.telegram.org/bot123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11/sendMessage"
    redacted_bot = redact_secrets(bot_token)
    assert "ABC-DEF1234ghIkl-zyx57W2v1u123ew11" not in redacted_bot


def test_certification_context_initialization(tmp_path: Path) -> None:
    """Test context initialization, redaction, and artifact directory creation."""
    ctx = CertificationContext(
        target="production",
        artifact_dir=tmp_path / "cert_artifacts",
    )
    assert ctx.artifact_dir.exists()
    assert ctx.run_id.startswith("cert_")
    assert ctx.git_revision != ""
    assert "password" not in ctx.database_url_redacted.lower()


def test_gate_registry_management() -> None:
    """Test default registry loading and filtering."""
    reg = GateRegistry.create_default()
    gates = reg.list_gates()
    assert len(gates) == 10

    # Filter single gate
    filtered = reg.list_gates("schema")
    assert len(filtered) == 1
    assert filtered[0].gate_id == "schema_migration"

    # Filter non-existent
    assert len(reg.list_gates("non_existent_gate")) == 0


def test_certification_reporter_formats(tmp_path: Path) -> None:
    """Test report summary formatting, ASCII card, and JSON export."""
    report = CertificationReport(
        schema_version="1.0",
        run_id="cert_test_123",
        target="local",
        git_revision="testsha",
        status="CERTIFIED",
        blocking_failures=0,
        warnings=0,
        total_duration_ms=123.45,
        gates=[
            GateResult(
                gate_id="schema_migration",
                name="Schema Migration",
                status=GateStatus.PASS,
                duration_ms=10.5,
                blocking=True,
                message="Schema is 100% synchronized",
            )
        ],
    )

    card = CertificationReporter.render_ascii_card(report)
    assert "KBO PLATFORM PRODUCTION CERTIFICATION REPORT" in card
    assert "CERTIFIED" in card
    assert "Schema Migration" in card

    out_file = tmp_path / "cert_report.json"
    saved = CertificationReporter.save_json_report(report, out_file)
    assert saved.exists()

    with saved.open("r", encoding="utf-8") as f:
        data = json.load(f)
        assert data["run_id"] == "cert_test_123"
        assert data["status"] == "CERTIFIED"
        assert len(data["gates"]) == 1


def test_certification_cli_execution_local(tmp_path: Path) -> None:
    """Test CLI execution in local development mode."""
    report_file = tmp_path / "cert_out.json"
    exit_code = certify_cli_main(
        [
            "--local",
            "--json-out",
            str(report_file),
        ]
    )

    assert exit_code == EXIT_CERTIFIED
    assert report_file.exists()

    with report_file.open("r", encoding="utf-8") as f:
        data = json.load(f)
        assert data["target"] == "local"
        assert data["status"] in {"CERTIFIED", "CERTIFIED_WITH_WARNINGS"}
