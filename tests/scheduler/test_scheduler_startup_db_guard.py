"""Startup database-target guard tests.

Regression for the 2026-09-22 incident: ``DATABASE_URL`` moved from Oracle to
PostgreSQL while a long-lived scheduler kept its old engine. Nothing logged the
resolved target, so the mismatch was invisible for a week and surfaced as a
quality-gate failure instead.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from src.scheduler.registry import (
    DB_STARTUP_PROBE_TIMEOUT_SECONDS,
    _log_resolved_database_target,
    _masked_database_target,
)


def test_masked_target_hides_password() -> None:
    """Credentials must never reach the log."""
    masked = _masked_database_target("postgresql://user:sup3rsecret@db.example:5432/bega_prod")

    assert "sup3rsecret" not in masked
    assert "user" in masked
    assert "db.example:5432" in masked
    assert masked.endswith("/bega_prod")


def test_masked_target_reports_unparseable_url() -> None:
    """A malformed URL must degrade to a placeholder, not raise."""
    assert _masked_database_target("not a url") == "<unparseable DATABASE_URL>"


def test_startup_guard_logs_target_and_passes_for_sqlite(
    tmp_path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A reachable database logs the resolved target and the success line."""
    db_path = tmp_path / "probe.db"
    monkeypatch.setattr("src.db.engine.DATABASE_URL", f"sqlite:///{db_path}")
    caplog.set_level(logging.INFO, logger="src.scheduler.registry")

    _log_resolved_database_target()

    messages = [record.getMessage() for record in caplog.records]
    assert any("Resolved database target:" in message for message in messages)
    assert "Database connectivity check passed" in messages


def test_startup_guard_reports_unbuildable_probe_engine(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A driver that cannot build an engine is reported but must not stop startup."""
    monkeypatch.setattr(
        "src.db.engine.DATABASE_URL",
        "postgresql://user:secret@db.example:5432/bega_prod",
    )

    def _boom(*_args: object, **_kwargs: object) -> object:
        raise OperationalError("SELECT 1", {}, Exception("no driver installed"))

    monkeypatch.setattr("sqlalchemy.create_engine", _boom)
    caplog.set_level(logging.ERROR, logger="src.scheduler.registry")

    _log_resolved_database_target()  # must not raise

    errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1
    assert "Could not build a probe engine" in errors[0]
    assert "secret" not in errors[0]


def test_startup_guard_reports_unreachable_database(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """An unreachable database is reported loudly but must not stop startup."""
    monkeypatch.setattr(
        "src.db.engine.DATABASE_URL",
        "postgresql://user:secret@unreachable.invalid:5432/bega_prod",
    )

    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.side_effect = OperationalError(
        "SELECT 1", {}, Exception("connection refused")
    )
    monkeypatch.setattr("sqlalchemy.create_engine", lambda *_a, **_kw: engine)
    caplog.set_level(logging.ERROR, logger="src.scheduler.registry")

    _log_resolved_database_target()  # must not raise

    errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1
    assert "did not answer SELECT 1" in errors[0]
    assert "secret" not in errors[0]
    engine.dispose.assert_called_once_with()


def test_postgres_probe_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    """A remote PostgreSQL probe must carry a connect timeout so startup cannot hang."""
    captured: dict[str, object] = {}

    def _capture(_url: object, **kwargs: object) -> MagicMock:
        captured.update(kwargs)
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value.execute.side_effect = OperationalError(
            "SELECT 1", {}, Exception("connection refused")
        )
        return engine

    monkeypatch.setattr(
        "src.db.engine.DATABASE_URL",
        "postgresql://user:pw@db.example:5432/bega_prod",
    )
    monkeypatch.setattr("sqlalchemy.create_engine", _capture)

    _log_resolved_database_target()

    assert captured["connect_args"] == {"connect_timeout": DB_STARTUP_PROBE_TIMEOUT_SECONDS}


def test_main_invokes_the_startup_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    """The guard must actually run on the scheduler startup path."""
    from src.scheduler import registry

    guard = MagicMock()
    monkeypatch.setattr(registry, "_log_resolved_database_target", guard)
    monkeypatch.setattr(registry, "_ensure_single_scheduler_instance", MagicMock())

    with (
        patch.object(registry, "init_sentry"),
        patch.object(registry, "start_metrics_server"),
        patch.object(registry, "_start_scheduler"),
    ):
        registry.main([])

    guard.assert_called_once_with()
