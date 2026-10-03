"""Tests for the guarded notification incident operator actions.

Kept in its own file rather than alongside the read-only incident tests so the
two surfaces can be verified independently.

The ledger has no command for acknowledging or recovering an incident, so these
run against a real SQLite ledger: the point of the change is the default-deny
guard plus the SQL, and a mocked session would verify neither.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from src.cli.incidents_operator import main as operator_main
from src.models.notification_delivery import NotificationDelivery
from src.models.notification_incident import (
    INCIDENT_STATE_ACKNOWLEDGED,
    INCIDENT_STATE_OPEN,
    INCIDENT_STATE_RECOVERED,
    NotificationIncident,
)
from src.notifications.publisher import AlertPublisher
from src.notifications.recorder import DeliveryRecorder

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

BASE = datetime(2026, 10, 3, 12, 0, 0)
KEY = "drift:selector:schedule"


@pytest.fixture(autouse=True)
def _no_real_delivery(monkeypatch) -> None:
    """Keep every test off the real transports."""
    monkeypatch.setenv("ALERT_DRY_RUN", "1")


@pytest.fixture
def ledger(tmp_path: Path, monkeypatch) -> Iterator[sessionmaker]:
    """Point the operator CLI at a throwaway SQLite ledger."""
    engine = create_engine(f"sqlite:///{tmp_path / 'operator.db'}")
    NotificationIncident.__table__.create(engine)
    NotificationDelivery.__table__.create(engine)
    factory = sessionmaker(bind=engine)

    @contextmanager
    def _session() -> Iterator:
        session = factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr("src.cli.incidents_operator.get_db_session", _session)
    # DeliveryRecorder owns an *independent* session by design, so point it at the
    # same file rather than letting it fall back to the production SessionLocal.
    monkeypatch.setattr(
        "src.cli.incidents_operator.AlertPublisher",
        lambda session: AlertPublisher(session, recorder=DeliveryRecorder(factory)),
    )
    yield factory
    engine.dispose()


def _add(factory: sessionmaker, key: str, *, state: str = INCIDENT_STATE_OPEN) -> None:
    # SQLAlchemy 2.0 `with Session()` closes without committing, which is exactly
    # why src.db.engine.get_db_session commits explicitly. Same requirement here.
    with factory() as session:
        session.add(
            NotificationIncident(
                incident_key=key,
                source="drift",
                component="selector:schedule",
                severity="ERROR",
                state=state,
                title=f"title {key}",
                message="",
                details_hash="",
                occurrence_count=3,
                notification_count=2,
                first_opened_at=BASE,
                last_seen_at=BASE,
                resolved_at=BASE if state == INCIDENT_STATE_RECOVERED else None,
            ),
        )
        session.commit()


def _state_of(factory: sessionmaker, key: str) -> str | None:
    # incident_key is a unique column, not the integer primary key, so
    # session.get() is the wrong lookup here.
    with factory() as session:
        stmt = select(NotificationIncident.state).where(NotificationIncident.incident_key == key)
        return session.execute(stmt).scalar_one_or_none()


class TestAck:
    def test_missing_key_returns_not_found(self, ledger, monkeypatch, capsys) -> None:
        monkeypatch.setenv("KBO_ALLOW_INCIDENT_MUTATION", "1")
        assert operator_main(["ack", "nope:missing", "--apply"]) == 1
        assert "not found" in capsys.readouterr().err

    def test_recovered_cannot_be_acknowledged(self, ledger, monkeypatch, capsys) -> None:
        monkeypatch.setenv("KBO_ALLOW_INCIDENT_MUTATION", "1")
        _add(ledger, "done:thing", state=INCIDENT_STATE_RECOVERED)
        assert operator_main(["ack", "done:thing", "--apply"]) == 2
        assert "cannot acknowledge" in capsys.readouterr().err
        assert _state_of(ledger, "done:thing") == INCIDENT_STATE_RECOVERED

    def test_preview_validates_without_writing(self, ledger, monkeypatch, capsys) -> None:
        monkeypatch.delenv("KBO_ALLOW_INCIDENT_MUTATION", raising=False)
        _add(ledger, KEY)
        assert operator_main(["ack", KEY]) == 0
        assert "would ack" in capsys.readouterr().out
        assert _state_of(ledger, KEY) == INCIDENT_STATE_OPEN

    def test_apply_without_env_is_denied_and_does_not_write(self, ledger, monkeypatch, capsys) -> None:
        monkeypatch.delenv("KBO_ALLOW_INCIDENT_MUTATION", raising=False)
        _add(ledger, KEY)
        assert operator_main(["ack", KEY, "--apply"]) == 3
        assert "KBO_ALLOW_INCIDENT_MUTATION" in capsys.readouterr().err
        assert _state_of(ledger, KEY) == INCIDENT_STATE_OPEN

    def test_env_without_apply_stays_a_preview(self, ledger, monkeypatch, capsys) -> None:
        monkeypatch.setenv("KBO_ALLOW_INCIDENT_MUTATION", "1")
        _add(ledger, KEY)
        assert operator_main(["ack", KEY]) == 0
        assert "would ack" in capsys.readouterr().out
        assert _state_of(ledger, KEY) == INCIDENT_STATE_OPEN

    def test_apply_with_both_gates_transitions(self, ledger, monkeypatch, capsys) -> None:
        monkeypatch.setenv("KBO_ALLOW_INCIDENT_MUTATION", "1")
        _add(ledger, KEY)
        assert operator_main(["ack", KEY, "--apply"]) == 0
        assert f"ack {KEY}" in capsys.readouterr().out
        assert _state_of(ledger, KEY) == INCIDENT_STATE_ACKNOWLEDGED

    def test_json_reports_applied_flag(self, ledger, monkeypatch, capsys) -> None:
        monkeypatch.setenv("KBO_ALLOW_INCIDENT_MUTATION", "1")
        _add(ledger, KEY)
        assert operator_main(["ack", KEY, "--apply", "--json"]) == 0
        payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
        assert payload["action"] == "ack"
        assert payload["applied"] is True
        assert payload["status"].startswith(INCIDENT_STATE_ACKNOWLEDGED)


class TestResolve:
    def test_missing_key_returns_not_found(self, ledger, monkeypatch, capsys) -> None:
        monkeypatch.setenv("KBO_ALLOW_INCIDENT_MUTATION", "1")
        assert operator_main(["resolve", "nope:missing", "--apply"]) == 1
        assert "not found" in capsys.readouterr().err

    def test_recovered_cannot_be_resolved_again(self, ledger, monkeypatch, capsys) -> None:
        monkeypatch.setenv("KBO_ALLOW_INCIDENT_MUTATION", "1")
        _add(ledger, "done:thing", state=INCIDENT_STATE_RECOVERED)
        assert operator_main(["resolve", "done:thing", "--apply"]) == 2
        assert "not active" in capsys.readouterr().err

    def test_acknowledged_is_still_resolvable(self, ledger, monkeypatch) -> None:
        monkeypatch.setenv("KBO_ALLOW_INCIDENT_MUTATION", "1")
        _add(ledger, "ack:thing", state=INCIDENT_STATE_ACKNOWLEDGED)
        assert operator_main(["resolve", "ack:thing", "--apply"]) == 0
        assert _state_of(ledger, "ack:thing") == INCIDENT_STATE_RECOVERED

    def test_preview_does_not_resolve(self, ledger, monkeypatch, capsys) -> None:
        monkeypatch.delenv("KBO_ALLOW_INCIDENT_MUTATION", raising=False)
        _add(ledger, KEY)
        assert operator_main(["resolve", KEY]) == 0
        assert "would resolve" in capsys.readouterr().out
        assert _state_of(ledger, KEY) == INCIDENT_STATE_OPEN

    def test_apply_without_env_is_denied(self, ledger, monkeypatch, capsys) -> None:
        monkeypatch.delenv("KBO_ALLOW_INCIDENT_MUTATION", raising=False)
        _add(ledger, KEY)
        assert operator_main(["resolve", KEY, "--apply"]) == 3
        assert "KBO_ALLOW_INCIDENT_MUTATION" in capsys.readouterr().err
        assert _state_of(ledger, KEY) == INCIDENT_STATE_OPEN

    def test_apply_resolves_and_sets_timestamp(self, ledger, monkeypatch, capsys) -> None:
        monkeypatch.setenv("KBO_ALLOW_INCIDENT_MUTATION", "1")
        _add(ledger, KEY)
        assert operator_main(["resolve", KEY, "--apply"]) == 0
        assert "RECOVERED" in capsys.readouterr().out
        with ledger() as session:
            stmt = select(NotificationIncident).where(NotificationIncident.incident_key == KEY)
            incident = session.execute(stmt).scalar_one()
            assert incident.state == INCIDENT_STATE_RECOVERED
            assert incident.resolved_at is not None
