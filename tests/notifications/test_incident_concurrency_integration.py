"""Concurrency integration tests for the incident ledger.

The deterministic interleavings live in ``test_incident_manager.py``; this file
proves the invariant under *real* parallelism with independent sessions.

Backend selection (matching the repo's integration convention):

* ``KBO_E2E_DATABASE_URL`` / a non-SQLite ``DATABASE_URL`` (the
  ``integration-test-postgres`` job) exercises genuine concurrent writers under
  PostgreSQL Read Committed — the only backend that can expose a lost update.
* Otherwise an isolated file-based SQLite database is used, with ``BEGIN
  IMMEDIATE`` + WAL so writers serialize instead of deadlocking.
"""

from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine, delete, event, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.models.base import Base
from src.models.notification_incident import INCIDENT_STATE_RECOVERED, NotificationIncident
from src.notifications.alert_dto import AlertEvent, AlertSeverity, AlertSource
from src.notifications.publisher import AlertPublisher

pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)

PREFIX = "concurrency-probe"
BASE_TIME = datetime(2026, 9, 25, 4, 45, 0)
WORKERS = 12


def _candidate_url() -> str | None:
    url = os.getenv("KBO_E2E_DATABASE_URL") or os.getenv("DATABASE_URL") or ""
    if url and not url.startswith("sqlite"):
        return url
    return None


def _make_engine(tmp_path) -> Engine:
    url = _candidate_url()
    if url:
        engine = create_engine(url, pool_pre_ping=True)
    else:
        db_path = tmp_path / "incident_concurrency.db"
        engine = create_engine(
            f"sqlite:///{db_path}",
            connect_args={"check_same_thread": False, "timeout": 30},
        )

        @event.listens_for(engine, "connect")
        def _pragmas(dbapi_connection, _record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.close()

        @event.listens_for(engine, "begin")
        def _begin_immediate(connection):
            connection.exec_driver_sql("BEGIN IMMEDIATE")

    Base.metadata.create_all(bind=engine)
    return engine


@pytest.fixture
def engine(tmp_path):
    eng = _make_engine(tmp_path)
    try:
        yield eng
    finally:
        with eng.begin() as connection:
            connection.execute(
                delete(NotificationIncident).where(NotificationIncident.incident_key.like(f"{PREFIX}%")),
            )
        eng.dispose()


@pytest.fixture
def session_factory(engine):
    return sessionmaker(bind=engine, expire_on_commit=False)


def _event(key: str, severity: AlertSeverity = AlertSeverity.ERROR) -> AlertEvent:
    return AlertEvent(
        source=AlertSource.QUALITY,
        component="daily",
        severity=severity,
        title="concurrency probe",
        message="probe",
        incident_key=key,
        occurred_at=BASE_TIME,
        metadata={"probe": key},
    )


def _run_concurrently(worker, count: int = WORKERS) -> tuple[list, list]:
    barrier = threading.Barrier(count)
    results: list = []
    errors: list = []
    lock = threading.Lock()

    def _wrapped(index: int) -> None:
        barrier.wait()
        try:
            outcome = worker(index)
        except Exception as exc:
            logger.exception("concurrency probe worker %s failed", index)
            with lock:
                errors.append(exc)
        else:
            with lock:
                results.append(outcome)

    with ThreadPoolExecutor(max_workers=count) as pool:
        list(pool.map(_wrapped, range(count)))
    return results, errors


class TestConcurrentPublish:
    def test_concurrent_first_publish_creates_one_incident(self, session_factory) -> None:
        key = f"{PREFIX}:single"

        def worker(_index: int) -> None:
            with session_factory() as session:
                AlertPublisher(session).publish(_event(key), now=BASE_TIME, dry_run=True)
                session.commit()

        _results, errors = _run_concurrently(worker)

        assert errors == []
        with session_factory() as session:
            rows = list(
                session.execute(
                    select(NotificationIncident).where(NotificationIncident.incident_key == key),
                ).scalars(),
            )
        assert len(rows) == 1

    def test_concurrent_publish_does_not_lose_occurrence_count(self, session_factory) -> None:
        key = f"{PREFIX}:count"

        def worker(_index: int) -> None:
            with session_factory() as session:
                AlertPublisher(session).publish(_event(key), now=BASE_TIME, dry_run=True)
                session.commit()

        _results, errors = _run_concurrently(worker)

        assert errors == []
        with session_factory() as session:
            row = session.execute(
                select(NotificationIncident).where(NotificationIncident.incident_key == key),
            ).scalar_one()
        assert row.occurrence_count == WORKERS

    def test_concurrent_publish_has_no_integrity_error(self, session_factory) -> None:
        key = f"{PREFIX}:no-error"

        def worker(index: int) -> str:
            with session_factory() as session:
                transition = AlertPublisher(session).publish(
                    _event(key, AlertSeverity.CRITICAL if index % 2 else AlertSeverity.ERROR),
                    now=BASE_TIME,
                    dry_run=True,
                )
                session.commit()
                return transition.decision.value

        results, errors = _run_concurrently(worker)

        assert errors == []
        assert len(results) == WORKERS


class TestConcurrentResolve:
    def test_concurrent_resolve_is_idempotent(self, session_factory) -> None:
        key = f"{PREFIX}:resolve"
        with session_factory() as session:
            AlertPublisher(session).publish(_event(key), now=BASE_TIME, dry_run=True)
            session.commit()

        resolve_at = BASE_TIME + timedelta(minutes=5)

        def worker(_index: int) -> bool:
            with session_factory() as session:
                transition = AlertPublisher(session).resolve(key, now=resolve_at, dry_run=True)
                session.commit()
                return transition is not None

        winners, errors = _run_concurrently(worker)

        assert errors == []
        assert sum(1 for won in winners if won) == 1
        with session_factory() as session:
            row = session.execute(
                select(NotificationIncident).where(NotificationIncident.incident_key == key),
            ).scalar_one()
        assert row.state == INCIDENT_STATE_RECOVERED
        assert row.resolved_at == resolve_at

    def test_publish_during_resolve_has_deterministic_state(self, session_factory) -> None:
        key = f"{PREFIX}:race"
        with session_factory() as session:
            AlertPublisher(session).publish(_event(key), now=BASE_TIME, dry_run=True)
            session.commit()

        def worker(index: int) -> None:
            with session_factory() as session:
                publisher = AlertPublisher(session)
                if index % 2:
                    publisher.resolve(key, now=BASE_TIME + timedelta(minutes=1), dry_run=True)
                else:
                    publisher.publish(_event(key), now=BASE_TIME + timedelta(minutes=1), dry_run=True)
                session.commit()

        _results, errors = _run_concurrently(worker, count=10)

        assert errors == []
        with session_factory() as session:
            row = session.execute(
                select(NotificationIncident).where(NotificationIncident.incident_key == key),
            ).scalar_one()
        assert row.state in {"OPEN", "ACKNOWLEDGED", "RECOVERED"}
        assert row.occurrence_count >= 1
