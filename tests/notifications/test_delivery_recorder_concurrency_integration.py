"""Concurrent delivery-audit recording under real parallelism.

The incident ledger's concurrency is covered by
``test_incident_concurrency_integration.py``. This module covers the other
append-only table: ``notification_deliveries``. Two properties matter, and
neither is observable in a serial test:

* every concurrent audit write is persisted — a lost row is a silently
  unaudited delivery;
* no audit write is downgraded to a logged failure — :meth:`DeliveryRecorder.record`
  contains persistence errors by design, so a race would surface as "fewer rows"
  or as a rising ``kbo_notification_delivery_audit_failures_total`` rather than
  as an exception.

Backend selection follows the repo convention: ``KBO_E2E_DATABASE_URL`` (or a
non-SQLite ``DATABASE_URL`` — the ``integration-test-postgres`` job) exercises
genuine concurrent writers. Otherwise an isolated file-based SQLite database is
used with WAL + ``BEGIN IMMEDIATE``, so the SQLite lane still runs real threads
instead of skipping.

Rows written here are tagged with a probe ``notification_type`` and deleted on
teardown, so a shared database is never truncated.
"""

from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pytest
from prometheus_client import REGISTRY
from sqlalchemy import create_engine, delete, event, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.notification_delivery import NotificationDelivery
from src.notifications.dto import (
    NotificationBatchReport,
    NotificationChannel,
    NotificationDispatchResult,
)
from src.notifications.recorder import DeliveryRecorder

pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)

#: Marks the rows this module owns, so teardown never touches anything else.
PROBE_TYPE = "concurrency-probe"
BASE_TIME = datetime(2026, 9, 25, 4, 45, 0)
WORKERS = 12

_AUDIT_FAILURE_METRIC = "kbo_notification_delivery_audit_failures_total"


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
        db_path = tmp_path / "delivery_concurrency.db"
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
        # Only this module's probe rows: the table is append-only and shared
        # backends may hold real audit history.
        with eng.begin() as connection:
            connection.execute(delete(NotificationDelivery).where(NotificationDelivery.notification_type == PROBE_TYPE))
        eng.dispose()


@pytest.fixture
def session_factory(engine):
    return sessionmaker(bind=engine, expire_on_commit=False)


def _result(channel: NotificationChannel) -> NotificationDispatchResult:
    return NotificationDispatchResult(channel=channel, status="SENT", attempt_count=1, duration_seconds=0.01)


def _report(*channels: NotificationChannel) -> NotificationBatchReport:
    results = [_result(channel) for channel in channels]
    return NotificationBatchReport(
        total_messages=len(results),
        sent_count=len(results),
        failed_count=0,
        suppressed_count=0,
        results=results,
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
            logger.exception("delivery concurrency probe worker %s failed", index)
            with lock:
                errors.append(exc)
        else:
            with lock:
                results.append(outcome)

    with ThreadPoolExecutor(max_workers=count) as pool:
        list(pool.map(_wrapped, range(count)))
    return results, errors


class TestConcurrentDeliveryAudit:
    def test_every_concurrent_write_is_persisted(self, session_factory) -> None:
        recorder = DeliveryRecorder(session_factory)

        written, errors = _run_concurrently(
            lambda _i: recorder.record(
                _report(NotificationChannel.TELEGRAM),
                notification_type=PROBE_TYPE,
                recorded_at=BASE_TIME,
            ),
        )

        assert errors == []
        assert sum(written) == WORKERS

        with session_factory() as session:
            rows = session.execute(
                select(func.count())
                .select_from(NotificationDelivery)
                .where(NotificationDelivery.notification_type == PROBE_TYPE),
            ).scalar_one()
            batches = session.execute(
                select(func.count(func.distinct(NotificationDelivery.batch_id))).where(
                    NotificationDelivery.notification_type == PROBE_TYPE
                ),
            ).scalar_one()

        assert rows == WORKERS, "a concurrent audit write was lost"
        assert batches == WORKERS, "each record() call must own its own batch_id"

    def test_concurrent_fanout_keeps_one_batch_per_record(self, session_factory) -> None:
        """A two-channel record writes two rows sharing one batch_id; twelve of
        them racing must still produce twelve batches, not a merged or lost one.
        """
        recorder = DeliveryRecorder(session_factory)

        written, errors = _run_concurrently(
            lambda _i: recorder.record(
                _report(NotificationChannel.TELEGRAM, NotificationChannel.SLACK),
                notification_type=PROBE_TYPE,
                recorded_at=BASE_TIME,
            ),
        )

        assert errors == []
        assert written == [2] * WORKERS

        with session_factory() as session:
            per_batch = session.execute(
                select(NotificationDelivery.batch_id, func.count())
                .where(NotificationDelivery.notification_type == PROBE_TYPE)
                .group_by(NotificationDelivery.batch_id),
            ).all()

        assert len(per_batch) == WORKERS
        assert {count for _batch, count in per_batch} == {2}

    def test_no_audit_write_is_silently_downgraded(self, session_factory) -> None:
        """`record()` swallows persistence errors, so a lock or connection race
        would look like "no rows" instead of failing loudly. The metric is the
        only signal that distinguishes the two.
        """
        before = REGISTRY.get_sample_value(_AUDIT_FAILURE_METRIC) or 0.0
        recorder = DeliveryRecorder(session_factory)

        _written, errors = _run_concurrently(
            lambda _i: recorder.record(
                _report(NotificationChannel.TELEGRAM),
                notification_type=PROBE_TYPE,
                recorded_at=BASE_TIME,
            ),
        )

        after = REGISTRY.get_sample_value(_AUDIT_FAILURE_METRIC) or 0.0

        assert errors == []
        assert after == before, "an audit write failed and was only logged"
