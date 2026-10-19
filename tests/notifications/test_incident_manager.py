"""Tests for the durable incident lifecycle manager."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select, update
from sqlalchemy.dialects import postgresql as postgresql_dialect
from sqlalchemy.dialects import sqlite as sqlite_dialect
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from src.models.base import Base
from src.models.notification_incident import (
    INCIDENT_STATE_ACKNOWLEDGED,
    INCIDENT_STATE_OPEN,
    INCIDENT_STATE_RECOVERED,
    NotificationIncident,
)
from src.notifications.alert_dto import (
    AlertDecision,
    AlertEvent,
    AlertSeverity,
    AlertSource,
    AlertStatus,
)
from src.notifications.incident import IncidentManager

BASE_TIME = datetime(2026, 9, 25, 4, 45, 0)

#: Both dialects whose create path is the same "insert-if-absent, ask RETURNING"
#: shape. Compiling the captured statement against each is what proves the
#: decision is available to the caller at all.
_DIALECTS = {
    "postgresql": postgresql_dialect.dialect(),
    "sqlite": sqlite_dialect.dialect(),
}


@pytest.fixture
def session() -> Session:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    maker = sessionmaker(bind=engine, expire_on_commit=False)
    sess = maker()
    try:
        yield sess
    finally:
        sess.close()


def _event(
    *,
    key: str = "integrity:game_stats:20260925",
    severity: AlertSeverity = AlertSeverity.ERROR,
    message: str = "3 integrity checks failed",
    occurred_at: datetime | None = None,
    source: AlertSource = AlertSource.INTEGRITY,
    component: str = "game_stats",
) -> AlertEvent:
    return AlertEvent(
        source=source,
        component=component,
        severity=severity,
        title="Data integrity failure",
        message=message,
        incident_key=key,
        occurred_at=occurred_at or BASE_TIME,
        metadata={"failed": 3},
    )


class TestIncidentLifecycle:
    """OPEN -> REPEAT/SUPPRESSED -> ESCALATED -> RECOVERED transitions."""

    def test_new_event_opens_incident(self, session: Session) -> None:
        manager = IncidentManager(session)
        transition = manager.process(_event(), now=BASE_TIME)

        assert transition.decision == AlertDecision.NEW
        assert transition.state == AlertStatus.OPEN
        assert transition.occurrence_count == 1
        assert transition.should_notify is True
        assert manager.count_active() == 1

    def test_duplicate_within_cooldown_is_suppressed(self, session: Session) -> None:
        manager = IncidentManager(session)
        manager.process(_event(), now=BASE_TIME)
        manager.mark_notified("integrity:game_stats:20260925", now=BASE_TIME)

        later = BASE_TIME + timedelta(minutes=1)
        transition = manager.process(_event(occurred_at=later), now=later)

        assert transition.decision == AlertDecision.SUPPRESSED
        assert transition.occurrence_count == 2
        assert transition.should_notify is False

    def test_duplicate_after_cooldown_repeats(self, session: Session) -> None:
        manager = IncidentManager(session)
        manager.process(_event(), now=BASE_TIME)
        manager.mark_notified("integrity:game_stats:20260925", now=BASE_TIME)

        later = BASE_TIME + timedelta(minutes=11)
        transition = manager.process(_event(occurred_at=later), now=later)

        assert transition.decision == AlertDecision.REPEAT
        assert transition.should_notify is True

    def test_severity_escalation_notifies_immediately(self, session: Session) -> None:
        manager = IncidentManager(session)
        manager.process(_event(severity=AlertSeverity.WARNING), now=BASE_TIME)
        manager.mark_notified("integrity:game_stats:20260925", now=BASE_TIME)

        later = BASE_TIME + timedelta(minutes=1)
        transition = manager.process(
            _event(severity=AlertSeverity.CRITICAL, occurred_at=later),
            now=later,
        )

        assert transition.decision == AlertDecision.ESCALATED
        assert transition.severity == AlertSeverity.CRITICAL
        assert transition.should_notify is True

    def test_severity_downgrade_does_not_notify(self, session: Session) -> None:
        manager = IncidentManager(session)
        manager.process(_event(severity=AlertSeverity.CRITICAL), now=BASE_TIME)
        manager.mark_notified("integrity:game_stats:20260925", now=BASE_TIME)

        later = BASE_TIME + timedelta(minutes=1)
        transition = manager.process(_event(severity=AlertSeverity.WARNING, occurred_at=later), now=later)

        assert transition.decision == AlertDecision.SUPPRESSED

    def test_info_below_floor_is_recorded_but_suppressed(self, session: Session) -> None:
        manager = IncidentManager(session)
        transition = manager.process(_event(severity=AlertSeverity.INFO), now=BASE_TIME)

        assert transition.decision == AlertDecision.SUPPRESSED
        assert transition.should_notify is False
        row = manager.get("integrity:game_stats:20260925")
        assert row is not None
        assert row.state == INCIDENT_STATE_OPEN

    def test_acknowledge_stops_renotify(self, session: Session) -> None:
        manager = IncidentManager(session)
        manager.process(_event(), now=BASE_TIME)
        manager.mark_notified("integrity:game_stats:20260925", now=BASE_TIME)

        ack = manager.acknowledge("integrity:game_stats:20260925", now=BASE_TIME)
        assert ack is not None
        assert ack.state == AlertStatus.ACKNOWLEDGED

        later = BASE_TIME + timedelta(minutes=30)
        transition = manager.process(_event(occurred_at=later), now=later)
        assert transition.decision == AlertDecision.SUPPRESSED
        assert transition.state == AlertStatus.ACKNOWLEDGED

    def test_acknowledged_incident_still_escalates(self, session: Session) -> None:
        manager = IncidentManager(session)
        manager.process(_event(severity=AlertSeverity.WARNING), now=BASE_TIME)
        manager.mark_notified("integrity:game_stats:20260925", now=BASE_TIME)
        manager.acknowledge("integrity:game_stats:20260925", now=BASE_TIME)

        later = BASE_TIME + timedelta(minutes=1)
        transition = manager.process(_event(severity=AlertSeverity.CRITICAL, occurred_at=later), now=later)
        assert transition.decision == AlertDecision.ESCALATED


class TestResolution:
    """Recovery is emitted exactly once and reopen resets the lifecycle."""

    def test_resolve_recovers_once(self, session: Session) -> None:
        manager = IncidentManager(session)
        manager.process(_event(), now=BASE_TIME)

        recovered_at = BASE_TIME + timedelta(minutes=16)
        transition = manager.resolve("integrity:game_stats:20260925", now=recovered_at)

        assert transition is not None
        assert transition.decision == AlertDecision.RECOVERED
        assert transition.state == AlertStatus.RECOVERED
        assert transition.duration_seconds == pytest.approx(16 * 60)
        assert manager.resolve("integrity:game_stats:20260925", now=recovered_at) is None

    def test_resolve_unknown_key_returns_none(self, session: Session) -> None:
        manager = IncidentManager(session)
        assert manager.resolve("does:not:exist", now=BASE_TIME) is None

    def test_reconcile_recovers_missing_keys(self, session: Session) -> None:
        manager = IncidentManager(session)
        manager.process(_event(key="integrity:a:20260925", component="a"), now=BASE_TIME)
        manager.process(_event(key="integrity:b:20260925", component="b"), now=BASE_TIME)

        recovered = manager.reconcile({"integrity:a:20260925"}, source=AlertSource.INTEGRITY, now=BASE_TIME)

        assert [t.incident_key for t in recovered] == ["integrity:b:20260925"]
        assert manager.count_active(source=AlertSource.INTEGRITY) == 1

    def test_reconcile_only_touches_requested_source(self, session: Session) -> None:
        manager = IncidentManager(session)
        manager.process(_event(key="integrity:a:20260925"), now=BASE_TIME)
        manager.process(
            _event(key="quality:daily", source=AlertSource.QUALITY, component="daily"),
            now=BASE_TIME,
        )

        recovered = manager.reconcile(set(), source=AlertSource.INTEGRITY, now=BASE_TIME)

        assert [t.incident_key for t in recovered] == ["integrity:a:20260925"]
        assert manager.count_active(source=AlertSource.QUALITY) == 1

    def test_reopen_after_recovery_resets_lifecycle(self, session: Session) -> None:
        manager = IncidentManager(session)
        manager.process(_event(), now=BASE_TIME)
        manager.resolve("integrity:game_stats:20260925", now=BASE_TIME + timedelta(minutes=16))

        reopened_at = BASE_TIME + timedelta(hours=2)
        transition = manager.process(_event(occurred_at=reopened_at), now=reopened_at)

        assert transition.decision == AlertDecision.NEW
        assert transition.occurrence_count == 1
        assert transition.first_opened_at == reopened_at


class TestDurability:
    """State survives a manager restart because it lives in the database."""

    def test_restart_does_not_reannounce_open_incident(self, session: Session) -> None:
        first = IncidentManager(session)
        first.process(_event(), now=BASE_TIME)
        first.mark_notified("integrity:game_stats:20260925", now=BASE_TIME)

        later = BASE_TIME + timedelta(minutes=2)
        second = IncidentManager(session)
        transition = second.process(_event(occurred_at=later), now=later)

        assert transition.decision == AlertDecision.SUPPRESSED
        assert transition.occurrence_count == 2

    def test_semantic_key_merges_reworded_message(self, session: Session) -> None:
        manager = IncidentManager(session)
        manager.process(_event(message="3 integrity checks failed"), now=BASE_TIME)
        manager.mark_notified("integrity:game_stats:20260925", now=BASE_TIME)

        later = BASE_TIME + timedelta(minutes=1)
        manager.process(_event(message="5 integrity checks failed", occurred_at=later), now=later)

        rows = list(session.execute(select(NotificationIncident)).scalars().all())
        assert len(rows) == 1

    def test_mark_notified_increments_only_once_per_call(self, session: Session) -> None:
        manager = IncidentManager(session)
        manager.process(_event(), now=BASE_TIME)
        manager.mark_notified("integrity:game_stats:20260925", now=BASE_TIME)
        manager.mark_notified("integrity:game_stats:20260925", now=BASE_TIME)

        row = manager.get("integrity:game_stats:20260925")
        assert row is not None
        assert row.notification_count == 2

    def test_prune_removes_old_recovered_only(self, session: Session) -> None:
        manager = IncidentManager(session)
        manager.process(_event(key="old", component="old"), now=BASE_TIME)
        manager.resolve("old", now=BASE_TIME + timedelta(minutes=1))
        manager.process(_event(key="active", component="active"), now=BASE_TIME)

        removed = manager.prune(recovered_before=BASE_TIME + timedelta(days=30))

        assert removed == 1
        assert manager.get("old") is None
        assert manager.get("active") is not None


class TestIncidentReset:
    """The reopen path fully resets stale fields."""

    def test_reopen_clears_resolved_at(self, session: Session) -> None:
        manager = IncidentManager(session)
        manager.process(_event(), now=BASE_TIME)
        manager.resolve("integrity:game_stats:20260925", now=BASE_TIME + timedelta(minutes=5))

        reopened_at = BASE_TIME + timedelta(hours=1)
        manager.process(_event(occurred_at=reopened_at), now=reopened_at)

        row = manager.get("integrity:game_stats:20260925")
        assert row is not None
        assert row.state == INCIDENT_STATE_OPEN
        assert row.resolved_at is None
        assert row.last_notified_at is None

    def test_acknowledged_state_is_distinct_from_open(self, session: Session) -> None:
        manager = IncidentManager(session)
        manager.process(_event(), now=BASE_TIME)
        manager.acknowledge("integrity:game_stats:20260925", now=BASE_TIME)

        row = manager.get("integrity:game_stats:20260925")
        assert row is not None
        assert row.state == INCIDENT_STATE_ACKNOWLEDGED
        assert row.state != INCIDENT_STATE_RECOVERED


class TestUtcNowContract:
    """utcnow() stays naive-UTC to match the schema datetime convention."""

    def test_utcnow_is_naive(self) -> None:
        from src.notifications.alert_dto import utcnow

        value = utcnow()
        assert value.tzinfo is None
        assert abs((datetime.now(UTC).replace(tzinfo=None) - value).total_seconds()) < 5


class TestConcurrencyInvariants:
    """Deterministic proofs that mutations are database-atomic, not ORM read-modify-write."""

    KEY = "integrity:game_stats:20260925"

    def test_occurrence_count_increments_from_database_value(self, session: Session) -> None:
        manager = IncidentManager(session)
        manager.process(_event(), now=BASE_TIME)

        # Simulate a concurrent writer bumping the counter behind the ORM's back.
        session.execute(
            update(NotificationIncident)
            .where(NotificationIncident.incident_key == self.KEY)
            .values(occurrence_count=5)
            .execution_options(synchronize_session=False),
        )
        session.flush()

        later = BASE_TIME + timedelta(minutes=20)
        transition = manager.process(_event(occurred_at=later), now=later)

        # Atomic SQL increment reads 5 from the database, not the stale ORM value 1.
        assert transition.occurrence_count == 6

    def test_create_race_falls_through_to_update(self, session: Session, monkeypatch: pytest.MonkeyPatch) -> None:
        peer = IncidentManager(session)
        peer.process(_event(), now=BASE_TIME)

        manager = IncidentManager(session)
        original_get = manager.get
        calls = {"count": 0}

        def stale_then_real(incident_key: str):
            calls["count"] += 1
            if calls["count"] == 1:
                return None  # simulate a stale pre-read that missed the peer's insert
            return original_get(incident_key)

        monkeypatch.setattr(manager, "get", stale_then_real)

        later = BASE_TIME + timedelta(minutes=1)
        transition = manager.process(_event(occurred_at=later), now=later)

        assert calls["count"] >= 2
        assert manager.count_active() == 1
        assert transition.occurrence_count == 2
        # The peer opened the incident but had not marked it notified, so this
        # publication is a repeat rather than a second creation.
        assert transition.decision == AlertDecision.REPEAT

    def test_resolve_is_idempotent_when_rowcount_is_zero(
        self,
        session: Session,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        manager = IncidentManager(session)
        manager.process(_event(), now=BASE_TIME)

        # Another resolver already recovered the row in the database.
        session.execute(
            update(NotificationIncident)
            .where(NotificationIncident.incident_key == self.KEY)
            .values(state=INCIDENT_STATE_RECOVERED)
            .execution_options(synchronize_session=False),
        )
        session.flush()

        # Stale pre-read still sees OPEN, so the conditional UPDATE must decide.
        stale = SimpleNamespace(state=INCIDENT_STATE_OPEN, severity=AlertSeverity.ERROR.value)
        monkeypatch.setattr(manager, "get", lambda _key: stale)

        assert manager.resolve(self.KEY, now=BASE_TIME) is None

    def test_acknowledge_rowcount_zero_returns_none(
        self,
        session: Session,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        manager = IncidentManager(session)
        manager.process(_event(), now=BASE_TIME)
        manager.acknowledge(self.KEY, now=BASE_TIME)

        stale = SimpleNamespace(state=INCIDENT_STATE_OPEN, severity=AlertSeverity.ERROR.value)
        monkeypatch.setattr(manager, "get", lambda _key: stale)

        assert manager.acknowledge(self.KEY, now=BASE_TIME) is None

    def test_mark_notified_increments_from_database_value(self, session: Session) -> None:
        manager = IncidentManager(session)
        manager.process(_event(), now=BASE_TIME)

        session.execute(
            update(NotificationIncident)
            .where(NotificationIncident.incident_key == self.KEY)
            .values(notification_count=7)
            .execution_options(synchronize_session=False),
        )
        session.flush()

        manager.mark_notified(self.KEY, now=BASE_TIME + timedelta(minutes=1))

        row = manager.get(self.KEY)
        assert row is not None
        assert row.notification_count == 8


class TestTheCreateDecisionComesFromTheStatement:
    """Whether this call created the row may not be read off the driver.

    psycopg3 reports ``rowcount = -1`` for ``INSERT ... ON CONFLICT DO NOTHING``
    whether or not the row was inserted, and ``bool(-1)`` is ``True``. A
    ``rowcount`` test therefore told every racing publisher that *it* had created
    the incident: nobody bumped ``occurrence_count`` (12 concurrent publications
    left it at 1) and every loser returned a ``NEW`` transition for an incident a
    peer had opened. That measurement needs a PostgreSQL server and lives in
    ``test_incident_concurrency_integration.py``; this case pins the decision
    rule itself -- the statement's own ``RETURNING`` output -- so reverting to
    ``rowcount`` fails on the default SQLite run as well.
    """

    KEY = "integrity:game_stats:20260925"

    class _ResultThatLies:
        """A psycopg3-shaped result: ``rowcount`` is ``-1`` either way."""

        def __init__(self, returned_row: object | None) -> None:
            self.rowcount = -1
            self._returned_row = returned_row
            self.first_calls = 0

        def first(self) -> object | None:
            self.first_calls += 1
            return self._returned_row

    class _RecordingSession:
        """The session surface the create path uses, plus a statement log.

        Deliberately not a ``Session``: the contract under test is how an
        execution *result* is interpreted, and a real backend cannot be asked to
        report the losing driver's numbers on demand.
        """

        def __init__(self, dialect_name: str, result: object) -> None:
            self._dialect_name = dialect_name
            self._result = result
            self.statements: list[object] = []

        def get_bind(self) -> object:
            return SimpleNamespace(dialect=SimpleNamespace(name=self._dialect_name))

        def execute(self, statement: object) -> object:
            self.statements.append(statement)
            return self._result

    @pytest.mark.parametrize("dialect_name", ["postgresql", "sqlite"])
    @pytest.mark.parametrize(("returned_row", "expected"), [(None, False), ((7,), True)])
    def test_the_statement_is_asked_whether_the_row_was_inserted(
        self,
        dialect_name: str,
        returned_row: object | None,
        expected: bool,
    ) -> None:
        result = self._ResultThatLies(returned_row)
        session = self._RecordingSession(dialect_name, result)

        inserted = IncidentManager(session)._insert_if_absent(_event(), BASE_TIME)

        assert inserted is expected
        # consulted exactly once, and consulted at all: a truthy rowcount must
        # not be able to answer in its place
        assert result.first_calls == 1

    @pytest.mark.parametrize("dialect_name", ["postgresql", "sqlite"])
    def test_the_insert_is_conflict_safe_and_returns_the_created_key(self, dialect_name: str) -> None:
        result = self._ResultThatLies(None)
        session = self._RecordingSession(dialect_name, result)

        IncidentManager(session)._insert_if_absent(_event(), BASE_TIME)

        assert len(session.statements) == 1
        compiled = str(session.statements[0].compile(dialect=_DIALECTS[dialect_name]))
        assert "ON CONFLICT DO NOTHING" in compiled
        assert "RETURNING" in compiled
        # sqlite emits a bare ``id``; postgresql qualifies it with the table name
        assert "id" in compiled.split("RETURNING", 1)[1]


class TestCreateAtomicityContract:
    """The incident create insert must live in the caller's transaction.

    pysqlite's default transaction control does not make a ``SAVEPOINT``
    participate in the enclosing transaction: an incident opened through the
    old ``begin_nested()`` insert-and-catch path survived an outer
    ``session.rollback()``, silently breaking the atomicity contract of every
    caller that wraps ``process()`` in its own transaction. The create path
    therefore uses a dialect-native insert-if-absent (``ON CONFLICT DO
    NOTHING``) issued on the caller's session.

    This is the regression that motivated the change. If ``begin_nested()``
    comes back, the row outlives the rollback and this test fails. It is
    deliberately separate from the PostgreSQL-only
    ``test_delivery_audit_survives_caller_transaction_rollback`` in
    ``tests/notifications/test_delivery_recorder.py``, which guards the
    delivery audit path instead.
    """

    KEY = "integrity:game_stats:20260925"

    def test_created_incident_is_discarded_by_outer_rollback(self, session: Session) -> None:
        manager = IncidentManager(session)
        manager.process(_event(), now=BASE_TIME)

        # Before the rollback the row is visible inside the open transaction.
        visible = session.execute(
            select(NotificationIncident).where(NotificationIncident.incident_key == self.KEY),
        ).scalar_one_or_none()
        assert visible is not None

        session.rollback()

        remaining = session.execute(select(NotificationIncident)).scalars().all()
        assert remaining == []

    def test_reopened_incident_update_is_discarded_by_outer_rollback(self, session: Session) -> None:
        manager = IncidentManager(session)
        manager.process(_event(), now=BASE_TIME)
        session.commit()

        later = BASE_TIME + timedelta(hours=1)
        manager.process(_event(occurred_at=later), now=later)
        session.rollback()

        # The committed OPEN row survives, but the reopened occurrence count
        # must not: the update belongs to the rolled-back transaction.
        row = manager.get(self.KEY)
        assert row is not None
        assert row.occurrence_count == 1


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
