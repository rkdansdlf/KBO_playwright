"""Durable incident lifecycle for the in-process alert manager.

The manager turns a stream of :class:`AlertEvent` publications into an
operational lifecycle: a new failure OPENS one incident, repeated publications
update it without re-notifying inside the cooldown window, a severity increase
ESCALATES immediately, and a passing check RECOVERS it exactly once.

State lives in the ``notification_incidents`` table so a scheduler restart does
not re-announce a still-open incident. The manager never commits — callers own
the transaction boundary, matching the repository contract.

Concurrency contract
--------------------
Every mutation is a single atomic SQL statement rather than an ORM
read-modify-write:

* creation races are absorbed by the ``incident_key`` unique constraint and an
  ``INSERT`` inside a SAVEPOINT, so the losing publisher falls through to the
  update path instead of aborting the caller's transaction;
* ``occurrence_count`` is incremented in the database
  (``occurrence_count = occurrence_count + 1``), so concurrent publications
  cannot lose a count;
* recovery and acknowledgement are guarded by a predicate on ``state`` and use
  the affected row count to stay idempotent.

The pre-read in :meth:`process` supplies *decision inputs* (previous severity,
acknowledged state, last notification time) only; it is never the basis for a
mutation.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from sqlalchemy import delete, func, insert, select, text, update
from sqlalchemy.exc import IntegrityError

from src.models.notification_incident import (
    ACTIVE_INCIDENT_STATES,
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
    IncidentTransition,
    severity_rank,
    utcnow,
)
from src.notifications.policy import cooldown_seconds, should_notify_severity

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_ACTIVE_STATES = tuple(ACTIVE_INCIDENT_STATES)


def _cooldown_elapsed(incident: NotificationIncident, severity: AlertSeverity, now: datetime) -> bool:
    if incident.last_notified_at is None:
        return True
    return (now - incident.last_notified_at).total_seconds() >= cooldown_seconds(severity)


def _notify_floor_allows(severity: AlertSeverity) -> bool:
    return should_notify_severity(severity)


class IncidentManager:
    """Own the durable lifecycle of alerts, deduping and cooling them down."""

    def __init__(self, session: Session) -> None:
        """Initialize the manager with a caller-managed session."""
        self.session = session

    # ------------------------------------------------------------------ reads

    def get(self, incident_key: str) -> NotificationIncident | None:
        """Return the incident with ``incident_key``, refreshed from the database."""
        stmt = (
            select(NotificationIncident)
            .where(NotificationIncident.incident_key == incident_key)
            .execution_options(populate_existing=True)
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def active_incidents(self, *, source: AlertSource | None = None) -> list[NotificationIncident]:
        """Return all currently open or acknowledged incidents."""
        stmt = select(NotificationIncident).where(NotificationIncident.state.in_(_ACTIVE_STATES))
        if source is not None:
            stmt = stmt.where(NotificationIncident.source == source.value)
        stmt = stmt.order_by(NotificationIncident.last_seen_at.desc())
        return list(self.session.execute(stmt).scalars().all())

    # ---------------------------------------------------------------- process

    def process(self, event: AlertEvent, *, now: datetime | None = None) -> IncidentTransition:
        """Apply an alert event, opening, updating, escalating or suppressing it."""
        moment = now or event.occurred_at or utcnow()
        incident = self.get(event.incident_key)

        if incident is None:
            return self._open(event, moment)

        previous_state = AlertStatus(incident.state)
        previous_severity = AlertSeverity(incident.severity)

        if previous_state == AlertStatus.RECOVERED:
            return self._reopen(event, moment, previous_severity)

        return self._update_active(event, moment, previous_state, previous_severity)

    def _open(self, event: AlertEvent, moment: datetime) -> IncidentTransition:
        """Insert a new incident, or fall through to the update path if a peer won.

        Uses a dialect-native insert-if-absent (``ON CONFLICT DO NOTHING`` on
        SQLite/PostgreSQL, ``MERGE`` on Oracle). A SAVEPOINT-based
        insert-and-catch is deliberately avoided: with pysqlite's default
        transaction control, a SAVEPOINT does not participate in the outer
        transaction, so a rolled-back caller would still leave the incident row
        behind.
        """
        if not self._insert_if_absent(event, moment):
            return self._recover_from_create_race(event, moment, None)

        incident = self._reload(event.incident_key)
        decision = AlertDecision.NEW if _notify_floor_allows(event.severity) else AlertDecision.SUPPRESSED
        return _build_transition(
            incident,
            decision=decision,
            previous_state=None,
            previous_severity=None,
            now=moment,
        )

    def _insert_if_absent(self, event: AlertEvent, moment: datetime) -> bool:
        """Insert the incident row atomically; return whether this call inserted it."""
        values = {
            "incident_key": event.incident_key,
            "source": event.source.value,
            "component": event.component,
            "severity": event.severity.value,
            "state": INCIDENT_STATE_OPEN,
            "title": event.title,
            "message": event.message,
            "details_hash": event.details_hash(),
            "occurrence_count": 1,
            "notification_count": 0,
            "first_opened_at": moment,
            "last_seen_at": moment,
            "metadata_json": event.metadata or None,
        }
        dialect = self.session.get_bind().dialect.name

        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as pg_insert

            stmt = pg_insert(NotificationIncident).values(**values).on_conflict_do_nothing()
            return bool(self.session.execute(stmt).rowcount)

        if dialect == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert

            stmt = sqlite_insert(NotificationIncident).values(**values).on_conflict_do_nothing()
            return bool(self.session.execute(stmt).rowcount)

        if dialect == "oracle":
            return self._oracle_merge_insert(values)

        # Unknown dialect: fall back to a savepoint-scoped insert and catch the
        # unique violation. Safe on dialects with real SAVEPOINT semantics.
        try:
            with self.session.begin_nested():
                self.session.execute(insert(NotificationIncident).values(**values))
        except IntegrityError:
            return False
        return True

    def _oracle_merge_insert(self, values: dict[str, object]) -> bool:
        """Oracle `MERGE` equivalent of INSERT ... ON CONFLICT DO NOTHING."""
        columns = (
            "incident_key",
            "source",
            "component",
            "severity",
            "state",
            "title",
            "message",
            "details_hash",
            "occurrence_count",
            "notification_count",
            "first_opened_at",
            "last_seen_at",
            "metadata",
        )
        column_list = ", ".join(columns)
        bind_list = ", ".join(f":{name}" for name in columns)
        statement = text(
            "MERGE INTO notification_incidents t "  # noqa: S608 - fixed literal column tuple, bound values
            "USING (SELECT :incident_key AS incident_key FROM dual) s "
            "ON (t.incident_key = s.incident_key) "
            "WHEN NOT MATCHED THEN INSERT "
            f"({column_list}) VALUES ({bind_list})",
        )
        payload = dict(values)
        metadata_value = payload.pop("metadata_json", None)
        payload["metadata"] = json.dumps(metadata_value) if metadata_value else None
        result = self.session.execute(statement, payload)
        return bool(result.rowcount)

    def _recover_from_create_race(
        self,
        event: AlertEvent,
        moment: datetime,
        exc: BaseException | None,
    ) -> IncidentTransition:
        """Handle losing the create race: treat this publication as a repeat."""
        raced = self.get(event.incident_key)
        if raced is None:  # pragma: no cover - unique violation without a visible row
            msg = "incident create race lost but no incident row is visible"
            raise RuntimeError(msg) from exc
        previous_state = AlertStatus(raced.state)
        previous_severity = AlertSeverity(raced.severity)
        if previous_state == AlertStatus.RECOVERED:
            return self._reopen(event, moment, previous_severity)
        return self._update_active(event, moment, previous_state, previous_severity)

    def _reopen(self, event: AlertEvent, moment: datetime, previous_severity: AlertSeverity) -> IncidentTransition:
        """Reopen a recovered incident, resetting its lifecycle counters."""
        self.session.execute(
            update(NotificationIncident)
            .where(
                NotificationIncident.incident_key == event.incident_key,
                NotificationIncident.state == INCIDENT_STATE_RECOVERED,
            )
            .values(
                state=INCIDENT_STATE_OPEN,
                source=event.source.value,
                component=event.component,
                severity=event.severity.value,
                title=event.title,
                message=event.message,
                details_hash=event.details_hash(),
                metadata_json=event.metadata or None,
                occurrence_count=1,
                first_opened_at=moment,
                last_seen_at=moment,
                last_notified_at=None,
                resolved_at=None,
            )
            .execution_options(synchronize_session=False),
        )
        self.session.flush()

        incident = self._reload(event.incident_key)
        decision = AlertDecision.NEW if _notify_floor_allows(event.severity) else AlertDecision.SUPPRESSED
        return _build_transition(
            incident,
            decision=decision,
            previous_state=AlertStatus.RECOVERED,
            previous_severity=previous_severity,
            now=moment,
        )

    def _update_active(
        self,
        event: AlertEvent,
        moment: datetime,
        previous_state: AlertStatus,
        previous_severity: AlertSeverity,
    ) -> IncidentTransition:
        """Atomically bump an active incident and decide whether to notify."""
        self.session.execute(
            update(NotificationIncident)
            .where(
                NotificationIncident.incident_key == event.incident_key,
                NotificationIncident.state.in_(_ACTIVE_STATES),
            )
            .values(
                occurrence_count=NotificationIncident.occurrence_count + 1,
                last_seen_at=moment,
                title=event.title,
                message=event.message,
                details_hash=event.details_hash(),
                metadata_json=event.metadata or None,
                severity=event.severity.value,
            )
            .execution_options(synchronize_session=False),
        )
        self.session.flush()

        incident = self._reload(event.incident_key)
        escalated = severity_rank(event.severity) > severity_rank(previous_severity)
        decision = self._active_decision(event.severity, incident, moment, escalated=escalated)
        return _build_transition(
            incident,
            decision=decision,
            previous_state=previous_state,
            previous_severity=previous_severity,
            now=moment,
        )

    def _active_decision(
        self,
        severity: AlertSeverity,
        incident: NotificationIncident,
        moment: datetime,
        *,
        escalated: bool,
    ) -> AlertDecision:
        if not _notify_floor_allows(severity):
            return AlertDecision.SUPPRESSED
        if escalated:
            return AlertDecision.ESCALATED
        if incident.state == INCIDENT_STATE_ACKNOWLEDGED:
            return AlertDecision.SUPPRESSED
        if _cooldown_elapsed(incident, severity, moment):
            return AlertDecision.REPEAT
        return AlertDecision.SUPPRESSED

    # ------------------------------------------------------------- resolution

    def resolve(
        self,
        incident_key: str,
        *,
        now: datetime | None = None,
    ) -> IncidentTransition | None:
        """Mark an active incident as recovered.

        Returns ``None`` when there is nothing to recover so callers can treat
        "no transition" and "recovery" distinctly. Concurrent resolvers are
        idempotent: only the caller whose conditional UPDATE affected a row gets
        a transition.
        """
        moment = now or utcnow()
        incident = self.get(incident_key)
        if incident is None or incident.state == INCIDENT_STATE_RECOVERED:
            return None
        previous_state = AlertStatus(incident.state)
        previous_severity = AlertSeverity(incident.severity)

        result = self.session.execute(
            update(NotificationIncident)
            .where(
                NotificationIncident.incident_key == incident_key,
                NotificationIncident.state != INCIDENT_STATE_RECOVERED,
            )
            .values(
                state=INCIDENT_STATE_RECOVERED,
                resolved_at=moment,
                last_seen_at=moment,
            )
            .execution_options(synchronize_session=False),
        )
        self.session.flush()
        if not result.rowcount:
            return None

        updated = self._reload(incident_key)
        return _build_transition(
            updated,
            decision=AlertDecision.RECOVERED,
            previous_state=previous_state,
            previous_severity=previous_severity,
            now=moment,
        )

    def reconcile(
        self,
        active_keys: set[str],
        *,
        source: AlertSource | None = None,
        now: datetime | None = None,
    ) -> list[IncidentTransition]:
        """Recover every active incident for ``source`` absent from ``active_keys``.

        A passing run reports only what is failing, so the caller must pass the
        full set of currently-active keys; anything else is treated as recovered.
        """
        recovered: list[IncidentTransition] = []
        for incident in self.active_incidents(source=source):
            if incident.incident_key in active_keys:
                continue
            transition = self.resolve(incident.incident_key, now=now)
            if transition is not None:
                recovered.append(transition)
        return recovered

    def acknowledge(self, incident_key: str, *, now: datetime | None = None) -> IncidentTransition | None:
        """Acknowledge an open incident so it stops re-notifying until it escalates."""
        moment = now or utcnow()
        incident = self.get(incident_key)
        if incident is None or incident.state != INCIDENT_STATE_OPEN:
            return None
        previous_severity = AlertSeverity(incident.severity)

        result = self.session.execute(
            update(NotificationIncident)
            .where(
                NotificationIncident.incident_key == incident_key,
                NotificationIncident.state == INCIDENT_STATE_OPEN,
            )
            .values(
                state=INCIDENT_STATE_ACKNOWLEDGED,
                last_seen_at=moment,
            )
            .execution_options(synchronize_session=False),
        )
        self.session.flush()
        if not result.rowcount:
            return None

        updated = self._reload(incident_key)
        return _build_transition(
            updated,
            decision=AlertDecision.ACKNOWLEDGED,
            previous_state=AlertStatus.OPEN,
            previous_severity=previous_severity,
            now=moment,
        )

    # -------------------------------------------------------------- bookkeeping

    def mark_notified(self, incident_key: str, *, now: datetime | None = None) -> None:
        """Record a successful delivery so the cooldown window starts now."""
        moment = now or utcnow()
        self.session.execute(
            update(NotificationIncident)
            .where(NotificationIncident.incident_key == incident_key)
            .values(
                last_notified_at=moment,
                notification_count=NotificationIncident.notification_count + 1,
            )
            .execution_options(synchronize_session=False),
        )
        self.session.flush()

    def prune(self, *, recovered_before: datetime) -> int:
        """Delete recovered incidents resolved before ``recovered_before``."""
        stmt = delete(NotificationIncident).where(
            NotificationIncident.state == INCIDENT_STATE_RECOVERED,
            NotificationIncident.resolved_at.is_not(None),
            NotificationIncident.resolved_at < recovered_before,
        )
        result = self.session.execute(stmt)
        self.session.flush()
        return int(result.rowcount or 0)

    def count_active(self, *, source: AlertSource | None = None) -> int:
        """Return the number of open or acknowledged incidents."""
        stmt = (
            select(func.count())
            .select_from(NotificationIncident)
            .where(
                NotificationIncident.state.in_(_ACTIVE_STATES),
            )
        )
        if source is not None:
            stmt = stmt.where(NotificationIncident.source == source.value)
        return int(self.session.execute(stmt).scalar_one())

    # --------------------------------------------------------------- internal

    def _reload(self, incident_key: str) -> NotificationIncident:
        """Re-read an incident row after an atomic update."""
        incident = self.get(incident_key)
        if incident is None:  # pragma: no cover - defensive
            msg = f"incident {incident_key!r} disappeared after mutation"
            raise RuntimeError(msg)
        return incident


def _build_transition(
    incident: NotificationIncident,
    *,
    decision: AlertDecision,
    previous_state: AlertStatus | None,
    previous_severity: AlertSeverity | None,
    now: datetime,
) -> IncidentTransition:
    """Assemble an :class:`IncidentTransition` from a persisted incident row."""
    first_opened_at = incident.first_opened_at
    duration = (now - first_opened_at).total_seconds() if first_opened_at else 0.0
    return IncidentTransition(
        incident_key=incident.incident_key,
        source=AlertSource(incident.source),
        severity=AlertSeverity(incident.severity),
        state=AlertStatus(incident.state),
        decision=decision,
        occurrence_count=incident.occurrence_count,
        first_opened_at=first_opened_at,
        last_seen_at=incident.last_seen_at,
        previous_state=previous_state,
        previous_severity=previous_severity,
        duration_seconds=duration,
        incident_id=incident.id,
    )
