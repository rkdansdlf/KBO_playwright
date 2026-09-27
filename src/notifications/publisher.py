"""Single application entry point for publishing alerts.

Producers call :meth:`AlertPublisher.publish` with a normalized
:class:`~src.notifications.alert_dto.AlertEvent`; they never render a message,
choose a channel, or touch Telegram/Slack. The publisher composes the two lower
layers::

    domain check -> IncidentManager (state/policy) -> NotificationDispatcher -> transport

It also exposes the recovery side of the lifecycle: :meth:`resolve`,
:meth:`reconcile` and :meth:`acknowledge`. ``reconcile`` is how a passing run
emits ``RECOVERED`` for the incidents it did not report.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.notifications.alert_dto import (
    AlertDecision,
    AlertEvent,
    AlertSeverity,
    AlertSource,
    AlertStatus,
    IncidentTransition,
    utcnow,
)
from src.notifications.dispatcher import NotificationDispatcher
from src.notifications.dto import NotificationBatchReport, NotificationChannel, NotificationMessage
from src.notifications.formatter import format_incident_body, format_incident_title
from src.notifications.incident import IncidentManager
from src.notifications.policy import resolve_chat_id, severity_to_priority
from src.notifications.recorder import DeliveryRecorder, default_session_factory
from src.utils.metrics import record_open_incidents

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.orm import Session

    from src.models.notification_incident import NotificationIncident

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PublishResult:
    """Outcome of one publisher operation.

    ``delivery_report`` preserves the per-channel result (``SENT`` /
    ``SKIPPED_UNCONFIGURED`` / ``FAILED``) of the fan-out, which would otherwise
    be reduced to a single failure count. It is ``None`` when the decision did
    not warrant a delivery.
    """

    incident: IncidentTransition
    delivery_report: NotificationBatchReport | None = None

    @property
    def incident_key(self) -> str:
        """Return the semantic incident key."""
        return self.incident.incident_key

    @property
    def decision(self) -> AlertDecision:
        """Return the lifecycle decision."""
        return self.incident.decision

    @property
    def state(self) -> AlertStatus:
        """Return the resulting incident state."""
        return self.incident.state

    @property
    def severity(self) -> AlertSeverity:
        """Return the incident severity."""
        return self.incident.severity

    @property
    def occurrence_count(self) -> int:
        """Return how many times the incident has been observed."""
        return self.incident.occurrence_count

    @property
    def first_opened_at(self) -> datetime:
        """Return when the incident was first opened."""
        return self.incident.first_opened_at

    @property
    def last_seen_at(self) -> datetime:
        """Return when the incident was last observed."""
        return self.incident.last_seen_at

    @property
    def duration_seconds(self) -> float:
        """Return the incident duration at the time of the transition."""
        return self.incident.duration_seconds

    @property
    def previous_state(self) -> AlertStatus | None:
        """Return the state before this transition."""
        return self.incident.previous_state

    @property
    def previous_severity(self) -> AlertSeverity | None:
        """Return the severity before this transition."""
        return self.incident.previous_severity

    @property
    def should_notify(self) -> bool:
        """Return whether the decision warranted delivery."""
        return self.incident.should_notify

    @property
    def is_active(self) -> bool:
        """Return whether the incident is still open or acknowledged."""
        return self.incident.is_active

    def to_dict(self) -> dict[str, object]:
        """Convert the result to a serializable dictionary."""
        return {
            "incident": self.incident.to_dict(),
            "delivery_report": self.delivery_report.to_dict() if self.delivery_report else None,
        }


def event_from_incident(incident: NotificationIncident) -> AlertEvent:
    """Reconstruct an :class:`AlertEvent` from a persisted incident row.

    Used to render recovery messages without requiring the recovery caller to
    re-supply the original event.
    """
    return AlertEvent(
        source=AlertSource(incident.source),
        component=incident.component,
        severity=AlertSeverity(incident.severity),
        title=incident.title,
        message=incident.message,
        incident_key=incident.incident_key,
        occurred_at=incident.last_seen_at,
        metadata=incident.metadata_json or {},
    )


class AlertPublisher:
    """Publish alert events and drive the incident lifecycle and delivery."""

    def __init__(
        self,
        session: Session,
        *,
        dispatcher: NotificationDispatcher | None = None,
        recorder: DeliveryRecorder | None = None,
    ) -> None:
        """Initialize the publisher with a caller-managed session.

        The delivery recorder is given a *session factory*, not the caller's
        session, so delivery audit is committed independently of the incident
        transaction.
        """
        self.session = session
        self.manager = IncidentManager(session)
        if dispatcher is not None:
            self.dispatcher = dispatcher
        else:
            effective_recorder = recorder or DeliveryRecorder(default_session_factory)
            self.dispatcher = NotificationDispatcher(recorder=effective_recorder)

    # --------------------------------------------------------------- publishing

    def publish(
        self,
        event: AlertEvent,
        *,
        now: datetime | None = None,
        dry_run: bool = False,
    ) -> PublishResult:
        """Open, update, escalate or suppress an incident and deliver if warranted.

        ``now`` is resolved once here and reused for the whole operation so the
        incident timestamp and the delivery timestamp cannot disagree.
        """
        moment = now or event.occurred_at or utcnow()
        transition = self.manager.process(event, now=moment)
        report = None
        if transition.should_notify:
            report = self._deliver(event, transition, now=moment, dry_run=dry_run)
        return PublishResult(incident=transition, delivery_report=report)

    def publish_many(
        self,
        events: list[AlertEvent],
        *,
        now: datetime | None = None,
        dry_run: bool = False,
    ) -> list[PublishResult]:
        """Publish a batch of events and refresh incident metrics once."""
        results = [self.publish(event, now=now, dry_run=dry_run) for event in events]
        self.refresh_metrics()
        return results

    # --------------------------------------------------------------- lifecycle

    def acknowledge(self, incident_key: str, *, now: datetime | None = None) -> IncidentTransition | None:
        """Acknowledge an incident so it stops re-notifying until it escalates."""
        return self.manager.acknowledge(incident_key, now=now or utcnow())

    def resolve(
        self,
        incident_key: str,
        *,
        now: datetime | None = None,
        dry_run: bool = False,
    ) -> PublishResult | None:
        """Resolve one incident and send a recovery notice exactly once."""
        moment = now or utcnow()
        incident = self.manager.get(incident_key)
        if incident is None:
            return None
        event = event_from_incident(incident)

        transition = self.manager.resolve(incident_key, now=moment)
        if transition is None:
            return None
        report = None
        if transition.should_notify:
            report = self._deliver(event, transition, now=moment, dry_run=dry_run)
        return PublishResult(incident=transition, delivery_report=report)

    def reconcile(
        self,
        active_keys: set[str],
        *,
        source: AlertSource | None = None,
        now: datetime | None = None,
        dry_run: bool = False,
    ) -> list[PublishResult]:
        """Recover active incidents absent from ``active_keys`` and notify once."""
        moment = now or utcnow()
        recovered: list[PublishResult] = []
        for incident in self.manager.active_incidents(source=source):
            if incident.incident_key in active_keys:
                continue
            event = event_from_incident(incident)
            transition = self.manager.resolve(incident.incident_key, now=moment)
            if transition is None:
                continue
            report = None
            if transition.should_notify:
                report = self._deliver(event, transition, now=moment, dry_run=dry_run)
            recovered.append(PublishResult(incident=transition, delivery_report=report))
        self.refresh_metrics()
        return recovered

    # ----------------------------------------------------------------- metrics

    def refresh_metrics(self) -> None:
        """Publish the current open-incident counts to Prometheus."""
        counts: dict[tuple[str, str], int] = {}
        for incident in self.manager.active_incidents():
            key = (incident.source, incident.severity)
            counts[key] = counts.get(key, 0) + 1
        record_open_incidents(counts)

    # --------------------------------------------------------------- internal

    def _deliver(
        self,
        event: AlertEvent,
        transition: IncidentTransition,
        *,
        now: datetime,
        dry_run: bool,
    ) -> NotificationBatchReport:
        message = NotificationMessage(
            title=format_incident_title(event, transition),
            body=format_incident_body(event, transition),
            priority=severity_to_priority(transition.severity),
            channel=NotificationChannel.CONSOLE,
            recipient_id=resolve_chat_id(event.source.value),
            metadata=event.metadata,
            notification_type="alert",
        )
        report = self.dispatcher.dispatch_by_priority(
            message,
            dry_run=dry_run,
            incident_id=transition.incident_id,
        )

        if dry_run:
            return report
        if report.failed_count == 0:
            self.manager.mark_notified(event.incident_key, now=now)
        else:
            logger.warning(
                "Incident %s delivery failed (%d channel(s)); will retry next run",
                event.incident_key,
                report.failed_count,
            )
        return report
