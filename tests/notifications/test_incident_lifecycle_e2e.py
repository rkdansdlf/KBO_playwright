"""End-to-end lifecycle contract for the incident notification subsystem.

One test locks the full behavior chain together:

    IncidentManager -> Policy -> Persistence -> Dispatcher -> Formatter -> Transport -> Recovery

Timeline exercised:

    T0      WARNING publish        -> OPEN, one delivery, occurrence_count = 1
    T+1m    WARNING publish        -> SUPPRESSED (inside cooldown), count = 2
    T+30m   CRITICAL publish       -> ESCALATED (severity jump bypasses cooldown), count = 3
    T+35m   resolve               -> RECOVERED, one recovery delivery, duration 35m
    T+36m   reconcile              -> no further deliveries
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from src.models.base import Base
from src.notifications.alert_dto import AlertDecision, AlertEvent, AlertSeverity, AlertSource
from src.notifications.dispatcher import NotificationDispatcher
from src.notifications.publisher import AlertPublisher
from src.utils.alerting import DeliveryOutcome, DeliveryResult

T0 = datetime(2026, 9, 25, 4, 45, 0)
KEY = "quality:daily"


class RecordingTransport:
    """Capture deliveries instead of hitting Telegram/Slack."""

    def __init__(self) -> None:
        self.telegram: list[str] = []
        self.slack: list[str] = []

    def telegram_deliver(self, message: str, chat_id: str | None = None) -> DeliveryResult:
        self.telegram.append(message)
        return DeliveryResult(channel="telegram", outcome=DeliveryOutcome.SENT, attempts=1)

    def slack_deliver_webhook(self, message: str, blocks=None) -> DeliveryResult:
        self.slack.append(message)
        return DeliveryResult(channel="slack", outcome=DeliveryOutcome.SENT, attempts=1)


@pytest.fixture
def session_factory():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


@pytest.fixture(autouse=True)
def _isolate_recorder(monkeypatch: pytest.MonkeyPatch, session_factory) -> None:
    """Keep the publisher's default delivery recorder in the test database."""
    monkeypatch.setattr("src.notifications.publisher.default_session_factory", session_factory)


@pytest.fixture
def session(session_factory) -> Session:
    sess = session_factory()
    try:
        yield sess
    finally:
        sess.close()


@pytest.fixture
def transport(monkeypatch: pytest.MonkeyPatch) -> RecordingTransport:
    rec = RecordingTransport()
    monkeypatch.setattr("src.utils.alerting.TelegramBotClient.deliver", rec.telegram_deliver)
    monkeypatch.setattr("src.utils.alerting.SlackWebhookClient.deliver_webhook", rec.slack_deliver_webhook)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "ops-chat")
    return rec


def _event(severity: AlertSeverity, moment: datetime) -> AlertEvent:
    return AlertEvent(
        source=AlertSource.QUALITY,
        component="daily",
        severity=severity,
        title="데이터 품질 게이트",
        message="quality score 40/100",
        incident_key=KEY,
        occurred_at=moment,
    )


def test_incident_full_lifecycle_e2e(session: Session, transport: RecordingTransport) -> None:
    """OPEN -> cooldown -> escalation -> RECOVERED, with exactly-once recovery."""
    publisher = AlertPublisher(session, dispatcher=NotificationDispatcher())

    # ---- T0: first failure opens the incident and notifies once
    opened = publisher.publish(_event(AlertSeverity.WARNING, T0), dry_run=False)
    assert opened.decision == AlertDecision.NEW
    assert opened.occurrence_count == 1
    assert len(transport.telegram) == 1
    row = publisher.manager.get(KEY)
    assert row is not None
    assert row.state == "OPEN"
    assert row.last_notified_at == T0

    # ---- T+1m: same failure inside the 30m WARNING cooldown is suppressed
    repeat_early = publisher.publish(_event(AlertSeverity.WARNING, T0 + timedelta(minutes=1)), dry_run=False)
    assert repeat_early.decision == AlertDecision.SUPPRESSED
    assert repeat_early.occurrence_count == 2
    assert len(transport.telegram) == 1

    # ---- T+30m: severity jump escalates immediately regardless of cooldown
    escalated = publisher.publish(_event(AlertSeverity.CRITICAL, T0 + timedelta(minutes=30)), dry_run=False)
    assert escalated.decision == AlertDecision.ESCALATED
    assert escalated.severity == AlertSeverity.CRITICAL
    assert escalated.occurrence_count == 3
    # CRITICAL fans out to Telegram and Slack.
    assert len(transport.telegram) == 2
    assert len(transport.slack) == 1

    # ---- T+35m: recovery is delivered exactly once with the full duration
    recovered = publisher.resolve(KEY, now=T0 + timedelta(minutes=35), dry_run=False)
    assert recovered is not None
    assert recovered.decision == AlertDecision.RECOVERED
    assert recovered.duration_seconds == pytest.approx(35 * 60)
    assert len(transport.telegram) == 3
    assert "RESOLVED" in transport.telegram[-1]

    row = publisher.manager.get(KEY)
    assert row is not None
    assert row.state == "RECOVERED"
    assert row.resolved_at == T0 + timedelta(minutes=35)

    # ---- T+36m: reconciling a recovered incident produces nothing
    before = (len(transport.telegram), len(transport.slack))
    recovered_again = publisher.reconcile(set(), source=AlertSource.QUALITY, now=T0 + timedelta(minutes=36))
    assert recovered_again == []
    assert (len(transport.telegram), len(transport.slack)) == before


def test_reopened_incident_starts_a_new_lifecycle(session: Session, transport: RecordingTransport) -> None:
    """A failure after recovery reopens with reset counters and a fresh delivery."""
    publisher = AlertPublisher(session)

    publisher.publish(_event(AlertSeverity.ERROR, T0), dry_run=False)
    publisher.resolve(KEY, now=T0 + timedelta(minutes=10), dry_run=False)

    reopened_at = T0 + timedelta(hours=2)
    reopened = publisher.publish(_event(AlertSeverity.ERROR, reopened_at), dry_run=False)

    assert reopened.decision == AlertDecision.NEW
    assert reopened.occurrence_count == 1
    assert reopened.first_opened_at == reopened_at
    # open + recovery + reopen
    assert len(transport.telegram) == 3

    row = publisher.manager.get(KEY)
    assert row is not None
    assert row.state == "OPEN"
    assert row.resolved_at is None


def test_restart_preserves_open_incident_and_cooldown(session: Session, transport: RecordingTransport) -> None:
    """A new publisher instance (simulated restart) does not re-announce."""
    first = AlertPublisher(session)
    first.publish(_event(AlertSeverity.ERROR, T0), dry_run=False)
    assert len(transport.telegram) == 1

    # A fresh publisher over the same durable state must not re-announce.
    restarted = AlertPublisher(session)
    later = T0 + timedelta(minutes=1)
    transition = restarted.publish(_event(AlertSeverity.ERROR, later), dry_run=False)

    assert transition.decision == AlertDecision.SUPPRESSED
    assert transition.occurrence_count == 2
    assert len(transport.telegram) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
