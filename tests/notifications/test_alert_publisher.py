"""End-to-end tests for AlertPublisher: publish -> delivery -> recovery."""

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

BASE_TIME = datetime(2026, 9, 25, 4, 45, 0)


@pytest.fixture
def session_factory():
    """File-based SQLite so the recorder gets an independent connection."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
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


class _RecordingTransport:
    """Record deliveries instead of hitting the network."""

    def __init__(self, outcome: DeliveryOutcome = DeliveryOutcome.SENT) -> None:
        self.outcome = outcome
        self.telegram: list[str] = []
        self.slack: list[str] = []

    def telegram_deliver(self, message: str, chat_id: str | None = None) -> DeliveryResult:
        self.telegram.append(message)
        return DeliveryResult(channel="telegram", outcome=self.outcome, attempts=1)

    def slack_deliver_webhook(self, message: str, blocks=None) -> DeliveryResult:
        self.slack.append(message)
        return DeliveryResult(channel="slack", outcome=self.outcome, attempts=1)


@pytest.fixture
def transport(monkeypatch: pytest.MonkeyPatch) -> _RecordingTransport:
    rec = _RecordingTransport()
    monkeypatch.setattr("src.utils.alerting.TelegramBotClient.deliver", rec.telegram_deliver)
    monkeypatch.setattr("src.utils.alerting.SlackWebhookClient.deliver_webhook", rec.slack_deliver_webhook)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "ops-chat")
    return rec


def _event(
    *,
    key: str = "integrity:game_stats:20260925",
    severity: AlertSeverity = AlertSeverity.ERROR,
    source: AlertSource = AlertSource.INTEGRITY,
    occurred_at: datetime | None = None,
) -> AlertEvent:
    return AlertEvent(
        source=source,
        component="game_stats",
        severity=severity,
        title="Data integrity failure",
        message="3 integrity checks failed",
        incident_key=key,
        occurred_at=occurred_at or BASE_TIME,
    )


class TestPublish:
    def test_publish_delivers_and_records(self, session: Session, transport: _RecordingTransport) -> None:
        publisher = AlertPublisher(session)
        transition = publisher.publish(_event(), dry_run=False)

        assert transition.decision == AlertDecision.NEW
        assert len(transport.telegram) == 1
        assert "Data integrity failure" in transport.telegram[0]
        assert "integrity:game_stats:20260925" in transport.telegram[0]

        incident = publisher.manager.get("integrity:game_stats:20260925")
        assert incident is not None
        assert incident.notification_count == 1
        assert incident.last_notified_at is not None

    def test_repeat_within_cooldown_is_not_delivered(
        self,
        session: Session,
        transport: _RecordingTransport,
    ) -> None:
        publisher = AlertPublisher(session)
        publisher.publish(_event(), dry_run=False)

        later = BASE_TIME + timedelta(minutes=2)
        transition = publisher.publish(_event(occurred_at=later), dry_run=False)

        assert transition.decision == AlertDecision.SUPPRESSED
        assert len(transport.telegram) == 1

    def test_critical_fans_out_to_telegram_and_slack(
        self,
        session: Session,
        transport: _RecordingTransport,
    ) -> None:
        publisher = AlertPublisher(session)
        publisher.publish(_event(severity=AlertSeverity.CRITICAL), dry_run=False)

        assert len(transport.telegram) == 1
        assert len(transport.slack) == 1

    def test_dry_run_does_not_deliver_or_mark(
        self,
        session: Session,
        transport: _RecordingTransport,
    ) -> None:
        publisher = AlertPublisher(session)
        publisher.publish(_event(), dry_run=True)

        assert transport.telegram == []
        incident = publisher.manager.get("integrity:game_stats:20260925")
        assert incident is not None
        assert incident.last_notified_at is None

    def test_failed_delivery_is_rertied_next_run(
        self,
        session: Session,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        rec = _RecordingTransport(outcome=DeliveryOutcome.FAILED)
        monkeypatch.setattr("src.utils.alerting.TelegramBotClient.deliver", rec.telegram_deliver)
        monkeypatch.setattr("src.utils.alerting.SlackWebhookClient.deliver_webhook", rec.slack_deliver_webhook)
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "ops-chat")

        publisher = AlertPublisher(session)
        publisher.publish(_event(), dry_run=False)
        incident = publisher.manager.get("integrity:game_stats:20260925")
        assert incident is not None
        assert incident.last_notified_at is None

        rec.outcome = DeliveryOutcome.SENT
        later = BASE_TIME + timedelta(minutes=1)
        transition = publisher.publish(_event(occurred_at=later), dry_run=False)
        assert transition.decision == AlertDecision.REPEAT

    def test_unconfigured_is_not_retried_every_run(
        self,
        session: Session,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        rec = _RecordingTransport(outcome=DeliveryOutcome.SKIPPED_UNCONFIGURED)
        monkeypatch.setattr("src.utils.alerting.TelegramBotClient.deliver", rec.telegram_deliver)
        monkeypatch.setattr("src.utils.alerting.SlackWebhookClient.deliver_webhook", rec.slack_deliver_webhook)
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "ops-chat")

        publisher = AlertPublisher(session)
        publisher.publish(_event(), dry_run=False)
        incident = publisher.manager.get("integrity:game_stats:20260925")
        assert incident is not None
        assert incident.last_notified_at is not None


class TestRecovery:
    def test_resolve_delivers_recovery_once(
        self,
        session: Session,
        transport: _RecordingTransport,
    ) -> None:
        publisher = AlertPublisher(session)
        publisher.publish(_event(), dry_run=False)
        transport.telegram.clear()

        recovered_at = BASE_TIME + timedelta(minutes=16)
        import src.notifications.incident as incident_module

        original = incident_module.utcnow
        try:
            incident_module.utcnow = lambda: recovered_at
            transition = publisher.resolve("integrity:game_stats:20260925", dry_run=False)
        finally:
            incident_module.utcnow = original

        assert transition is not None
        assert transition.decision == AlertDecision.RECOVERED
        assert len(transport.telegram) == 1
        assert "RESOLVED" in transport.telegram[0]

    def test_reconcile_recovers_missing_keys(
        self,
        session: Session,
        transport: _RecordingTransport,
    ) -> None:
        publisher = AlertPublisher(session)
        publisher.publish(_event(key="integrity:a:20260925"), dry_run=False)
        publisher.publish(_event(key="integrity:b:20260925"), dry_run=False)
        transport.telegram.clear()

        recovered = publisher.reconcile({"integrity:a:20260925"}, source=AlertSource.INTEGRITY, dry_run=False)

        assert [t.incident_key for t in recovered] == ["integrity:b:20260925"]
        assert len(transport.telegram) == 1
        assert "RESOLVED" in transport.telegram[0]

    def test_reconcile_with_all_active_is_noop(
        self,
        session: Session,
        transport: _RecordingTransport,
    ) -> None:
        publisher = AlertPublisher(session)
        publisher.publish(_event(), dry_run=False)
        transport.telegram.clear()

        recovered = publisher.reconcile({"integrity:game_stats:20260925"}, source=AlertSource.INTEGRITY)

        assert recovered == []
        assert transport.telegram == []


class TestMetrics:
    def test_refresh_metrics_does_not_raise(self, session: Session, transport: _RecordingTransport) -> None:
        publisher = AlertPublisher(session)
        publisher.publish(_event(), dry_run=False)
        publisher.refresh_metrics()


class TestDispatcherInjection:
    def test_custom_dispatcher_is_used(self, session: Session, transport: _RecordingTransport) -> None:
        dispatcher = NotificationDispatcher()
        publisher = AlertPublisher(session, dispatcher=dispatcher)
        assert publisher.dispatcher is dispatcher


class TestPublishResultContract:
    """Per-channel delivery outcomes are preserved on the result."""

    def test_critical_fanout_reports_each_channel(
        self,
        session: Session,
        transport: _RecordingTransport,
    ) -> None:
        publisher = AlertPublisher(session)
        result = publisher.publish(_event(severity=AlertSeverity.CRITICAL), dry_run=False)

        assert result.delivery_report is not None
        statuses = {r.channel.value: r.status for r in result.delivery_report.results}
        assert statuses == {"telegram": "SENT", "slack": "SENT"}

    def test_suppressed_result_has_no_delivery_report(
        self,
        session: Session,
        transport: _RecordingTransport,
    ) -> None:
        publisher = AlertPublisher(session)
        publisher.publish(_event(), dry_run=False)

        later = BASE_TIME + timedelta(minutes=1)
        suppressed = publisher.publish(_event(occurred_at=later), dry_run=False)

        assert suppressed.decision == AlertDecision.SUPPRESSED
        assert suppressed.delivery_report is None

    def test_failed_and_skipped_channels_are_distinguishable(
        self,
        session: Session,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            "src.utils.alerting.TelegramBotClient.deliver",
            lambda message, chat_id=None: DeliveryResult(
                channel="telegram",
                outcome=DeliveryOutcome.FAILED,
                attempts=3,
                error="boom",
            ),
        )
        monkeypatch.setattr(
            "src.utils.alerting.SlackWebhookClient.deliver_webhook",
            lambda message, blocks=None: DeliveryResult(
                channel="slack",
                outcome=DeliveryOutcome.SKIPPED_UNCONFIGURED,
            ),
        )
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "ops-chat")

        publisher = AlertPublisher(session)
        result = publisher.publish(_event(severity=AlertSeverity.CRITICAL), dry_run=False)

        assert result.delivery_report is not None
        statuses = {r.channel.value: r.status for r in result.delivery_report.results}
        assert statuses == {"telegram": "FAILED", "slack": "SKIPPED_UNCONFIGURED"}
        assert result.delivery_report.failed_count == 1

    def test_result_to_dict_includes_delivery_report(
        self,
        session: Session,
        transport: _RecordingTransport,
    ) -> None:
        publisher = AlertPublisher(session)
        payload = publisher.publish(_event(severity=AlertSeverity.CRITICAL), dry_run=False).to_dict()

        assert payload["incident"]["incident_key"] == "integrity:game_stats:20260925"
        assert payload["delivery_report"]["sent_count"] == 2


class TestTimeThreading:
    """`now` is dependency-injected through the whole publisher operation."""

    def test_publish_uses_event_time_for_last_notified_at(
        self,
        session: Session,
        transport: _RecordingTransport,
    ) -> None:
        publisher = AlertPublisher(session)
        publisher.publish(_event(occurred_at=BASE_TIME), dry_run=False)

        incident = publisher.manager.get("integrity:game_stats:20260925")
        assert incident is not None
        assert incident.last_notified_at == BASE_TIME
        assert incident.first_opened_at == BASE_TIME

    def test_resolve_uses_provided_now(
        self,
        session: Session,
        transport: _RecordingTransport,
    ) -> None:
        publisher = AlertPublisher(session)
        publisher.publish(_event(occurred_at=BASE_TIME), dry_run=False)

        recovered_at = BASE_TIME + timedelta(minutes=35)
        transition = publisher.resolve("integrity:game_stats:20260925", now=recovered_at, dry_run=False)

        assert transition is not None
        assert transition.duration_seconds == pytest.approx(35 * 60)
        incident = publisher.manager.get("integrity:game_stats:20260925")
        assert incident is not None
        assert incident.resolved_at == recovered_at

    def test_reconcile_uses_provided_now(
        self,
        session: Session,
        transport: _RecordingTransport,
    ) -> None:
        publisher = AlertPublisher(session)
        publisher.publish(_event(key="integrity:a:20260925", occurred_at=BASE_TIME), dry_run=False)
        publisher.publish(_event(key="integrity:b:20260925", occurred_at=BASE_TIME), dry_run=False)

        reconcile_at = BASE_TIME + timedelta(minutes=10)
        recovered = publisher.reconcile({"integrity:a:20260925"}, source=AlertSource.INTEGRITY, now=reconcile_at)

        assert [t.incident_key for t in recovered] == ["integrity:b:20260925"]
        assert recovered[0].last_seen_at == reconcile_at

    def test_acknowledge_uses_provided_now(
        self,
        session: Session,
        transport: _RecordingTransport,
    ) -> None:
        publisher = AlertPublisher(session)
        publisher.publish(_event(occurred_at=BASE_TIME), dry_run=False)

        ack_at = BASE_TIME + timedelta(minutes=5)
        transition = publisher.acknowledge("integrity:game_stats:20260925", now=ack_at)

        assert transition is not None
        incident = publisher.manager.get("integrity:game_stats:20260925")
        assert incident is not None
        assert incident.last_seen_at == ack_at

    def test_publish_without_now_uses_one_instant_for_incident_and_delivery(
        self,
        session: Session,
        transport: _RecordingTransport,
    ) -> None:
        publisher = AlertPublisher(session)
        transition = publisher.publish(_event(occurred_at=None), dry_run=False)

        incident = publisher.manager.get("integrity:game_stats:20260925")
        assert incident is not None
        assert incident.last_notified_at == transition.last_seen_at


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
