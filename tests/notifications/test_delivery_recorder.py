"""Delivery audit ledger tests: persistence, isolation and survival.

Uses a **file-based** SQLite database so the recorder's independent transaction
runs on a separate connection from the caller — that is what makes the
"audit survives a caller rollback" contract meaningful.
"""

from __future__ import annotations

import os
from datetime import datetime

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.notification_delivery import NotificationDelivery
from src.models.notification_incident import NotificationIncident
from src.notifications.alert_dto import AlertEvent, AlertSeverity, AlertSource
from src.notifications.dispatcher import NotificationDispatcher
from src.notifications.dto import NotificationChannel, NotificationMessage, NotificationPriority
from src.notifications.publisher import AlertPublisher
from src.notifications.recorder import DeliveryRecorder
from src.notifications.standalone import send_notification
from src.utils.alerting import DeliveryOutcome, DeliveryResult

T0 = datetime(2026, 9, 25, 4, 45, 0)


class RecordingTransport:
    def __init__(self, telegram: DeliveryOutcome = DeliveryOutcome.SENT, slack: DeliveryOutcome = DeliveryOutcome.SENT):
        self.telegram_outcome = telegram
        self.slack_outcome = slack
        self.telegram: list[str] = []
        self.slack: list[str] = []

    def telegram_deliver(self, message: str, chat_id: str | None = None) -> DeliveryResult:
        self.telegram.append(message)
        return DeliveryResult(channel="telegram", outcome=self.telegram_outcome, attempts=1)

    def slack_deliver_webhook(self, message: str, blocks=None) -> DeliveryResult:
        self.slack.append(message)
        return DeliveryResult(channel="slack", outcome=self.slack_outcome, attempts=1)


@pytest.fixture
def session_factory(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'delivery.db'}", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


@pytest.fixture
def transport(monkeypatch: pytest.MonkeyPatch) -> RecordingTransport:
    rec = RecordingTransport()
    monkeypatch.setattr("src.utils.alerting.TelegramBotClient.deliver", rec.telegram_deliver)
    monkeypatch.setattr("src.utils.alerting.SlackWebhookClient.deliver_webhook", rec.slack_deliver_webhook)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "ops-chat")
    return rec


def _message(priority: NotificationPriority = NotificationPriority.NORMAL, **overrides) -> NotificationMessage:
    payload = {
        "title": "제목",
        "body": "본문",
        "priority": priority,
        "channel": NotificationChannel.TELEGRAM,
        "notification_type": "digest",
    }
    payload.update(overrides)
    return NotificationMessage(**payload)


def _rows(session_factory) -> list[NotificationDelivery]:
    with session_factory() as session:
        return list(session.execute(select(NotificationDelivery)).scalars().all())


class TestRecorderPersistence:
    def test_single_channel_records_one_row(self, session_factory, transport) -> None:
        dispatcher = NotificationDispatcher(recorder=DeliveryRecorder(session_factory))
        dispatcher.dispatch(_message(), dry_run=False)

        rows = _rows(session_factory)
        assert len(rows) == 1
        assert rows[0].channel == "telegram"
        assert rows[0].status == "SENT"
        assert rows[0].notification_type == "digest"

    def test_critical_fanout_shares_one_batch_id(self, session_factory, transport) -> None:
        dispatcher = NotificationDispatcher(recorder=DeliveryRecorder(session_factory))
        dispatcher.dispatch_by_priority(_message(NotificationPriority.CRITICAL), dry_run=False)

        rows = _rows(session_factory)
        assert {r.channel for r in rows} == {"telegram", "slack"}
        assert len({r.batch_id for r in rows}) == 1
        assert all(r.status == "SENT" for r in rows)

    def test_suppressed_is_not_recorded(self, session_factory, transport) -> None:
        dispatcher = NotificationDispatcher(recorder=DeliveryRecorder(session_factory))
        dispatcher.dispatch(_message(), dry_run=False)
        dispatcher.dispatch(_message(), dry_run=False, suppress_window=300)

        rows = _rows(session_factory)
        assert len(rows) == 1

    def test_dry_run_is_recorded(self, session_factory, transport) -> None:
        dispatcher = NotificationDispatcher(recorder=DeliveryRecorder(session_factory))
        dispatcher.dispatch(_message(), dry_run=True)

        rows = _rows(session_factory)
        assert len(rows) == 1
        assert rows[0].status == "DRY_RUN"

    def test_skipped_unconfigured_is_recorded(self, session_factory, monkeypatch) -> None:
        monkeypatch.setattr(
            "src.utils.alerting.TelegramBotClient.deliver",
            lambda message, chat_id=None: DeliveryResult(
                channel="telegram",
                outcome=DeliveryOutcome.SKIPPED_UNCONFIGURED,
            ),
        )
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

        dispatcher = NotificationDispatcher(recorder=DeliveryRecorder(session_factory))
        dispatcher.dispatch(_message(), dry_run=False)

        rows = _rows(session_factory)
        assert len(rows) == 1
        assert rows[0].status == "SKIPPED_UNCONFIGURED"

    def test_failed_status_and_error_are_persisted(self, session_factory, monkeypatch) -> None:
        monkeypatch.setattr(
            "src.utils.alerting.TelegramBotClient.deliver",
            lambda message, chat_id=None: DeliveryResult(
                channel="telegram",
                outcome=DeliveryOutcome.FAILED,
                attempts=3,
                error="HTTP 500",
            ),
        )

        dispatcher = NotificationDispatcher(recorder=DeliveryRecorder(session_factory))
        dispatcher.dispatch(_message(), dry_run=False)

        rows = _rows(session_factory)
        assert len(rows) == 1
        assert rows[0].status == "FAILED"
        assert rows[0].attempt_count == 3
        assert rows[0].error_message == "HTTP 500"

    def test_incident_id_soft_reference_roundtrip(self, session_factory) -> None:
        recorder = DeliveryRecorder(session_factory)
        dispatcher = NotificationDispatcher(recorder=recorder)
        dispatcher.dispatch(_message(), dry_run=True, incident_id=4242)

        with session_factory() as session:
            row = session.execute(
                select(NotificationDelivery).where(NotificationDelivery.incident_id == 4242),
            ).scalar_one()
        assert row.incident_id == 4242


class TestRecorderIsolation:
    def test_audit_failure_returns_zero_without_raising(self, transport) -> None:
        def broken_factory():
            raise RuntimeError("db down")

        recorder = DeliveryRecorder(broken_factory)
        dispatcher = NotificationDispatcher(recorder=recorder)

        report = dispatcher.dispatch(_message(), dry_run=False)
        assert report.status == "SENT"


class TestAuditSurvivesCallerRollback:
    @pytest.mark.integration
    def test_delivery_audit_survives_caller_transaction_rollback(self, transport) -> None:
        """A real external send must remain audited even if the caller rolls back.

        SQLite serializes writers, so an independent audit connection cannot
        commit while the caller holds an open write transaction. The authoritative
        proof therefore runs on a concurrent-writer backend (PostgreSQL); the
        SQLite lane skips it.
        """
        url = os.getenv("KBO_E2E_DATABASE_URL") or os.getenv("DATABASE_URL") or ""
        if not url or url.startswith("sqlite"):
            pytest.skip("requires a concurrent-writer backend (PostgreSQL)")

        engine = create_engine(url, pool_pre_ping=True)
        Base.metadata.create_all(bind=engine)
        maker = sessionmaker(bind=engine, expire_on_commit=False)
        key = "quality:delivery-rollback-probe"

        with maker() as probe:
            max_delivery_id = probe.execute(select(func.max(NotificationDelivery.id))).scalar() or 0

        session = maker()
        publisher = AlertPublisher(session, recorder=DeliveryRecorder(maker))
        event = AlertEvent(
            source=AlertSource.QUALITY,
            component="daily",
            severity=AlertSeverity.ERROR,
            title="품질 게이트",
            message="score 40",
            incident_key=key,
            occurred_at=T0,
        )

        try:
            publisher.publish(event, now=T0, dry_run=False)
            assert len(transport.telegram) == 1  # the side effect happened

            session.rollback()  # discard the incident/business transaction
            session.close()

            with maker() as check:
                deliveries = list(
                    check.execute(
                        select(NotificationDelivery).where(NotificationDelivery.id > max_delivery_id),
                    ).scalars(),
                )
                incident = check.execute(
                    select(NotificationIncident).where(NotificationIncident.incident_key == key),
                ).scalar_one_or_none()
        finally:
            with engine.begin() as cleanup:
                cleanup.execute(
                    NotificationDelivery.__table__.delete().where(NotificationDelivery.id > max_delivery_id),
                )
                cleanup.execute(
                    NotificationIncident.__table__.delete().where(NotificationIncident.incident_key == key),
                )

        assert len(deliveries) == 1
        assert deliveries[0].status == "SENT"
        assert deliveries[0].notification_type == "alert"
        assert incident is None


class TestStandaloneRecording:
    def test_standalone_notification_records_with_null_incident_id(self, session_factory, transport) -> None:
        report = send_notification(
            "일일 요약",
            "정상",
            session_factory=session_factory,
        )

        assert report.sent_count == 1
        rows = _rows(session_factory)
        assert len(rows) == 1
        assert rows[0].incident_id is None
        assert rows[0].notification_type == "notification"

    def test_standalone_falls_back_to_slack_when_telegram_unconfigured(
        self,
        session_factory,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(
            "src.utils.alerting.TelegramBotClient.deliver",
            lambda message, chat_id=None: DeliveryResult(
                channel="telegram",
                outcome=DeliveryOutcome.SKIPPED_UNCONFIGURED,
            ),
        )
        monkeypatch.setattr(
            "src.utils.alerting.SlackWebhookClient.deliver_webhook",
            lambda message, blocks=None: DeliveryResult(channel="slack", outcome=DeliveryOutcome.SENT, attempts=1),
        )
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

        send_notification("요약", "본문", session_factory=session_factory)

        rows = {r.channel: r.status for r in _rows(session_factory)}
        # The skipped Telegram attempt and the successful Slack fallback are both audited.
        assert rows == {"telegram": "SKIPPED_UNCONFIGURED", "slack": "SENT"}


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
