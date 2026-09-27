"""Unified Notification Dispatcher for Telegram, Slack, Webhook, and Console alerting.

This is the application's single outbound delivery entry point. Monitoring code
publishes an incident through :class:`src.notifications.publisher.AlertPublisher`,
which composes this dispatcher; nothing outside the transport adapters should
talk to Telegram or Slack directly.

Responsibilities: render plain text for the target channel (Telegram HTML vs
Slack mrkdwn), escape it correctly, fan a priority out to the right channels,
record delivery metrics, and hand the completed batch to an optional
:class:`~src.notifications.recorder.DeliveryRecorder`.

Recording happens once per completed batch, so a CRITICAL fan-out shares a single
``batch_id``. The recorder owns its own transaction, so an audit failure can
never contaminate the transport result. Deduplication here is only a short
backstop — the authoritative cooldown lives in
:class:`src.notifications.incident.IncidentManager`.
"""

from __future__ import annotations

import hashlib
import html
import logging
import os
import time
from collections import OrderedDict
from dataclasses import replace
from typing import TYPE_CHECKING

from src.notifications.dto import (
    NotificationBatchReport,
    NotificationChannel,
    NotificationDispatchResult,
    NotificationMessage,
    NotificationPriority,
)
from src.utils.alerting import (
    GenericWebhookClient,
    SlackWebhookClient,
    TelegramBotClient,
)

if TYPE_CHECKING:
    from src.notifications.recorder import DeliveryRecorder

logger = logging.getLogger(__name__)

#: Maximum number of fingerprints retained in the per-dispatcher backstop cache.
_DEDUP_MAX_ENTRIES = 1024

#: Non-secret destination labels for channels whose real address is a URL.
_SLACK_DESTINATION = "slack-webhook"
_WEBHOOK_DESTINATION = "generic-webhook"


def _escape_html(text: str) -> str:
    return html.escape(text, quote=False)


def _escape_mrkdwn(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _render_telegram(message: NotificationMessage) -> str:
    return f"<b>{_escape_html(message.title)}</b>\n\n{_escape_html(message.body)}"


def _render_slack(message: NotificationMessage) -> str:
    return f"*{_escape_mrkdwn(message.title)}*\n{_escape_mrkdwn(message.body)}"


def channels_for_priority(priority: NotificationPriority) -> tuple[NotificationChannel, ...]:
    """Return the channels a priority fans out to.

    CRITICAL goes to every channel; INFO stays on the console so low-severity
    events never page an operator.
    """
    if priority == NotificationPriority.CRITICAL:
        return (NotificationChannel.TELEGRAM, NotificationChannel.SLACK)
    if priority == NotificationPriority.HIGH:
        return (NotificationChannel.TELEGRAM,)
    if priority == NotificationPriority.NORMAL:
        return (NotificationChannel.TELEGRAM,)
    return (NotificationChannel.CONSOLE,)


def _build_report(results: list[NotificationDispatchResult]) -> NotificationBatchReport:
    """Aggregate per-channel results into a batch report."""
    sent = sum(1 for r in results if r.status in ("SENT", "DRY_RUN"))
    failed = sum(1 for r in results if r.status == "FAILED")
    suppressed = sum(1 for r in results if r.status == "SUPPRESSED")
    skipped = sum(1 for r in results if r.status == "SKIPPED_UNCONFIGURED")
    return NotificationBatchReport(
        total_messages=len(results),
        sent_count=sent,
        failed_count=failed,
        suppressed_count=suppressed,
        skipped_count=skipped,
        results=results,
    )


class NotificationDispatcher:
    """Orchestrates multi-channel alert delivery, deduplication, and suppression."""

    def __init__(self, *, recorder: DeliveryRecorder | None = None) -> None:
        """Initialize notification dispatcher with a bounded deduplication cache."""
        self._sent_cache: OrderedDict[str, float] = OrderedDict()
        self._recorder = recorder

    def _get_fingerprint(self, message: NotificationMessage) -> str:
        """Compute unique fingerprint for alert deduplication."""
        raw = f"{message.channel}:{message.title}:{message.body}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def is_suppressed(self, message: NotificationMessage, window_seconds: int = 300) -> bool:
        """Check if an identical notification was sent within the time window.

        Pure predicate: it does not mutate the cache. Use :meth:`_record_sent`
        after an actual delivery attempt.
        """
        fp = self._get_fingerprint(message)
        now = time.monotonic()
        last_sent = self._sent_cache.get(fp)
        return last_sent is not None and (now - last_sent) < window_seconds

    def _record_sent(self, message: NotificationMessage) -> None:
        """Record a delivery so the backstop window starts now, evicting old entries."""
        fp = self._get_fingerprint(message)
        self._sent_cache[fp] = time.monotonic()
        self._sent_cache.move_to_end(fp)
        while len(self._sent_cache) > _DEDUP_MAX_ENTRIES:
            self._sent_cache.popitem(last=False)

    def send_telegram(
        self,
        message: NotificationMessage,
        *,
        dry_run: bool = False,
    ) -> NotificationDispatchResult:
        """Dispatch notification to Telegram channel."""
        start_mono = time.monotonic()
        chat_id = message.recipient_id or os.getenv("TELEGRAM_CHAT_ID")
        if dry_run:
            return NotificationDispatchResult(
                channel=NotificationChannel.TELEGRAM,
                status="DRY_RUN",
                duration_seconds=time.monotonic() - start_mono,
                destination=chat_id,
            )

        result = TelegramBotClient.deliver(_render_telegram(message), chat_id=chat_id)
        duration = time.monotonic() - start_mono

        return NotificationDispatchResult(
            channel=NotificationChannel.TELEGRAM,
            status=result.outcome.value,
            duration_seconds=duration,
            error_message=result.error,
            attempt_count=result.attempts,
            destination=chat_id,
        )

    def send_slack(
        self,
        message: NotificationMessage,
        *,
        dry_run: bool = False,
    ) -> NotificationDispatchResult:
        """Dispatch notification directly to the Slack webhook."""
        start_mono = time.monotonic()
        if dry_run:
            return NotificationDispatchResult(
                channel=NotificationChannel.SLACK,
                status="DRY_RUN",
                duration_seconds=time.monotonic() - start_mono,
                destination=_SLACK_DESTINATION,
            )

        result = SlackWebhookClient.deliver_webhook(_render_slack(message))
        duration = time.monotonic() - start_mono

        return NotificationDispatchResult(
            channel=NotificationChannel.SLACK,
            status=result.outcome.value,
            duration_seconds=duration,
            error_message=result.error,
            attempt_count=result.attempts,
            destination=_SLACK_DESTINATION,
        )

    def send_webhook(
        self,
        message: NotificationMessage,
        *,
        dry_run: bool = False,
    ) -> NotificationDispatchResult:
        """Dispatch notification to the generic JSON webhook."""
        start_mono = time.monotonic()
        if dry_run:
            return NotificationDispatchResult(
                channel=NotificationChannel.WEBHOOK,
                status="DRY_RUN",
                duration_seconds=time.monotonic() - start_mono,
                destination=_WEBHOOK_DESTINATION,
            )

        result = GenericWebhookClient.deliver(_render_slack(message), payload={"metadata": message.metadata})
        duration = time.monotonic() - start_mono

        return NotificationDispatchResult(
            channel=NotificationChannel.WEBHOOK,
            status=result.outcome.value,
            duration_seconds=duration,
            error_message=result.error,
            attempt_count=result.attempts,
            destination=_WEBHOOK_DESTINATION,
        )

    def send_console(
        self,
        message: NotificationMessage,
        *,
        dry_run: bool = False,
    ) -> NotificationDispatchResult:
        """Process console channel notification."""
        start_mono = time.monotonic()
        if not dry_run:
            logger.info("[NOTIFY] %s: %s", message.title, message.body)
        return NotificationDispatchResult(
            channel=NotificationChannel.CONSOLE,
            status="DRY_RUN" if dry_run else "SENT",
            duration_seconds=time.monotonic() - start_mono,
        )

    def dispatch(
        self,
        message: NotificationMessage,
        *,
        dry_run: bool = False,
        suppress_window: int = 300,
        incident_id: int | None = None,
    ) -> NotificationDispatchResult:
        """Dispatch a single notification and record its delivery."""
        result = self._dispatch_one(message, dry_run=dry_run, suppress_window=suppress_window)
        self._record(
            _build_report([result]),
            notification_type=message.notification_type,
            incident_id=incident_id,
        )
        return result

    def dispatch_by_priority(
        self,
        message: NotificationMessage,
        *,
        dry_run: bool = False,
        suppress_window: int = 300,
        incident_id: int | None = None,
    ) -> NotificationBatchReport:
        """Fan a message out to the channels implied by its priority."""
        channels = channels_for_priority(message.priority)
        messages = [replace(message, channel=channel) for channel in channels]
        return self.dispatch_batch(
            messages,
            dry_run=dry_run,
            suppress_window=suppress_window,
            incident_id=incident_id,
        )

    def dispatch_batch(
        self,
        messages: list[NotificationMessage],
        *,
        dry_run: bool = False,
        suppress_window: int = 300,
        incident_id: int | None = None,
    ) -> NotificationBatchReport:
        """Dispatch multiple notifications and record the completed batch once."""
        results = [self._dispatch_one(msg, dry_run=dry_run, suppress_window=suppress_window) for msg in messages]
        report = _build_report(results)
        notification_type = messages[0].notification_type if messages else "notification"
        self._record(report, notification_type=notification_type, incident_id=incident_id)
        return report

    def _dispatch_one(
        self,
        message: NotificationMessage,
        *,
        dry_run: bool,
        suppress_window: int,
    ) -> NotificationDispatchResult:
        """Send one message without recording; used by both single and batch paths."""
        if not dry_run and self.is_suppressed(message, window_seconds=suppress_window):
            return NotificationDispatchResult(
                channel=message.channel,
                status="SUPPRESSED",
                duration_seconds=0.0,
            )

        result = self._dispatch_channel(message, dry_run=dry_run)
        if not dry_run:
            self._record_sent(message)
        return result

    def _dispatch_channel(
        self,
        message: NotificationMessage,
        *,
        dry_run: bool,
    ) -> NotificationDispatchResult:
        if message.channel == NotificationChannel.TELEGRAM:
            return self.send_telegram(message, dry_run=dry_run)
        if message.channel == NotificationChannel.SLACK:
            return self.send_slack(message, dry_run=dry_run)
        if message.channel == NotificationChannel.WEBHOOK:
            return self.send_webhook(message, dry_run=dry_run)
        return self.send_console(message, dry_run=dry_run)

    def _record(
        self,
        report: NotificationBatchReport,
        *,
        notification_type: str,
        incident_id: int | None,
    ) -> None:
        """Hand a completed batch to the recorder; never propagate its failures."""
        if self._recorder is None:
            return
        self._recorder.record(
            report,
            incident_id=incident_id,
            notification_type=notification_type,
        )
