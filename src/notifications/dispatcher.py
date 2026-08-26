"""Unified Notification Dispatcher for Telegram, Slack, Webhook, and Console alerting."""

from __future__ import annotations

import hashlib
import logging
import os
import time

from src.notifications.dto import (
    NotificationBatchReport,
    NotificationChannel,
    NotificationDispatchResult,
    NotificationMessage,
)
from src.utils.alerting import SlackWebhookClient, TelegramBotClient

logger = logging.getLogger(__name__)


class NotificationDispatcher:
    """Orchestrates multi-channel alert delivery, deduplication, and suppression."""

    def __init__(self) -> None:
        """Initialize notification dispatcher with deduplication cache."""
        self._sent_cache: dict[str, float] = {}

    def _get_fingerprint(self, message: NotificationMessage) -> str:
        """Compute unique fingerprint for alert deduplication."""
        raw = f"{message.channel}:{message.title}:{message.body}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def is_suppressed(self, message: NotificationMessage, window_seconds: int = 300) -> bool:
        """Check if identical notification was sent within the time window."""
        fp = self._get_fingerprint(message)
        now = time.monotonic()
        last_sent = self._sent_cache.get(fp)

        if last_sent is not None and (now - last_sent) < window_seconds:
            return True

        self._sent_cache[fp] = now
        return False

    def send_telegram(
        self,
        message: NotificationMessage,
        *,
        dry_run: bool = False,
    ) -> NotificationDispatchResult:
        """Dispatch notification to Telegram channel."""
        start_mono = time.monotonic()
        if dry_run:
            return NotificationDispatchResult(
                channel=NotificationChannel.TELEGRAM,
                status="DRY_RUN",
                duration_seconds=time.monotonic() - start_mono,
            )

        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = message.recipient_id or os.getenv("TELEGRAM_CHAT_ID")

        if not token or not chat_id:
            logger.info("Telegram credentials missing; skipping Telegram dispatch.")
            return NotificationDispatchResult(
                channel=NotificationChannel.TELEGRAM,
                status="SKIPPED",
                duration_seconds=time.monotonic() - start_mono,
            )

        formatted = f"*{message.title}*\n\n{message.body}"
        success = TelegramBotClient.send_message(message=formatted, chat_id=chat_id)
        duration = time.monotonic() - start_mono

        return NotificationDispatchResult(
            channel=NotificationChannel.TELEGRAM,
            status="SENT" if success else "FAILED",
            duration_seconds=duration,
            error_message=None if success else "TelegramBotClient failed to send message.",
        )

    def send_slack(
        self,
        message: NotificationMessage,
        *,
        dry_run: bool = False,
    ) -> NotificationDispatchResult:
        """Dispatch notification to Slack webhook."""
        start_mono = time.monotonic()
        if dry_run:
            return NotificationDispatchResult(
                channel=NotificationChannel.SLACK,
                status="DRY_RUN",
                duration_seconds=time.monotonic() - start_mono,
            )

        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            logger.info("Slack webhook URL missing; skipping Slack dispatch.")
            return NotificationDispatchResult(
                channel=NotificationChannel.SLACK,
                status="SKIPPED",
                duration_seconds=time.monotonic() - start_mono,
            )

        formatted = f"*{message.title}*\n{message.body}"
        success = SlackWebhookClient.send_alert(message=formatted)
        duration = time.monotonic() - start_mono

        return NotificationDispatchResult(
            channel=NotificationChannel.SLACK,
            status="SENT" if success else "FAILED",
            duration_seconds=duration,
            error_message=None if success else "SlackWebhookClient failed to send alert.",
        )

    def send_console(
        self,
        _message: NotificationMessage,
        *,
        dry_run: bool = False,
    ) -> NotificationDispatchResult:
        """Process console channel notification."""
        start_mono = time.monotonic()
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
    ) -> NotificationDispatchResult:
        """Dispatch single notification based on its configured channel."""
        if not dry_run and self.is_suppressed(message, window_seconds=suppress_window):
            return NotificationDispatchResult(
                channel=message.channel,
                status="SUPPRESSED",
                duration_seconds=0.0,
            )

        if message.channel == NotificationChannel.TELEGRAM:
            return self.send_telegram(message, dry_run=dry_run)
        if message.channel == NotificationChannel.SLACK:
            return self.send_slack(message, dry_run=dry_run)
        return self.send_console(message, dry_run=dry_run)

    def dispatch_batch(
        self,
        messages: list[NotificationMessage],
        *,
        dry_run: bool = False,
        suppress_window: int = 300,
    ) -> NotificationBatchReport:
        """Dispatch multiple notifications and aggregate results into a batch report."""
        results: list[NotificationDispatchResult] = []

        for msg in messages:
            res = self.dispatch(msg, dry_run=dry_run, suppress_window=suppress_window)
            results.append(res)

        sent = sum(1 for r in results if r.status in ("SENT", "DRY_RUN"))
        failed = sum(1 for r in results if r.status == "FAILED")
        suppressed = sum(1 for r in results if r.status == "SUPPRESSED")

        return NotificationBatchReport(
            total_messages=len(messages),
            sent_count=sent,
            failed_count=failed,
            suppressed_count=suppressed,
            results=results,
        )
