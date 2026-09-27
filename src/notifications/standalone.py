"""Stateless notification entry point.

For one-shot digests, CLI reports and maintenance notices that have **no**
incident lifecycle. They must not go through :class:`AlertPublisher` (which owns
OPEN/RECOVERED state and cooldown), but they must still produce a delivery audit
row, so they share the dispatcher and recorder.

The layer contract is therefore::

    Domain -> NotificationDispatcher -> Transport        (stateless, this module)
    Domain -> AlertPublisher -> IncidentManager -> ...   (stateful, incidents)
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Any

from src.notifications.dispatcher import NotificationDispatcher, channels_for_priority
from src.notifications.dto import (
    NotificationBatchReport,
    NotificationChannel,
    NotificationMessage,
    NotificationPriority,
)
from src.notifications.recorder import DeliveryRecorder

if TYPE_CHECKING:
    from collections.abc import Callable


def send_notification(  # noqa: PLR0913 - keyword-only options for a public helper
    title: str,
    body: str,
    *,
    priority: NotificationPriority = NotificationPriority.NORMAL,
    notification_type: str = "notification",
    recipient_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    dry_run: bool = False,
    session_factory: Callable[[], object] | None = None,
) -> NotificationBatchReport:
    """Deliver a stateless notification and record its delivery history.

    Channels are chosen by priority. If every chosen channel is unconfigured, a
    Slack webhook is tried as a fallback, preserving the legacy
    Telegram-first-then-Slack behaviour.
    """
    if session_factory is None:
        from src.db.engine import SessionLocal

        session_factory = SessionLocal

    dispatcher = NotificationDispatcher(recorder=DeliveryRecorder(session_factory))
    message = NotificationMessage(
        title=title,
        body=body,
        priority=priority,
        channel=NotificationChannel.CONSOLE,
        recipient_id=recipient_id,
        metadata=metadata or {},
        notification_type=notification_type,
    )

    report = dispatcher.dispatch_by_priority(message, dry_run=dry_run)
    already_includes_slack = NotificationChannel.SLACK in channels_for_priority(priority)
    if report.sent_count == 0 and report.skipped_count == report.total_messages and not already_includes_slack:
        slack_message = replace(message, channel=NotificationChannel.SLACK)
        return dispatcher.dispatch_batch([slack_message], dry_run=dry_run)
    return report
