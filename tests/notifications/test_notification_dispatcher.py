"""Unit tests for src.notifications.dispatcher."""

from __future__ import annotations

from src.notifications.dispatcher import NotificationDispatcher
from src.notifications.dto import (
    NotificationChannel,
    NotificationMessage,
    NotificationPriority,
)


def test_notification_dispatcher_dry_run() -> None:
    dispatcher = NotificationDispatcher()
    msg = NotificationMessage(
        title="Test Matchup",
        body="KIA vs LG Game Started",
        priority=NotificationPriority.NORMAL,
        channel=NotificationChannel.TELEGRAM,
    )
    res = dispatcher.dispatch(msg, dry_run=True)
    assert res.status == "DRY_RUN"
    assert res.channel == NotificationChannel.TELEGRAM


def test_notification_dispatcher_suppression() -> None:
    dispatcher = NotificationDispatcher()
    msg = NotificationMessage(
        title="Duplicate Alert",
        body="Alert body for deduplication",
        priority=NotificationPriority.HIGH,
        channel=NotificationChannel.CONSOLE,
    )

    # First dispatch should succeed
    res1 = dispatcher.dispatch(msg, dry_run=False, suppress_window=10)
    assert res1.status == "SENT"

    # Immediate second dispatch with same payload should be suppressed
    res2 = dispatcher.dispatch(msg, dry_run=False, suppress_window=10)
    assert res2.status == "SUPPRESSED"


def test_notification_dispatch_batch() -> None:
    dispatcher = NotificationDispatcher()
    messages = [
        NotificationMessage(title="Msg 1", body="Body 1", channel=NotificationChannel.CONSOLE),
        NotificationMessage(title="Msg 2", body="Body 2", channel=NotificationChannel.CONSOLE),
    ]

    report = dispatcher.dispatch_batch(messages, dry_run=True)
    assert report.total_messages == 2
    assert report.sent_count == 2
    assert report.failed_count == 0
