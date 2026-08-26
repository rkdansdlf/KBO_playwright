"""Unit tests for src.notifications.dto."""

from __future__ import annotations

from src.notifications.dto import (
    NotificationBatchReport,
    NotificationChannel,
    NotificationDispatchResult,
    NotificationMessage,
    NotificationPriority,
)


def test_notification_channel_values() -> None:
    assert NotificationChannel.TELEGRAM == "telegram"
    assert NotificationChannel.SLACK == "slack"
    assert NotificationChannel.WEBHOOK == "webhook"
    assert NotificationChannel.CONSOLE == "console"


def test_notification_priority_values() -> None:
    assert NotificationPriority.LOW == "low"
    assert NotificationPriority.NORMAL == "normal"
    assert NotificationPriority.HIGH == "high"
    assert NotificationPriority.CRITICAL == "critical"


def test_notification_message_to_dict() -> None:
    msg = NotificationMessage(
        title="Test Alert",
        body="Detailed test alert body",
        priority=NotificationPriority.HIGH,
        channel=NotificationChannel.TELEGRAM,
    )
    d = msg.to_dict()
    assert d["title"] == "Test Alert"
    assert d["priority"] == "high"
    assert d["channel"] == "telegram"


def test_notification_dispatch_result_to_dict() -> None:
    res = NotificationDispatchResult(
        channel=NotificationChannel.SLACK,
        status="SENT",
        duration_seconds=0.123,
    )
    d = res.to_dict()
    assert d["channel"] == "slack"
    assert d["status"] == "SENT"
    assert d["duration_seconds"] == 0.123


def test_notification_batch_report_to_dict() -> None:
    rep = NotificationBatchReport(
        total_messages=2,
        sent_count=2,
        failed_count=0,
        suppressed_count=0,
        results=[],
    )
    d = rep.to_dict()
    assert d["total_messages"] == 2
    assert d["sent_count"] == 2
