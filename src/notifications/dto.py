"""Standard Data Transfer Objects (DTOs) for Multi-Channel Notifications and Alerting."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class NotificationChannel(StrEnum):
    """Supported delivery channels for system notifications."""

    TELEGRAM = "telegram"
    SLACK = "slack"
    WEBHOOK = "webhook"
    CONSOLE = "console"


class NotificationPriority(StrEnum):
    """Priority levels for alert routing and suppression."""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class NotificationMessage:
    """Standard payload representing an alert or system notification."""

    title: str
    body: str
    priority: NotificationPriority = NotificationPriority.NORMAL
    channel: NotificationChannel = NotificationChannel.CONSOLE
    recipient_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert notification message to dictionary."""
        return {
            "title": self.title,
            "body": self.body,
            "priority": self.priority.value,
            "channel": self.channel.value,
            "recipient_id": self.recipient_id,
            "metadata": self.metadata,
            "timestamp": self.timestamp,
        }


@dataclass
class NotificationDispatchResult:
    """Outcome of dispatching a single notification."""

    channel: NotificationChannel
    status: str  # SENT, FAILED, SUPPRESSED, DRY_RUN
    duration_seconds: float = 0.0
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert dispatch result to dictionary."""
        return {
            "channel": self.channel.value,
            "status": self.status,
            "duration_seconds": round(self.duration_seconds, 3),
            "error_message": self.error_message,
        }


@dataclass
class NotificationBatchReport:
    """Aggregated outcome of batch notification dispatching."""

    total_messages: int
    sent_count: int
    failed_count: int
    suppressed_count: int
    results: list[NotificationDispatchResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert batch report to dictionary."""
        return {
            "total_messages": self.total_messages,
            "sent_count": self.sent_count,
            "failed_count": self.failed_count,
            "suppressed_count": self.suppressed_count,
            "results": [r.to_dict() for r in self.results],
        }
