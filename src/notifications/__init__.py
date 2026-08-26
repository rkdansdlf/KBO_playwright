"""Unified Multi-Channel Notification and Alerting package."""

from __future__ import annotations

from src.notifications.dispatcher import NotificationDispatcher
from src.notifications.dto import (
    NotificationBatchReport,
    NotificationChannel,
    NotificationDispatchResult,
    NotificationMessage,
    NotificationPriority,
)

__all__ = [
    "NotificationBatchReport",
    "NotificationChannel",
    "NotificationDispatchResult",
    "NotificationDispatcher",
    "NotificationMessage",
    "NotificationPriority",
]
