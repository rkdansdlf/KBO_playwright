"""Unified Multi-Channel Notification and Alerting package."""

from __future__ import annotations

from src.notifications.alert_dto import (
    AlertDecision,
    AlertEvent,
    AlertSeverity,
    AlertSource,
    AlertStatus,
    IncidentTransition,
    severity_at_least,
    severity_rank,
    utcnow,
)
from src.notifications.dispatcher import NotificationDispatcher
from src.notifications.dto import (
    NotificationBatchReport,
    NotificationChannel,
    NotificationDispatchResult,
    NotificationMessage,
    NotificationPriority,
)
from src.notifications.incident import IncidentManager
from src.notifications.publisher import AlertPublisher, PublishResult, event_from_incident
from src.notifications.recorder import DeliveryRecorder
from src.notifications.standalone import send_notification

__all__ = [
    "AlertDecision",
    "AlertEvent",
    "AlertPublisher",
    "AlertSeverity",
    "AlertSource",
    "AlertStatus",
    "DeliveryRecorder",
    "IncidentManager",
    "IncidentTransition",
    "NotificationBatchReport",
    "NotificationChannel",
    "NotificationDispatchResult",
    "NotificationDispatcher",
    "NotificationMessage",
    "NotificationPriority",
    "PublishResult",
    "event_from_incident",
    "send_notification",
    "severity_at_least",
    "severity_rank",
    "utcnow",
]
