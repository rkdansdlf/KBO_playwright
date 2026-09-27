"""Routing, cooldown and suppression policy for the in-process alert manager.

Policy is deliberately separated from both the incident lifecycle (which owns
*state*) and the dispatcher (which owns *transport*). This module answers only:
which destination, at what severity floor, and how often may this incident
re-notify.

The legacy per-gap Telegram routing table (formerly
``src.utils.alerting.GAP_CATEGORY_ENV_MAP``) lives here so that every alert
source — not just the gap report — can route to a dedicated chat.
"""

from __future__ import annotations

import os

from src.notifications.alert_dto import AlertSeverity, severity_rank
from src.notifications.dto import NotificationPriority

#: Map of alert token (source value or legacy gap category, lowercased) to the
#: environment variable holding its Telegram chat ID.
DESTINATION_ENV_MAP: dict[str, str] = {
    # Alert sources
    "database": "TELEGRAM_CHAT_ID_DATABASE",
    "crawler": "TELEGRAM_CHAT_ID_CRAWLER",
    "integrity": "TELEGRAM_CHAT_ID_INTEGRITY",
    "quality": "TELEGRAM_CHAT_ID_QUALITY",
    "freshness": "TELEGRAM_CHAT_ID_FRESHNESS",
    "drift": "TELEGRAM_CHAT_ID_DRIFT",
    "rag": "TELEGRAM_CHAT_ID_RAG",
    "recovery": "TELEGRAM_CHAT_ID_RECOVERY",
    "scheduler": "TELEGRAM_CHAT_ID_SCHEDULER",
    "pipeline": "TELEGRAM_CHAT_ID_PIPELINE",
    "report": "TELEGRAM_CHAT_ID_REPORT",
    "system": "TELEGRAM_CHAT_ID_SYSTEM",
    # Legacy gap categories (retained while callers migrate off send_gap_alert)
    "p0": "TELEGRAM_CHAT_ID_P0",
    "staleness": "TELEGRAM_CHAT_ID_STALENESS",
    "relay": "TELEGRAM_CHAT_ID_RELAY",
    "profile": "TELEGRAM_CHAT_ID_PROFILE",
    "id_resolution": "TELEGRAM_CHAT_ID_ID_RESOLUTION",
    "pa_formula": "TELEGRAM_CHAT_ID_PA_FORMULA",
    "team_stats": "TELEGRAM_CHAT_ID_TEAM_STATS",
    "standings": "TELEGRAM_CHAT_ID_STANDINGS",
}

#: Emoji prefix per legacy gap category (retained for message compatibility).
GAP_EMOJI_MAP: dict[str, str] = {
    "FRESHNESS": "\u2757",
    "P0": "\u26a1",
    "STALENESS": "\u23f3",
    "RELAY": "\U0001f4be",
    "PROFILE": "\U0001f464",
    "ID_RESOLUTION": "\U0001f50d",
    "PA_FORMULA": "\U0001f4ca",
    "TEAM_STATS": "\U0001f3c1",
    "STANDINGS": "\U0001f3c5",
}

_HOUR = 3600

#: Re-notification cooldown per severity, in seconds.
DEFAULT_COOLDOWN_SECONDS: dict[AlertSeverity, int] = {
    AlertSeverity.INFO: 6 * _HOUR,
    AlertSeverity.WARNING: 30 * 60,
    AlertSeverity.ERROR: 10 * 60,
    AlertSeverity.CRITICAL: 5 * 60,
}

_COOLDOWN_ENV_VARS: dict[AlertSeverity, str] = {
    AlertSeverity.INFO: "ALERT_COOLDOWN_INFO_SECONDS",
    AlertSeverity.WARNING: "ALERT_COOLDOWN_WARNING_SECONDS",
    AlertSeverity.ERROR: "ALERT_COOLDOWN_ERROR_SECONDS",
    AlertSeverity.CRITICAL: "ALERT_COOLDOWN_CRITICAL_SECONDS",
}

_SEVERITY_BY_NAME: dict[str, AlertSeverity] = {
    "info": AlertSeverity.INFO,
    "warning": AlertSeverity.WARNING,
    "error": AlertSeverity.ERROR,
    "critical": AlertSeverity.CRITICAL,
}

#: Severity below which incidents are recorded but never delivered.
DEFAULT_MIN_NOTIFY_SEVERITY = AlertSeverity.WARNING

_SEVERITY_TO_PRIORITY: dict[AlertSeverity, NotificationPriority] = {
    AlertSeverity.INFO: NotificationPriority.LOW,
    AlertSeverity.WARNING: NotificationPriority.NORMAL,
    AlertSeverity.ERROR: NotificationPriority.HIGH,
    AlertSeverity.CRITICAL: NotificationPriority.CRITICAL,
}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def cooldown_seconds(severity: AlertSeverity) -> int:
    """Return the re-notification cooldown for a severity, honoring env overrides."""
    return _env_int(_COOLDOWN_ENV_VARS[severity], DEFAULT_COOLDOWN_SECONDS[severity])


def severity_to_priority(severity: AlertSeverity) -> NotificationPriority:
    """Map a canonical severity onto a transport priority."""
    return _SEVERITY_TO_PRIORITY[severity]


def min_notify_severity() -> AlertSeverity:
    """Return the delivery floor; incidents below it are recorded but not sent."""
    raw = (os.getenv("ALERT_MIN_SEVERITY") or "").strip().lower()
    return _SEVERITY_BY_NAME.get(raw, DEFAULT_MIN_NOTIFY_SEVERITY)


def should_notify_severity(severity: AlertSeverity) -> bool:
    """Return whether a severity is at or above the configured delivery floor."""
    return severity_rank(severity) >= severity_rank(min_notify_severity())


def resolve_chat_id(token: str | None, *, explicit: str | None = None) -> str | None:
    """Resolve the Telegram chat ID for an alert token.

    Resolution order: explicit override, token-specific env var, then the
    default ``TELEGRAM_CHAT_ID``.
    """
    if explicit:
        return explicit
    if token:
        env_name = DESTINATION_ENV_MAP.get(token.lower())
        if env_name:
            value = os.getenv(env_name)
            if value:
                return value
    return os.getenv("TELEGRAM_CHAT_ID")
