"""Standard incident message rendering (channel-neutral).

The formatter produces plain ``(title, body)`` text for an incident transition.
Markup (Telegram HTML vs Slack mrkdwn) and escaping are applied centrally by
:class:`src.notifications.dispatcher.NotificationDispatcher`, so a producer never
builds a string and the two channels cannot drift.

Telegram is sent with ``parse_mode="HTML"``; the dispatcher escapes the plain
text, so user-supplied ``<`` or ``&`` can no longer corrupt the message.
"""

from __future__ import annotations

from src.notifications.alert_dto import AlertDecision, AlertEvent, IncidentTransition

_SEVERITY_ICON: dict[str, str] = {
    "INFO": "\u2139\ufe0f",
    "WARNING": "\U0001f7e0",
    "ERROR": "\U0001f534",
    "CRITICAL": "\U0001f6a8",
}

_DECISION_CAPTION: dict[AlertDecision, str] = {
    AlertDecision.REPEAT: "STILL FAILING",
    AlertDecision.ESCALATED: "ESCALATED",
    AlertDecision.RECOVERED: "RESOLVED",
    AlertDecision.ACKNOWLEDGED: "ACKNOWLEDGED",
}


def _icon(transition: IncidentTransition) -> str:
    if transition.decision == AlertDecision.RECOVERED:
        return "\U0001f7e2"
    return _SEVERITY_ICON.get(transition.severity.value, "\u26a0\ufe0f")


def format_duration(seconds: float) -> str:
    """Render a duration in a compact human-readable form."""
    total = max(0, int(seconds))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def format_incident_title(event: AlertEvent, transition: IncidentTransition) -> str:
    """Render the one-line incident title."""
    return f"{_icon(transition)} {event.title}"


def format_incident_body(event: AlertEvent, transition: IncidentTransition) -> str:
    """Render the incident body as plain text."""
    lines: list[str] = []

    caption = _DECISION_CAPTION.get(transition.decision)
    if caption:
        lines.append(f"{caption} \u00b7 {transition.severity.value}")

    lines.append(event.message)
    lines.append("")
    lines.append(f"Source: {event.source.value}/{event.component}")
    lines.append(f"Severity: {transition.severity.value}")
    lines.append(f"Opened: {transition.first_opened_at.isoformat(sep=' ')}")
    lines.append(f"Occurrences: {transition.occurrence_count}")
    if transition.decision == AlertDecision.RECOVERED:
        lines.append(f"Duration: {format_duration(transition.duration_seconds)}")
    else:
        lines.append(f"Duration: {format_duration(transition.duration_seconds)} (ongoing)")
    lines.append(f"Incident: {event.incident_key}")

    if event.remediation:
        lines.append("")
        lines.append("Remediation")
        lines.extend(f"- {cmd}" for cmd in event.remediation)

    return "\n".join(lines)
