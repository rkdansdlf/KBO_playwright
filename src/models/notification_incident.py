"""Notification incident ledger for the in-process alert manager.

One row tracks the lifecycle of a single operational incident identified by a
*semantic* key (for example ``data_integrity:game_stats:20260925``) rather than
a hash of the rendered message. Message wording can change between runs without
splitting the same underlying incident in two.

The ledger is the durable backing for the OPEN -> ACKNOWLEDGED -> RECOVERED
lifecycle so a scheduler restart does not re-announce an incident that is still
open, and so recovery notices can be sent exactly once.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, DateTime, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin

INCIDENT_STATE_OPEN = "OPEN"
INCIDENT_STATE_ACKNOWLEDGED = "ACKNOWLEDGED"
INCIDENT_STATE_RECOVERED = "RECOVERED"

ACTIVE_INCIDENT_STATES = frozenset({INCIDENT_STATE_OPEN, INCIDENT_STATE_ACKNOWLEDGED})
INCIDENT_STATES = frozenset(
    {
        INCIDENT_STATE_OPEN,
        INCIDENT_STATE_ACKNOWLEDGED,
        INCIDENT_STATE_RECOVERED,
    },
)


class NotificationIncident(Base, TimestampMixin):
    """Durable state for one operational incident."""

    __tablename__ = "notification_incidents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    incident_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    component: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default=INCIDENT_STATE_OPEN)
    title: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    details_hash: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    occurrence_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    notification_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    first_opened_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_notified_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    metadata_json: Mapped[dict | None] = mapped_column("metadata", JSON, nullable=True)

    __table_args__ = (
        Index("idx_notification_incidents_state_severity", "state", "severity"),
        Index("idx_notification_incidents_source_last_seen", "source", "last_seen_at"),
    )
