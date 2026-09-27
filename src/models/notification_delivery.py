"""Append-only delivery audit ledger for the notification subsystem.

Each row records one external delivery attempt for one channel — the actual side
effect that left the process. It is intentionally decoupled from
``notification_incidents``:

* ``incident_id`` is a **soft reference** (no foreign key) so incident retention
  and delivery retention are independent and pruning incidents never destroys
  delivery history;
* delivery rows outlive the business transaction that triggered them, because
  they are written in their own short transaction *after* the transport attempt
  has already happened.

Only dispatch-pipeline outcomes are recorded (``SENT`` / ``FAILED`` /
``SKIPPED_UNCONFIGURED`` / ``DRY_RUN``). ``SUPPRESSED`` is an incident-policy
decision that never reached the pipeline and is tracked on the incident instead.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin

#: Delivery outcomes that represent an actual entry into the dispatch pipeline.
DELIVERY_STATUS_SENT = "SENT"
DELIVERY_STATUS_FAILED = "FAILED"
DELIVERY_STATUS_SKIPPED = "SKIPPED_UNCONFIGURED"
DELIVERY_STATUS_DRY_RUN = "DRY_RUN"
DELIVERY_STATUSES = frozenset(
    {
        DELIVERY_STATUS_SENT,
        DELIVERY_STATUS_FAILED,
        DELIVERY_STATUS_SKIPPED,
        DELIVERY_STATUS_DRY_RUN,
    },
)

#: Status that is an incident-policy decision, never a delivery, so it is never persisted here.
DELIVERY_STATUS_SUPPRESSED = "SUPPRESSED"


class NotificationDelivery(Base, TimestampMixin):
    """One external delivery attempt for one channel."""

    __tablename__ = "notification_deliveries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    incident_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    notification_type: Mapped[str] = mapped_column(String(32), nullable=False, default="notification")
    batch_id: Mapped[str] = mapped_column(String(36), nullable=False, default="")
    channel: Mapped[str] = mapped_column(String(32), nullable=False)
    destination: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    dispatched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("idx_notification_deliveries_incident", "incident_id", "dispatched_at"),
        Index("idx_notification_deliveries_channel_status", "channel", "status", "dispatched_at"),
        Index("idx_notification_deliveries_batch", "batch_id"),
    )
