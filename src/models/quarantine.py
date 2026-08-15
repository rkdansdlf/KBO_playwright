"""Quarantined records model for storing and tracking invalid or corrupted data."""

from __future__ import annotations

from sqlalchemy import JSON, Column, DateTime, Integer, String, Text

from src.models.base import Base, TimestampMixin


class QuarantinedRecord(Base, TimestampMixin):
    """Storage for raw payloads that failed validation checks."""

    __tablename__ = "quarantined_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), nullable=True, index=True)
    entity_type = Column(String(32), nullable=False, index=True)  # 'batting', 'pitching', 'game', 'pbp'
    entity_id = Column(String(64), nullable=True)
    rule_id = Column(String(32), nullable=False, index=True)
    severity = Column(String(16), nullable=False)  # 'ERROR', 'WARNING'
    failure_reason = Column(Text, nullable=False)
    raw_payload = Column(JSON, nullable=False)
    source = Column(String(64), nullable=False, default="kbo_official")
    status = Column(String(16), nullable=False, default="PENDING", index=True)  # 'PENDING', 'RECONCILED', 'DISCARDED'
    retry_count = Column(Integer, nullable=False, default=0)
    resolved_at = Column(DateTime, nullable=True)


__all__ = ["QuarantinedRecord"]
