"""Audit trail model for tracking data corrections, multi-source reconciliations, and lineage."""

from __future__ import annotations

from sqlalchemy import JSON, Column, Float, Integer, String, Text

from src.models.base import Base, TimestampMixin


class CorrectionAuditTrail(Base, TimestampMixin):
    """Audit log for automated corrections and data reconciliations."""

    __tablename__ = "correction_audit_trail"

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), nullable=True, index=True)
    entity_type = Column(String(32), nullable=False, index=True)  # 'game', 'batting', 'pitching', 'pbp'
    entity_id = Column(String(64), nullable=True)
    field_name = Column(String(64), nullable=False)
    raw_value = Column(Text, nullable=True)
    raw_source = Column(String(64), nullable=False)
    corrected_value = Column(Text, nullable=True)
    corrected_source = Column(String(64), nullable=False)
    correction_reason = Column(Text, nullable=False)
    confidence = Column(Float, nullable=False, default=1.0)
    extra_metadata = Column(JSON, nullable=True)


__all__ = ["CorrectionAuditTrail"]
