"""Service for quarantining invalid records and managing quarantined data lifecycle."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from src.models.quarantine import QuarantinedRecord

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

    from src.validators.stat_validator import ValidationResult


class QuarantineService:
    """Manage quarantine storage, inspection, and recovery status."""

    def __init__(self, session: Session) -> None:
        """Initialize with an active database session."""
        self.session = session

    def quarantine_validation_failures(
        self,
        results: Sequence[ValidationResult],
        raw_payload: dict[str, Any],
        *,
        game_id: str | None = None,
        source: str = "kbo_official",
    ) -> list[QuarantinedRecord]:
        """Save blocking validation failures into the quarantine table.

        Returns list of newly created QuarantinedRecord entities.
        """
        saved_records: list[QuarantinedRecord] = []
        for res in results:
            if not res.is_blocking:
                continue

            qr = QuarantinedRecord(
                game_id=game_id or res.game_id,
                entity_type=res.entity_type,
                entity_id=str(res.entity_id) if res.entity_id is not None else None,
                rule_id=res.rule_id,
                severity=res.severity.value,
                failure_reason=res.message or f"Failed rule {res.rule_id} on field {res.field_name}",
                raw_payload=raw_payload,
                source=source or res.source,
                status="PENDING",
                retry_count=0,
            )
            self.session.add(qr)
            saved_records.append(qr)

        if saved_records:
            self.session.flush()
        return saved_records

    def get_pending_quarantines(
        self,
        *,
        game_id: str | None = None,
        entity_type: str | None = None,
        limit: int = 100,
    ) -> list[QuarantinedRecord]:
        """Retrieve unresolved quarantined records."""
        stmt = select(QuarantinedRecord).where(QuarantinedRecord.status == "PENDING")
        if game_id:
            stmt = stmt.where(QuarantinedRecord.game_id == game_id)
        if entity_type:
            stmt = stmt.where(QuarantinedRecord.entity_type == entity_type)
        stmt = stmt.order_by(QuarantinedRecord.id.desc()).limit(limit)
        return list(self.session.execute(stmt).scalars().all())

    def mark_resolved(
        self,
        quarantine_id: int,
        *,
        status: str = "RECONCILED",
    ) -> bool:
        """Mark a quarantined record as resolved (RECONCILED or DISCARDED)."""
        qr = self.session.get(QuarantinedRecord, quarantine_id)
        if qr:
            qr.status = status
            qr.resolved_at = datetime.now(UTC).replace(tzinfo=None)
            self.session.flush()
            return True
        return False


__all__ = ["QuarantineService"]
