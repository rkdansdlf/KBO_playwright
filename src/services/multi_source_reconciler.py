"""Multi-Source Reconciliation Engine for KBO Data Pipeline.

Cross-checks statistical discrepancies across KBO Official, Naver Sports, and PBP,
applying confidence-rated auto-healing while preserving 100% lineage in audit trails.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from src.models.audit_trail import CorrectionAuditTrail
from src.models.quarantine import QuarantinedRecord

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@dataclass(frozen=True, slots=True)
class ReconciliationDecision:
    """Outcome of cross-checking multiple data sources."""

    field_name: str
    chosen_value: object
    confidence: float  # 0.0 to 1.0
    status: str  # 'RESOLVED', 'CONFLICT', 'NO_SECONDARY_DATA'
    source_used: str
    reason: str


@dataclass(frozen=True, slots=True)
class CorrectionAuditRequest:
    """Payload for recording a data correction event."""

    game_id: str | None
    entity_type: str
    entity_id: str | None
    field_name: str
    raw_value: object
    raw_source: str
    corrected_value: object
    corrected_source: str
    reason: str
    confidence: float = 1.0
    extra_metadata: dict[str, Any] | None = None


class MultiSourceReconciler:
    """Engine that reconciles primary KBO data with secondary sources (Naver/PBP)."""

    def __init__(self, session: Session | None = None) -> None:
        """Initialize MultiSourceReconciler with optional database session."""
        self.session = session

    def record_audit(
        self,
        req: CorrectionAuditRequest,
        session: Session | None = None,
    ) -> CorrectionAuditTrail:
        """Provide a convenience wrapper for record_correction_audit."""
        sess = session or self.session
        if sess is None:
            msg = "A database session is required to record correction audit trail."
            raise ValueError(msg)
        return self.record_correction_audit(sess, req)

    def reconcile_field(
        self,
        field_name: str,
        primary_val: object,
        secondary_val: object,
        pbp_val: object | None = None,
    ) -> ReconciliationDecision:
        """Cross-check values from primary (KBO), secondary (Naver), and tertiary (PBP) sources."""
        # 1. Primary and secondary match
        if primary_val is not None and primary_val == secondary_val:
            return ReconciliationDecision(
                field_name=field_name,
                chosen_value=primary_val,
                confidence=1.0,
                status="RESOLVED",
                source_used="kbo_official+naver",
                reason="Primary and secondary sources in complete agreement",
            )

        # 2. Both have values but conflict
        if primary_val is not None and secondary_val is not None:
            if pbp_val == secondary_val:
                return ReconciliationDecision(
                    field_name=field_name,
                    chosen_value=secondary_val,
                    confidence=0.9,
                    status="RESOLVED",
                    source_used="naver+pbp",
                    reason="Secondary verified by PBP against primary discrepancy",
                )
            if pbp_val == primary_val:
                return ReconciliationDecision(
                    field_name=field_name,
                    chosen_value=primary_val,
                    confidence=0.9,
                    status="RESOLVED",
                    source_used="kbo_official+pbp",
                    reason="Primary verified by PBP against secondary discrepancy",
                )
            return ReconciliationDecision(
                field_name=field_name,
                chosen_value=primary_val,
                confidence=0.5,
                status="CONFLICT",
                source_used="kbo_official",
                reason=f"Conflict between sources: KBO({primary_val}) vs Naver({secondary_val})",
            )

        # 3. Missing primary but secondary available
        if primary_val is None and secondary_val is not None:
            return ReconciliationDecision(
                field_name=field_name,
                chosen_value=secondary_val,
                confidence=0.8,
                status="RESOLVED",
                source_used="naver",
                reason="Primary value absent; filled from secondary source",
            )

        # 4. Fallback
        return ReconciliationDecision(
            field_name=field_name,
            chosen_value=primary_val,
            confidence=0.6,
            status="NO_SECONDARY_DATA",
            source_used="kbo_official",
            reason="No secondary data available to cross-check",
        )

    def record_correction_audit(
        self,
        session: Session,
        req: CorrectionAuditRequest,
    ) -> CorrectionAuditTrail:
        """Persist a single correction into the correction_audit_trail table."""
        audit_entry = CorrectionAuditTrail(
            game_id=req.game_id,
            entity_type=req.entity_type,
            entity_id=req.entity_id,
            field_name=req.field_name,
            raw_value=str(req.raw_value) if req.raw_value is not None else None,
            raw_source=req.raw_source,
            corrected_value=str(req.corrected_value) if req.corrected_value is not None else None,
            corrected_source=req.corrected_source,
            correction_reason=req.reason,
            confidence=req.confidence,
            extra_metadata=req.extra_metadata,
        )
        session.add(audit_entry)
        session.flush()
        return audit_entry

    def reconcile_and_heal_quarantine(
        self,
        session: Session,
        quarantine_id: int,
        secondary_payload: dict[str, Any],
    ) -> bool:
        """Attempt to auto-heal a quarantined record using secondary source payload."""
        qr = session.get(QuarantinedRecord, quarantine_id)
        if not qr or qr.status != "PENDING":
            return False

        raw = qr.raw_payload if isinstance(qr.raw_payload, dict) else json.loads(qr.raw_payload)
        rule_id = qr.rule_id

        # Auto-healing for Batting hits > AB discrepancy
        if rule_id == "BAT-001" and "hits" in raw and "hits" in secondary_payload:
            sec_hits = secondary_payload.get("hits")
            sec_ab = secondary_payload.get("at_bats", raw.get("at_bats"))

            if sec_hits is not None and sec_ab is not None and int(sec_hits) <= int(sec_ab):
                self.record_correction_audit(
                    session,
                    CorrectionAuditRequest(
                        game_id=qr.game_id,
                        entity_type=qr.entity_type,
                        entity_id=qr.entity_id,
                        field_name="hits",
                        raw_value=raw.get("hits"),
                        raw_source=qr.source,
                        corrected_value=sec_hits,
                        corrected_source="naver_sports",
                        reason=f"Reconciled {rule_id} via secondary payload",
                        confidence=0.95,
                    ),
                )
                qr.status = "RECONCILED"
                session.flush()
                return True

        return False


__all__ = ["CorrectionAuditRequest", "MultiSourceReconciler", "ReconciliationDecision"]
