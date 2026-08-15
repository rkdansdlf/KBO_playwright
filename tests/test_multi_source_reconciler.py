"""Unit tests for MultiSourceReconciler and CorrectionAuditTrail."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.audit_trail import CorrectionAuditTrail
from src.models.base import Base
from src.models.quarantine import QuarantinedRecord
from src.services.multi_source_reconciler import CorrectionAuditRequest, MultiSourceReconciler


def _get_in_memory_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def test_reconcile_field_agreement() -> None:
    """When KBO and Naver match, confidence is 1.0."""
    reconciler = MultiSourceReconciler()
    dec = reconciler.reconcile_field("hits", 3, 3)
    assert dec.status == "RESOLVED"
    assert dec.confidence == 1.0
    assert dec.chosen_value == 3


def test_reconcile_field_conflict_resolved_by_pbp() -> None:
    """When KBO and Naver conflict but PBP matches Naver, Naver is chosen with 0.90 confidence."""
    reconciler = MultiSourceReconciler()
    dec = reconciler.reconcile_field("hits", 2, 3, pbp_val=3)
    assert dec.status == "RESOLVED"
    assert dec.chosen_value == 3
    assert dec.confidence == 0.9


def test_reconcile_field_unresolvable_conflict() -> None:
    """When KBO and Naver conflict and no PBP tiebreaker exists, report CONFLICT."""
    reconciler = MultiSourceReconciler()
    dec = reconciler.reconcile_field("hits", 2, 4, pbp_val=None)
    assert dec.status == "CONFLICT"
    assert dec.chosen_value == 2
    assert dec.confidence == 0.5


def test_record_correction_audit() -> None:
    """Audit record should be saved with full lineage."""
    session = _get_in_memory_session()
    reconciler = MultiSourceReconciler()

    req = CorrectionAuditRequest(
        game_id="20260815LGKIA0",
        entity_type="batting",
        entity_id="52600",
        field_name="hits",
        raw_value=4,
        raw_source="kbo_official",
        corrected_value=2,
        corrected_source="naver_sports",
        reason="Fixed hits > at_bats violation via Naver cross-check",
        confidence=0.95,
    )
    entry = reconciler.record_correction_audit(session, req)
    assert entry.id is not None
    assert entry.raw_value == "4"
    assert entry.corrected_value == "2"

    saved = session.query(CorrectionAuditTrail).first()
    assert saved is not None
    assert saved.entity_type == "batting"


def test_reconcile_and_heal_quarantine() -> None:
    """Quarantined record should be marked RECONCILED after secondary payload healing."""
    session = _get_in_memory_session()
    reconciler = MultiSourceReconciler()

    qr = QuarantinedRecord(
        game_id="20260815LGKIA0",
        entity_type="batting",
        entity_id="52600",
        rule_id="BAT-001",
        severity="ERROR",
        failure_reason="Hits exceeds AB",
        raw_payload={"at_bats": 2, "hits": 4},
        source="kbo_official",
        status="PENDING",
    )
    session.add(qr)
    session.flush()

    secondary_payload = {"at_bats": 2, "hits": 1}
    success = reconciler.reconcile_and_heal_quarantine(session, qr.id, secondary_payload)
    assert success is True

    updated_qr = session.get(QuarantinedRecord, qr.id)
    assert updated_qr.status == "RECONCILED"

    audit = session.query(CorrectionAuditTrail).first()
    assert audit is not None
    assert audit.corrected_value == "1"
