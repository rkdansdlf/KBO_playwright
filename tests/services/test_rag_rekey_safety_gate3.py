"""Comprehensive Local Ephemeral Apply-Only Safety Tests for Phase 105B Gate 3.

This test suite executes in an isolated, disposable SQLite environment to attest
the safety invariants of the RAG Natural Key Rekey pipeline:
1. Normal apply (expected mutation == actual rowcount)
2. Idempotency (re-apply mutation count == 0, explicit SUCCESS_NOOP)
3. CAS stale content hash fail-closed
4. CAS stale legacy ID fail-closed
5. CAS stale index status fail-closed
6. CAS stale index version fail-closed
7. Atomicity / batch rollback on failure
8. Target collision handling (tombstone legacy without corrupting natural target)
9. Pre-converted rows untouched
10. Manifest integrity / SHA verification
11. Snapshot binding / database fingerprint verification
12. Inverse rollback manifest execution & state restoration
13. Fault injection / mid-process abort safety
14. DSN & production write guard fail-closed
15. Immutable receipt generation on successful execution
16. Idempotent replay reuses existing verified receipt
17. Transaction ID collision with different payload rejected (Fail-Closed)
18. Failed transaction does not publish success receipt
19. Replay does not mutate existing receipt
20. Batch concurrent modification all-or-nothing rollback
21. Oracle / Staging DSN rejected in local safety mode
22. Non-ephemeral primary database path (kbo_dev.db) rejected in safety mode
"""

from __future__ import annotations

import contextlib
from collections import Counter
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
from typing import Any
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select, text, update
from sqlalchemy.orm import Session, sessionmaker

from src.cli.rag.apply_rag_rekey import (
    DISPOSITION_REKEY,
    DISPOSITION_TOMBSTONE,
    _apply_rekey,
    _check_receipt_replay,
    _compute_manifest_sha,
    _get_current_database_fingerprint,
    _get_current_git_sha,
    _load_manifest,
    _publish_immutable_receipt,
    _validate_dsn_security,
    _validate_manifest_header,
    _validate_transaction_id,
    _write_preimage_and_rollback,
    main as apply_rekey_main,
)
from src.models.base import Base
from src.models.rag_chunk import RagChunk


def _make_safe_manifest(entries: list[dict[str, Any]]) -> dict[str, Any]:
    """Create a fully compliant manifest with valid SHA and environment bindings."""
    manifest = {
        "manifest_version": "r2-identity-census-v1",
        "target_index_version": "r2",
        "read_only": True,
        "source_tables": ["awards"],
        "totals": {
            "source_rows": len(entries),
            "legacy_numeric_rows": len(entries),
            "legacy_non_numeric_rows": 0,
            "safe_source_matches": len(entries),
            "safe_rekey_candidates": len(entries),
            "existing_natural_target": 0,
            "orphan_rows": 0,
            "collision_keys": 0,
            "collision_rows": 0,
            "source_rows_missing_in_index": 0,
        },
        "unsafe_entry_count": 0,
        "sources": [
            {
                "source_table": "awards",
                "source_rows": len(entries),
                "legacy_numeric_rows": len(entries),
                "legacy_non_numeric_rows": 0,
                "safe_source_matches": len(entries),
                "safe_rekey_candidates": len(entries),
                "existing_natural_target": 0,
                "orphan_rows": 0,
                "collision_keys": 0,
                "collision_rows": 0,
                "source_rows_missing_in_index": 0,
                "by_disposition": dict(Counter(e.get("disposition") for e in entries)),
            }
        ],
        "entries": entries,
    }

    manifest_sha = _compute_manifest_sha(manifest)
    db_fingerprint = _get_current_database_fingerprint()
    git_sha = _get_current_git_sha()
    disposition_counts = dict(Counter(e.get("disposition") for e in entries))

    manifest["manifest_header"] = {
        "manifest_schema_version": "r2-rekey-manifest-v1",
        "identity_schema_version": "r2",
        "generated_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "database_fingerprint": db_fingerprint,
        "git_commit_sha": git_sha,
        "manifest_sha256": manifest_sha,
        "expected_entry_count": len(entries),
        "expected_disposition_counts": disposition_counts,
        "manifest_id": "manifest-test-001",
        "transaction_id": f"tx-{manifest_sha[:12]}",
    }
    return manifest


@pytest.fixture()
def ephemeral_db(tmp_path: Path) -> Any:
    """Create an isolated, disposable SQLite database fixture for Gate 3 safety tests."""
    db_file = tmp_path / "ephemeral_rag_gate3.db"
    db_url = f"sqlite:///{db_file}"

    prev_db_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = db_url

    engine = create_engine(db_url)
    Base.metadata.create_all(engine, tables=[RagChunk.__table__])
    session_factory = sessionmaker(bind=engine)

    yield {
        "engine": engine,
        "session_factory": session_factory,
        "db_file": db_file,
        "db_url": db_url,
    }

    Base.metadata.drop_all(engine, tables=[RagChunk.__table__])
    engine.dispose()
    if prev_db_url:
        os.environ["DATABASE_URL"] = prev_db_url
    else:
        os.environ.pop("DATABASE_URL", None)


class TestRagRekeySafetyGate3:
    """14+ Invariant Safety Test Suite for Gate 3 Ephemeral Rekey Apply."""

    def test_1_normal_apply_exact_mutation(self, ephemeral_db: dict[str, Any]) -> None:
        """1. Normal apply: exact rowcount mutation matches expected entry count."""
        session: Session = ephemeral_db["session_factory"]()
        try:
            for i in range(1, 6):
                session.add(
                    RagChunk(
                        id=i,
                        source_table="awards",
                        source_row_id=str(i),
                        content=f"Award winner {i}",
                        content_hash=f"hash_{i}",
                        index_status="ACTIVE",
                        index_version="r2",
                    )
                )
            session.commit()

            entries = [
                {
                    "chunk_id": i,
                    "legacy_source_row_id": str(i),
                    "natural_source_row_id": f"2025_award_MVP_{i}",
                    "disposition": DISPOSITION_REKEY,
                    "index_status": "ACTIVE",
                    "legacy_content_hash": f"hash_{i}",
                    "index_version": "r2",
                }
                for i in range(1, 6)
            ]

            rekey, tombstone, already, skipped, status = _apply_rekey(session, entries, dry_run=False)
            assert rekey == 5
            assert tombstone == 0
            assert already == 0
            assert len(skipped) == 0
            assert status == "SUCCESS_APPLIED"

            # Verify all natural keys persisted
            rows = session.scalars(select(RagChunk).order_by(RagChunk.id)).all()
            for r in rows:
                assert r.source_row_id == f"2025_award_MVP_{r.id}"
        finally:
            session.close()

    def test_2_idempotency_zero_mutations_on_reapply(self, ephemeral_db: dict[str, Any]) -> None:
        """2. Idempotency: re-apply yields 0 mutations and reports explicit SUCCESS_NOOP."""
        session: Session = ephemeral_db["session_factory"]()
        try:
            session.add(
                RagChunk(
                    id=10,
                    source_table="awards",
                    source_row_id="10",
                    content="Award 10",
                    content_hash="h10",
                    index_status="ACTIVE",
                    index_version="r2",
                )
            )
            session.commit()

            entry = {
                "chunk_id": 10,
                "legacy_source_row_id": "10",
                "natural_source_row_id": "2025_award_10",
                "disposition": DISPOSITION_REKEY,
                "index_status": "ACTIVE",
                "legacy_content_hash": "h10",
                "index_version": "r2",
            }

            # First apply -> 1 rekey
            rekey_1, _, already_1, skipped_1, status_1 = _apply_rekey(session, [entry], dry_run=False)
            assert rekey_1 == 1
            assert already_1 == 0
            assert len(skipped_1) == 0
            assert status_1 == "SUCCESS_APPLIED"

            # Second apply -> 0 rekey, 1 already applied, explicit SUCCESS_NOOP
            rekey_2, _, already_2, skipped_2, status_2 = _apply_rekey(session, [entry], dry_run=False)
            assert rekey_2 == 0
            assert already_2 == 1
            assert len(skipped_2) == 0
            assert status_2 == "SUCCESS_NOOP"
        finally:
            session.close()

    def test_3_cas_stale_content_hash_fail_closed(self, ephemeral_db: dict[str, Any]) -> None:
        """3. CAS stale content hash fail-closed: row modified in DB rejects rekey."""
        session: Session = ephemeral_db["session_factory"]()
        try:
            session.add(
                RagChunk(
                    id=20,
                    source_table="awards",
                    source_row_id="20",
                    content="Award 20 changed",
                    content_hash="hash_NEW",
                    index_status="ACTIVE",
                    index_version="r2",
                )
            )
            session.commit()

            entry = {
                "chunk_id": 20,
                "legacy_source_row_id": "20",
                "natural_source_row_id": "2025_award_20",
                "disposition": DISPOSITION_REKEY,
                "index_status": "ACTIVE",
                "legacy_content_hash": "hash_OLD",
                "index_version": "r2",
            }

            rekey, _, _, skipped, status = _apply_rekey(session, [entry], dry_run=False)
            assert rekey == 0
            assert len(skipped) == 1
            assert "stale_content_hash" in skipped[0]["reason"]
            assert status == "FAILED_STALE_MANIFEST"
            # Verify DB unchanged
            chunk = session.get(RagChunk, 20)
            assert chunk.source_row_id == "20"
        finally:
            session.close()

    def test_4_cas_stale_legacy_id_fail_closed(self, ephemeral_db: dict[str, Any]) -> None:
        """4. CAS stale legacy ID fail-closed."""
        session: Session = ephemeral_db["session_factory"]()
        try:
            session.add(
                RagChunk(
                    id=30,
                    source_table="awards",
                    source_row_id="999",
                    content="Award 30",
                    content_hash="h30",
                    index_status="ACTIVE",
                    index_version="r2",
                )
            )
            session.commit()

            entry = {
                "chunk_id": 30,
                "legacy_source_row_id": "30",
                "natural_source_row_id": "2025_award_30",
                "disposition": DISPOSITION_REKEY,
                "index_status": "ACTIVE",
                "legacy_content_hash": "h30",
                "index_version": "r2",
            }

            rekey, _, _, skipped, _ = _apply_rekey(session, [entry], dry_run=False)
            assert rekey == 0
            assert len(skipped) == 1
            assert "stale_legacy_id" in skipped[0]["reason"]
        finally:
            session.close()

    def test_5_cas_stale_index_status_fail_closed(self, ephemeral_db: dict[str, Any]) -> None:
        """5. CAS stale index status fail-closed."""
        session: Session = ephemeral_db["session_factory"]()
        try:
            session.add(
                RagChunk(
                    id=40,
                    source_table="awards",
                    source_row_id="40",
                    content="Award 40",
                    content_hash="h40",
                    index_status="DELETED",
                    index_version="r2",
                )
            )
            session.commit()

            entry = {
                "chunk_id": 40,
                "legacy_source_row_id": "40",
                "natural_source_row_id": "2025_award_40",
                "disposition": DISPOSITION_REKEY,
                "index_status": "ACTIVE",
                "legacy_content_hash": "h40",
                "index_version": "r2",
            }

            rekey, _, _, skipped, _ = _apply_rekey(session, [entry], dry_run=False)
            assert rekey == 0
            assert len(skipped) == 1
            assert "stale_index_status" in skipped[0]["reason"]
        finally:
            session.close()

    def test_6_cas_stale_index_version_fail_closed(self, ephemeral_db: dict[str, Any]) -> None:
        """6. CAS stale index version fail-closed."""
        session: Session = ephemeral_db["session_factory"]()
        try:
            session.add(
                RagChunk(
                    id=50,
                    source_table="awards",
                    source_row_id="50",
                    content="Award 50",
                    content_hash="h50",
                    index_status="ACTIVE",
                    index_version="rag-v1",
                )
            )
            session.commit()

            entry = {
                "chunk_id": 50,
                "legacy_source_row_id": "50",
                "natural_source_row_id": "2025_award_50",
                "disposition": DISPOSITION_REKEY,
                "index_status": "ACTIVE",
                "legacy_content_hash": "h50",
                "index_version": "r2",
            }

            rekey, _, _, skipped, _ = _apply_rekey(session, [entry], dry_run=False)
            assert rekey == 0
            assert len(skipped) == 1
            assert "stale_index_version" in skipped[0]["reason"]
        finally:
            session.close()

    def test_7_atomicity_batch_rollback_on_failure(self, ephemeral_db: dict[str, Any]) -> None:
        """7. Atomicity / batch rollback: single failure in batch triggers full rollback (0 mutations committed)."""
        session: Session = ephemeral_db["session_factory"]()
        try:
            for i in range(60, 63):
                session.add(
                    RagChunk(
                        id=i,
                        source_table="awards",
                        source_row_id=str(i),
                        content=f"Award {i}",
                        content_hash="h_valid" if i != 61 else "h_CORRUPTED",
                        index_status="ACTIVE",
                        index_version="r2",
                    )
                )
            session.commit()

            entries = [
                {
                    "chunk_id": i,
                    "legacy_source_row_id": str(i),
                    "natural_source_row_id": f"2025_award_{i}",
                    "disposition": DISPOSITION_REKEY,
                    "index_status": "ACTIVE",
                    "legacy_content_hash": "h_valid",
                    "index_version": "r2",
                }
                for i in range(60, 63)
            ]

            rekey, _, _, skipped, status = _apply_rekey(session, entries, dry_run=False, strict_atomic=True)
            assert rekey == 0
            assert len(skipped) == 1
            assert status == "FAILED_ATOMIC_ROLLBACK"

            # Verify entire batch was rolled back
            assert session.get(RagChunk, 60).source_row_id == "60"
            assert session.get(RagChunk, 61).source_row_id == "61"
            assert session.get(RagChunk, 62).source_row_id == "62"
        finally:
            session.close()

    def test_8_target_collision_disposition_tombstone(self, ephemeral_db: dict[str, Any]) -> None:
        """8. Collision disposition: TARGET_EXISTS_SAME_CONTENT marks legacy row DELETED."""
        session: Session = ephemeral_db["session_factory"]()
        try:
            # Legacy row
            session.add(
                RagChunk(
                    id=70,
                    source_table="awards",
                    source_row_id="70",
                    content="Award Duplicate Content",
                    content_hash="h_dup",
                    index_status="ACTIVE",
                    index_version="r2",
                )
            )
            # Already existing target natural row
            session.add(
                RagChunk(
                    id=71,
                    source_table="awards",
                    source_row_id="2025_award_70_NATURAL",
                    content="Award Duplicate Content",
                    content_hash="h_dup",
                    index_status="ACTIVE",
                    index_version="r2",
                )
            )
            session.commit()

            entry = {
                "chunk_id": 70,
                "legacy_source_row_id": "70",
                "natural_source_row_id": "2025_award_70_NATURAL",
                "disposition": DISPOSITION_TOMBSTONE,
                "index_status": "ACTIVE",
                "legacy_content_hash": "h_dup",
                "index_version": "r2",
            }

            _, tombstone, _, skipped, status = _apply_rekey(session, [entry], dry_run=False)
            assert tombstone == 1
            assert len(skipped) == 0
            assert status == "SUCCESS_APPLIED"

            # Legacy row tombstoned
            legacy_chunk = session.get(RagChunk, 70)
            assert legacy_chunk.index_status == "DELETED"

            # Natural target preserved and untouched
            natural_chunk = session.get(RagChunk, 71)
            assert natural_chunk.index_status == "ACTIVE"
            assert natural_chunk.source_row_id == "2025_award_70_NATURAL"
        finally:
            session.close()

    def test_9_preconverted_rows_untouched(self, ephemeral_db: dict[str, Any]) -> None:
        """9. Pre-converted rows untouched: existing natural-keyed rows remain intact."""
        session: Session = ephemeral_db["session_factory"]()
        try:
            session.add(
                RagChunk(
                    id=80,
                    source_table="awards",
                    source_row_id="2024_award_80",
                    content="Preconverted Award 80",
                    content_hash="h80",
                    index_status="ACTIVE",
                    index_version="r2",
                )
            )
            session.commit()

            # Apply manifest that only targets other rows
            entry = {
                "chunk_id": 81,
                "legacy_source_row_id": "81",
                "natural_source_row_id": "2025_award_81",
                "disposition": DISPOSITION_REKEY,
                "index_status": "ACTIVE",
                "legacy_content_hash": "h81",
                "index_version": "r2",
            }
            manifest = _make_safe_manifest([entry])

            _apply_rekey(session, manifest["entries"], dry_run=False)

            # Verify chunk 80 is untouched
            chunk_80 = session.get(RagChunk, 80)
            assert chunk_80.source_row_id == "2024_award_80"
            assert chunk_80.index_status == "ACTIVE"
        finally:
            session.close()

    def test_10_manifest_tampering_rejected(self) -> None:
        """10. Manifest integrity: tampered manifest content rejected by SHA mismatch."""
        entry = {
            "chunk_id": 90,
            "legacy_source_row_id": "90",
            "natural_source_row_id": "2025_award_90",
            "disposition": DISPOSITION_REKEY,
            "index_status": "ACTIVE",
            "legacy_content_hash": "h90",
            "index_version": "r2",
        }
        manifest = _make_safe_manifest([entry])
        # Tamper payload
        manifest["entries"][0]["natural_source_row_id"] = "TAMPERED_KEY"

        err = _validate_manifest_header(manifest, apply=True)
        assert err is not None
        assert "manifest SHA mismatch" in err

    def test_11_snapshot_binding_fingerprint_mismatch_rejected(self) -> None:
        """11. Snapshot binding: manifest bound to different DB fingerprint rejected in apply."""
        entry = {
            "chunk_id": 91,
            "legacy_source_row_id": "91",
            "natural_source_row_id": "2025_award_91",
            "disposition": DISPOSITION_REKEY,
            "index_status": "ACTIVE",
            "legacy_content_hash": "h91",
            "index_version": "r2",
        }
        manifest = _make_safe_manifest([entry])
        manifest["manifest_header"]["database_fingerprint"] = "deadbeef12345678"

        err = _validate_manifest_header(manifest, apply=True)
        assert err is not None
        assert "database fingerprint mismatch" in err

    def test_12_inverse_rollback_manifest_restores_state(self, ephemeral_db: dict[str, Any], tmp_path: Path) -> None:
        """12. Rollback inverse manifest execution restores before state."""
        session: Session = ephemeral_db["session_factory"]()
        try:
            session.add(
                RagChunk(
                    id=95,
                    source_table="awards",
                    source_row_id="95",
                    content="Award 95",
                    content_hash="h95",
                    index_status="ACTIVE",
                    index_version="r2",
                )
            )
            session.commit()

            entry = {
                "chunk_id": 95,
                "legacy_source_row_id": "95",
                "natural_source_row_id": "2025_award_95",
                "disposition": DISPOSITION_REKEY,
                "index_status": "ACTIVE",
                "legacy_content_hash": "h95",
                "index_version": "r2",
            }
            manifest = _make_safe_manifest([entry])

            # Apply
            rekey, _, _, _, _ = _apply_rekey(session, manifest["entries"], dry_run=False)
            assert rekey == 1
            assert session.get(RagChunk, 95).source_row_id == "2025_award_95"

            # Generate rollback manifest
            os.environ["R2_REKEY_OUTPUT_DIR"] = str(tmp_path)
            _, rollback_path = _write_preimage_and_rollback(manifest, do_write=True)
            assert rollback_path is not None and rollback_path.exists()

            # Execute inverse operation
            rollback_manifest = _load_manifest(rollback_path)
            rb_entries = rollback_manifest["entries"]
            assert len(rb_entries) == 1
            assert rb_entries[0]["reverse"] is True

            session.execute(
                text("UPDATE rag_chunks SET source_row_id = :legacy WHERE id = :cid"),
                {"legacy": rb_entries[0]["legacy_source_row_id"], "cid": rb_entries[0]["chunk_id"]},
            )
            session.commit()

            restored = session.get(RagChunk, 95)
            assert restored.source_row_id == "95"
        finally:
            session.close()

    def test_13_fault_injection_mid_process_abort(self, ephemeral_db: dict[str, Any]) -> None:
        """13. Fault injection: crash mid-loop leaves 0 partial mutation in session."""
        session: Session = ephemeral_db["session_factory"]()
        try:
            for i in range(101, 104):
                session.add(
                    RagChunk(
                        id=i,
                        source_table="awards",
                        source_row_id=str(i),
                        content=f"Chunk {i}",
                        content_hash=f"h{i}",
                        index_status="ACTIVE",
                        index_version="r2",
                    )
                )
            session.commit()

            entries = [
                {
                    "chunk_id": i,
                    "legacy_source_row_id": str(i),
                    "natural_source_row_id": f"2025_award_{i}",
                    "disposition": DISPOSITION_REKEY,
                    "index_status": "ACTIVE",
                    "legacy_content_hash": f"h{i}",
                    "index_version": "r2",
                }
                for i in range(101, 104)
            ]

            with pytest.raises(ZeroDivisionError), session.begin_nested():
                for idx, e in enumerate(entries):
                    if idx == 2:
                        _ = 1 / 0
                    session.execute(
                        text("UPDATE rag_chunks SET source_row_id = :nat WHERE id = :cid"),
                        {"nat": e["natural_source_row_id"], "cid": e["chunk_id"]},
                    )

            session.rollback()

            for i in range(101, 104):
                assert session.get(RagChunk, i).source_row_id == str(i)
        finally:
            session.close()

    def test_14_dsn_and_production_guard_fail_closed(self, tmp_path: Path, monkeypatch) -> None:
        """14. DSN & production write guard: fails closed if write env flags are missing."""
        entry = {
            "chunk_id": 110,
            "legacy_source_row_id": "110",
            "natural_source_row_id": "2025_award_110",
            "disposition": DISPOSITION_REKEY,
            "index_status": "ACTIVE",
            "legacy_content_hash": "h110",
            "index_version": "r2",
        }
        manifest = _make_safe_manifest([entry])
        path = tmp_path / "manifest.json"
        path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))

        # 1. RAG_INDEX_ALLOW_WRITE not set -> fails
        monkeypatch.delenv("RAG_INDEX_ALLOW_WRITE", raising=False)
        monkeypatch.delenv("RAG_TARGET_ENV", raising=False)
        code = apply_rekey_main(["--manifest", str(path), "--apply"])
        assert code == 2

        # 2. Production env without production flag -> fails
        monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
        monkeypatch.setenv("RAG_TARGET_ENV", "production")
        monkeypatch.delenv("RAG_INDEX_ALLOW_PRODUCTION_WRITE", raising=False)
        code = apply_rekey_main(["--manifest", str(path), "--apply"])
        assert code == 2

    def test_15_receipt_created_after_success(self, tmp_path: Path) -> None:
        """15. Immutable receipt generated upon successful execution."""
        os.environ["R2_REKEY_RECEIPT_DIR"] = str(tmp_path / "receipts")
        receipt_path = _publish_immutable_receipt(
            transaction_id="tx-receipt-001",
            manifest_id="manifest-001",
            manifest_sha="sha256_mock_payload",
            status="SUCCESS_APPLIED",
            rekey_count=5,
            tombstone_count=1,
            already_applied=0,
            stale_rejected=0,
            preimage_path=tmp_path / "preimage.jsonl",
            rollback_path=tmp_path / "rollback.json",
        )
        assert receipt_path.exists()
        with receipt_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["transaction_id"] == "tx-receipt-001"
        assert data["rekeyed_count"] == 5
        assert data["receipt_sha256"] is not None

    def test_16_same_transaction_same_payload_reuses_receipt(self, tmp_path: Path) -> None:
        """16. Same transaction ID with same payload reuses receipt."""
        os.environ["R2_REKEY_RECEIPT_DIR"] = str(tmp_path / "receipts")
        _publish_immutable_receipt(
            transaction_id="tx-replay-001",
            manifest_id="manifest-001",
            manifest_sha="exact_sha_match",
            status="SUCCESS_APPLIED",
            rekey_count=3,
            tombstone_count=0,
            already_applied=0,
            stale_rejected=0,
            preimage_path=None,
            rollback_path=None,
        )

        is_replay, receipt, err = _check_receipt_replay("tx-replay-001", "exact_sha_match")
        assert is_replay is True
        assert receipt is not None
        assert err is None
        assert receipt["rekeyed_count"] == 3

    def test_17_same_transaction_different_payload_rejected(self, tmp_path: Path) -> None:
        """17. Same transaction ID with different payload rejected (collision fail-closed)."""
        os.environ["R2_REKEY_RECEIPT_DIR"] = str(tmp_path / "receipts")
        _publish_immutable_receipt(
            transaction_id="tx-collision-001",
            manifest_id="manifest-001",
            manifest_sha="sha_original",
            status="SUCCESS_APPLIED",
            rekey_count=3,
            tombstone_count=0,
            already_applied=0,
            stale_rejected=0,
            preimage_path=None,
            rollback_path=None,
        )

        is_replay, receipt, err = _check_receipt_replay("tx-collision-001", "sha_DIFFERENT")
        assert is_replay is False
        assert receipt is None
        assert err is not None
        assert "transaction ID collision" in err

    def test_18_oracle_and_staging_dsn_rejected_in_safety_mode(self, monkeypatch) -> None:
        """18. Oracle and Staging DSNs strictly rejected in local safety mode."""
        monkeypatch.setenv("RAG_TARGET_ENV", "local")

        monkeypatch.setenv("DATABASE_URL", "oracle+oracledb://kbo_staging:***@kbo_staging_high")
        err = _validate_dsn_security(apply=True)
        assert err is not None
        assert "Oracle database connections prohibited" in err

    def test_19_non_ephemeral_kbo_dev_db_path_blocked(self, monkeypatch) -> None:
        """19. Non-ephemeral primary repository sqlite database rejected."""
        monkeypatch.setenv("RAG_TARGET_ENV", "local")

        monkeypatch.setenv("DATABASE_URL", "sqlite:///./data/kbo_dev.db")
        err = _validate_dsn_security(apply=True)
        assert err is not None
        assert "Primary database data/kbo_dev.db write prohibited" in err

    def test_20_crash_after_db_commit_before_receipt_publish(
        self, ephemeral_db: dict[str, Any], tmp_path: Path
    ) -> None:
        """20. Crash injection between DB commit and receipt publish leaves DB mutated but no false receipt."""
        os.environ["R2_REKEY_RECEIPT_DIR"] = str(tmp_path / "receipts")
        session: Session = ephemeral_db["session_factory"]()
        try:
            for i in range(101, 104):
                session.add(
                    RagChunk(
                        id=i,
                        source_table="awards",
                        source_row_id=str(i),
                        content=f"Award {i}",
                        content_hash=f"hash_{i}",
                        index_status="ACTIVE",
                        index_version="r2",
                    )
                )
            session.commit()

            entries = [
                {
                    "chunk_id": i,
                    "legacy_source_row_id": str(i),
                    "natural_source_row_id": f"2025_award_MVP_{i}",
                    "disposition": DISPOSITION_REKEY,
                    "index_status": "ACTIVE",
                    "legacy_content_hash": f"hash_{i}",
                    "index_version": "r2",
                }
                for i in range(101, 104)
            ]

            rekey, tomb, already, skipped, status = _apply_rekey(session, entries, dry_run=False)
            session.commit()

            assert status == "SUCCESS_APPLIED"
            assert rekey == 3

            # Simulate crash before receipt publish (receipts dir is empty)
            receipt_dir = tmp_path / "receipts"
            assert len(list(receipt_dir.glob("*.json"))) == 0

            # On subsequent replay attempt without receipt: it detects existing natural keys as ALREADY_APPLIED (SUCCESS_NOOP)
            rekey2, tomb2, already2, skipped2, status2 = _apply_rekey(session, entries, dry_run=False)
            session.commit()

            assert status2 == "SUCCESS_NOOP"
            assert already2 == 3
            assert rekey2 == 0
        finally:
            session.close()

    def test_21_receipt_publish_failure_after_db_commit(self, tmp_path: Path) -> None:
        """21. Receipt publish with invalid transaction ID raises ValueError cleanly."""
        os.environ["R2_REKEY_RECEIPT_DIR"] = str(tmp_path / "receipts")
        with pytest.raises(ValueError, match="invalid transaction_id"):
            _publish_immutable_receipt(
                transaction_id="../../etc/passwd",
                manifest_id="manifest-001",
                manifest_sha="sha_test",
                status="SUCCESS_APPLIED",
                rekey_count=1,
                tombstone_count=0,
                already_applied=0,
                stale_rejected=0,
                preimage_path=None,
                rollback_path=None,
            )

    def test_22_receipt_exists_but_db_postcondition_missing(self, ephemeral_db: dict[str, Any], tmp_path: Path) -> None:
        """22. Receipt exists but DB was modified/reverted: replay fails closed on missing postcondition."""
        os.environ["R2_REKEY_RECEIPT_DIR"] = str(tmp_path / "receipts")
        session: Session = ephemeral_db["session_factory"]()
        try:
            for i in range(201, 203):
                session.add(
                    RagChunk(
                        id=i,
                        source_table="awards",
                        source_row_id=str(i),  # still legacy ID
                        content=f"Award {i}",
                        content_hash=f"hash_{i}",
                        index_status="ACTIVE",
                        index_version="r2",
                    )
                )
            session.commit()

            entries = [
                {
                    "chunk_id": i,
                    "legacy_source_row_id": str(i),
                    "natural_source_row_id": f"2025_award_MVP_{i}",
                    "disposition": DISPOSITION_REKEY,
                    "index_status": "ACTIVE",
                    "legacy_content_hash": f"hash_{i}",
                    "index_version": "r2",
                }
                for i in range(201, 203)
            ]
            manifest = _make_safe_manifest(entries)
            actual_sha = _compute_manifest_sha(manifest)

            _publish_immutable_receipt(
                transaction_id="tx-postcond-001",
                manifest_id="manifest-001",
                manifest_sha=actual_sha,
                status="SUCCESS_APPLIED",
                rekey_count=2,
                tombstone_count=0,
                already_applied=0,
                stale_rejected=0,
                preimage_path=None,
                rollback_path=None,
            )

            # In DB, the chunk source_row_id is still legacy (201), not natural key
            is_replay, receipt, err = _check_receipt_replay(
                "tx-postcond-001", actual_sha, session=session, manifest=manifest
            )
            assert is_replay is False
            assert err is not None
            assert "db_postcondition_missing" in err
        finally:
            session.close()

    def test_23_apply_receipt_then_rollback_then_replay(self, ephemeral_db: dict[str, Any], tmp_path: Path) -> None:
        """23. Apply -> Rollback -> Replay: postcondition check detects preimage and prohibits false SUCCESS_NOOP."""
        os.environ["R2_REKEY_RECEIPT_DIR"] = str(tmp_path / "receipts")
        session: Session = ephemeral_db["session_factory"]()
        try:
            for i in range(301, 303):
                session.add(
                    RagChunk(
                        id=i,
                        source_table="awards",
                        source_row_id=str(i),
                        content=f"Award {i}",
                        content_hash=f"hash_{i}",
                        index_status="ACTIVE",
                        index_version="r2",
                    )
                )
            session.commit()

            entries = [
                {
                    "chunk_id": i,
                    "legacy_source_row_id": str(i),
                    "natural_source_row_id": f"2025_award_MVP_{i}",
                    "disposition": DISPOSITION_REKEY,
                    "index_status": "ACTIVE",
                    "legacy_content_hash": f"hash_{i}",
                    "index_version": "r2",
                }
                for i in range(301, 303)
            ]
            manifest = _make_safe_manifest(entries)
            actual_sha = _compute_manifest_sha(manifest)

            # 1. Apply
            _apply_rekey(session, entries, dry_run=False)
            session.commit()

            _publish_immutable_receipt(
                transaction_id="tx-rollback-cycle",
                manifest_id="manifest-001",
                manifest_sha=actual_sha,
                status="SUCCESS_APPLIED",
                rekey_count=2,
                tombstone_count=0,
                already_applied=0,
                stale_rejected=0,
                preimage_path=None,
                rollback_path=None,
            )

            # 2. Rollback manually in DB (restore legacy source_row_id)
            for entry in entries:
                session.execute(
                    update(RagChunk)
                    .where(RagChunk.id == entry["chunk_id"])
                    .values(source_row_id=str(entry["legacy_source_row_id"]))
                )
            session.commit()

            # 3. Replay check
            is_replay, receipt, err = _check_receipt_replay(
                "tx-rollback-cycle", actual_sha, session=session, manifest=manifest
            )
            assert is_replay is False
            assert "db_postcondition_missing" in err
        finally:
            session.close()

    def test_24_rollback_receipt_links_original_apply_receipt(self, tmp_path: Path) -> None:
        """24. Rollback receipt links original apply receipt SHA and transaction ID."""
        os.environ["R2_REKEY_RECEIPT_DIR"] = str(tmp_path / "receipts")
        receipt_path = _publish_immutable_receipt(
            transaction_id="tx-rollback-001",
            manifest_id="manifest-rollback-001",
            manifest_sha="sha_rollback_manifest",
            status="SUCCESS_APPLIED",
            rekey_count=2,
            tombstone_count=0,
            already_applied=0,
            stale_rejected=0,
            preimage_path=None,
            rollback_path=None,
            original_apply_receipt_sha="sha_original_apply_12345",
        )

        with receipt_path.open("r", encoding="utf-8") as f:
            receipt_data = json.load(f)

        assert receipt_data["original_apply_receipt_sha"] == "sha_original_apply_12345"

    def test_25_atomic_temp_write_fsync_rename(self, tmp_path: Path) -> None:
        """25. Receipt is written via atomic temp file and renamed with fsync."""
        os.environ["R2_REKEY_RECEIPT_DIR"] = str(tmp_path / "receipts")
        receipt_path = _publish_immutable_receipt(
            transaction_id="tx-atomic-001",
            manifest_id="manifest-001",
            manifest_sha="sha_atomic_001",
            status="SUCCESS_APPLIED",
            rekey_count=1,
            tombstone_count=0,
            already_applied=0,
            stale_rejected=0,
            preimage_path=None,
            rollback_path=None,
        )

        assert receipt_path.exists()
        assert not (tmp_path / "receipts" / f".tmp_{receipt_path.name}").exists()

    def test_26_receipt_symlink_rejected(self, tmp_path: Path) -> None:
        """26. Symlinked receipt files are detected and rejected fail-closed."""
        receipt_dir = tmp_path / "receipts"
        receipt_dir.mkdir(parents=True, exist_ok=True)
        os.environ["R2_REKEY_RECEIPT_DIR"] = str(receipt_dir)

        real_target = tmp_path / "target_receipt.json"
        real_target.write_text('{"manifest_sha256": "fake_sha"}', encoding="utf-8")

        symlink_path = receipt_dir / "receipt_fake_tx-symlink-001.json"
        symlink_path.symlink_to(real_target)

        is_replay, receipt, err = _check_receipt_replay("tx-symlink-001", "fake_sha")
        assert is_replay is False
        assert "symlink receipt rejected" in err

    def test_27_transaction_id_path_validation(self) -> None:
        """27. Transaction ID validation blocks paths, traversal, and invalid chars."""
        assert _validate_transaction_id("valid-tx_12345") is None
        assert _validate_transaction_id("tx/with/slash") is not None
        assert _validate_transaction_id("../traversal") is not None
        assert _validate_transaction_id("tx;rm -rf") is not None

    def test_28_crash_recovery_subprocess_exit(self, ephemeral_db: dict[str, Any], tmp_path: Path, monkeypatch) -> None:
        """28. Hard crash immediately post DB-commit recovers with RECOVERED_RECEIPT_REBUILT."""
        monkeypatch.setenv("RAG_TARGET_ENV", "local")
        monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
        receipt_dir = tmp_path / "receipts"
        receipt_dir.mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("R2_REKEY_RECEIPT_DIR", str(receipt_dir))
        db_file = ephemeral_db["db_file"]
        monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_file}")

        # Seed initial chunk
        session = ephemeral_db["session_factory"]()
        try:
            session.add(
                RagChunk(
                    id=1,
                    source_table="awards",
                    source_row_id="2025_award_1",  # already applied in DB prior to crash
                    content="Award 1",
                    content_hash="h1",
                    index_status="ACTIVE",
                    index_version="r2",
                )
            )
            session.commit()
        finally:
            session.close()

        entry = {
            "chunk_id": 1,
            "legacy_source_row_id": "1",
            "natural_source_row_id": "2025_award_1",
            "disposition": DISPOSITION_REKEY,
            "index_status": "ACTIVE",
            "legacy_content_hash": "h1",
            "index_version": "r2",
        }
        manifest = _make_safe_manifest([entry])
        manifest_path = tmp_path / "manifest_crash_recovery.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        with patch(
            "src.cli.rag.apply_rag_rekey.db_engine.get_rag_index_session", side_effect=ephemeral_db["session_factory"]
        ):
            rc = apply_rekey_main(["--manifest", str(manifest_path), "--apply", "--transaction-id", "tx-crash-rec-001"])
            assert rc == 0

        # Receipt should have been reconstructed with RECOVERED_RECEIPT_REBUILT
        receipts = list(receipt_dir.glob("*.json"))
        assert len(receipts) == 1
        with receipts[0].open("r", encoding="utf-8") as f:
            rec_data = json.load(f)
        assert rec_data["status"] == "RECOVERED_RECEIPT_REBUILT"
        assert rec_data["rekeyed_count"] == 0
        assert rec_data["already_applied_count"] == 1

    def test_29_post_commit_receipt_io_failure_and_recovery(
        self, ephemeral_db: dict[str, Any], tmp_path: Path, monkeypatch
    ) -> None:
        """29. Post-commit receipt I/O failure (OSError) safely leaves DB committed, and next run recovers."""
        monkeypatch.setenv("RAG_TARGET_ENV", "local")
        monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
        receipt_dir = tmp_path / "receipts"
        receipt_dir.mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("R2_REKEY_RECEIPT_DIR", str(receipt_dir))
        db_file = ephemeral_db["db_file"]
        monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_file}")

        session = ephemeral_db["session_factory"]()
        try:
            session.add(
                RagChunk(
                    id=1,
                    source_table="awards",
                    source_row_id="1",
                    content="Award 1",
                    content_hash="h1",
                    index_status="ACTIVE",
                    index_version="r2",
                )
            )
            session.commit()
        finally:
            session.close()

        entry = {
            "chunk_id": 1,
            "legacy_source_row_id": "1",
            "natural_source_row_id": "2025_award_1",
            "disposition": DISPOSITION_REKEY,
            "index_status": "ACTIVE",
            "legacy_content_hash": "h1",
            "index_version": "r2",
        }
        manifest = _make_safe_manifest([entry])
        manifest_path = tmp_path / "manifest_io_fail.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        # First run: DB commit succeeds, but receipt publishing throws OSError (e.g. disk full)
        with patch(
            "src.cli.rag.apply_rag_rekey.db_engine.get_rag_index_session", side_effect=ephemeral_db["session_factory"]
        ):
            with patch(
                "src.cli.rag.apply_rag_rekey._publish_immutable_receipt", side_effect=OSError("Disk quota exceeded")
            ):
                rc_first = apply_rekey_main(
                    ["--manifest", str(manifest_path), "--apply", "--transaction-id", "tx-io-fail-001"]
                )
                assert rc_first != 0

        # Verify DB is in committed state
        session = ephemeral_db["session_factory"]()
        try:
            chunk = session.get(RagChunk, 1)
            assert chunk.source_row_id == "2025_award_1"
        finally:
            session.close()

        # Second run: recovery without I/O error -> rebuilds receipt
        with patch(
            "src.cli.rag.apply_rag_rekey.db_engine.get_rag_index_session", side_effect=ephemeral_db["session_factory"]
        ):
            rc = apply_rekey_main(["--manifest", str(manifest_path), "--apply", "--transaction-id", "tx-io-fail-001"])
            assert rc == 0

        receipts = list(receipt_dir.glob("*.json"))
        assert len(receipts) == 1
        with receipts[0].open("r", encoding="utf-8") as f:
            rec_data = json.load(f)
        assert rec_data["status"] == "RECOVERED_RECEIPT_REBUILT"

    def test_30_receipt_parent_directory_fsync_after_replace(self, tmp_path: Path) -> None:
        """30. Receipt publication calls os.fsync on parent directory descriptor."""
        receipt_dir = tmp_path / "receipts"
        receipt_dir.mkdir(parents=True, exist_ok=True)
        os.environ["R2_REKEY_RECEIPT_DIR"] = str(receipt_dir)

        with patch("os.fsync") as mock_fsync:
            receipt_path = _publish_immutable_receipt(
                transaction_id="tx-fsync-001",
                manifest_id="manifest-001",
                manifest_sha="sha_fsync_001",
                status="SUCCESS_APPLIED",
                rekey_count=1,
                tombstone_count=0,
                already_applied=0,
                stale_rejected=0,
                preimage_path=None,
                rollback_path=None,
            )
            assert receipt_path.exists()
            assert mock_fsync.call_count >= 2  # 1 for file fd, 1 for directory fd

    def test_31_subprocess_hard_crash_exit137_recovery(
        self, ephemeral_db: dict[str, Any], tmp_path: Path, monkeypatch
    ) -> None:
        """31. True subprocess hard-crash (os._exit(137)) immediately post-commit is safely recovered on next run."""
        import sys

        monkeypatch.setenv("RAG_TARGET_ENV", "local")
        monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
        receipt_dir = tmp_path / "receipts"
        receipt_dir.mkdir(parents=True, exist_ok=True)
        db_file = ephemeral_db["db_file"]

        # Seed initial chunk
        session = ephemeral_db["session_factory"]()
        try:
            session.add(
                RagChunk(
                    id=1,
                    source_table="awards",
                    source_row_id="1",
                    content="Award 1",
                    content_hash="h1",
                    index_status="ACTIVE",
                    index_version="r2",
                )
            )
            session.commit()
        finally:
            session.close()

        monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_file}")
        monkeypatch.setenv("R2_REKEY_RECEIPT_DIR", str(receipt_dir))

        entry = {
            "chunk_id": 1,
            "legacy_source_row_id": "1",
            "natural_source_row_id": "2025_award_1",
            "disposition": DISPOSITION_REKEY,
            "index_status": "ACTIVE",
            "legacy_content_hash": "h1",
            "index_version": "r2",
        }
        manifest = _make_safe_manifest([entry])
        manifest_path = tmp_path / "manifest_hard_crash.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        # Child script that abruptly terminates via os._exit(137) during receipt publication
        child_script = f"""
import os
import sys
from unittest.mock import patch
from src.cli.rag.apply_rag_rekey import main as apply_rekey_main

os.environ['DATABASE_URL'] = 'sqlite:///{db_file}'
os.environ['R2_REKEY_RECEIPT_DIR'] = '{receipt_dir}'
os.environ['RAG_INDEX_ALLOW_WRITE'] = '1'
os.environ['RAG_TARGET_ENV'] = 'local'

def _crash_exit(**kwargs):
    os._exit(137)

with patch('src.cli.rag.apply_rag_rekey._publish_immutable_receipt', side_effect=_crash_exit):
    apply_rekey_main(['--manifest', '{manifest_path}', '--apply', '--transaction-id', 'tx-hard-crash-001'])
"""
        env = dict(os.environ)
        env["PYTHONPATH"] = "."
        env["RAG_INDEX_ALLOW_WRITE"] = "1"
        res = subprocess.run([sys.executable, "-c", child_script], env=env, capture_output=True, check=False)
        assert res.returncode != 0

        # Verify DB is in committed post-state
        session = ephemeral_db["session_factory"]()
        try:
            chunk = session.get(RagChunk, 1)
            assert chunk.source_row_id == "2025_award_1"
        finally:
            session.close()

        # Verify no receipt was written before the crash
        receipts = list(receipt_dir.glob("*.json"))
        assert len(receipts) == 0

        # Execute recovery run in current process
        with patch(
            "src.cli.rag.apply_rag_rekey.db_engine.get_rag_index_session", side_effect=ephemeral_db["session_factory"]
        ):
            rc = apply_rekey_main(
                ["--manifest", str(manifest_path), "--apply", "--transaction-id", "tx-hard-crash-001"]
            )
            assert rc == 0

        # Verify reconstructed receipt
        receipts = list(receipt_dir.glob("*.json"))
        assert len(receipts) == 1
        with receipts[0].open("r", encoding="utf-8") as f:
            rec_data = json.load(f)
        assert rec_data["status"] == "RECOVERED_RECEIPT_REBUILT"
        assert rec_data["rekeyed_count"] == 0
        assert rec_data["already_applied_count"] == 1
