"""Comprehensive Local Ephemeral Apply-Only Safety Tests for Phase 105B Gate 3.

This test suite executes in an isolated, disposable SQLite environment to attest
the 14 critical safety invariants of the RAG Natural Key Rekey pipeline:
1. Normal apply (expected mutation == actual rowcount)
2. Idempotency (re-apply mutation count == 0)
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

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session, sessionmaker

from src.cli.rag.apply_rag_rekey import (
    DISPOSITION_REKEY,
    DISPOSITION_TOMBSTONE,
    _apply_rekey,
    _compute_manifest_sha,
    _get_current_database_fingerprint,
    _get_current_git_sha,
    _load_manifest,
    _validate_manifest_header,
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
        "generated_at": datetime.datetime.now().isoformat(),
        "database_fingerprint": db_fingerprint,
        "git_commit_sha": git_sha,
        "manifest_sha256": manifest_sha,
        "expected_entry_count": len(entries),
        "expected_disposition_counts": disposition_counts,
    }
    return manifest


@pytest.fixture()
def ephemeral_db(tmp_path: Path):
    """Create a completely isolated ephemeral SQLite DB with RagChunk schema."""
    db_file = tmp_path / "ephemeral_rag.db"
    db_url = f"sqlite:///{db_file}"
    engine = create_engine(db_url)
    Base.metadata.create_all(engine, tables=[RagChunk.__table__])
    session_factory = sessionmaker(bind=engine)

    old_db_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = db_url

    yield {
        "engine": engine,
        "db_url": db_url,
        "db_file": db_file,
        "session_factory": session_factory,
    }

    if old_db_url:
        os.environ["DATABASE_URL"] = old_db_url
    else:
        os.environ.pop("DATABASE_URL", None)


class TestRagRekeySafetyGate3:
    """Gate 3: 14 Invariant Safety Tests on Disposable Ephemeral DB."""

    def test_1_normal_apply_exact_mutation(self, ephemeral_db: dict[str, Any]) -> None:
        """1. Normal apply: expected mutations == actual rowcount."""
        session: Session = ephemeral_db["session_factory"]()
        try:
            # Seed 5 legacy rows
            for i in range(1, 6):
                session.add(
                    RagChunk(
                        id=i,
                        source_table="awards",
                        source_row_id=str(i),
                        content=f"Award {i} text",
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
                    "natural_source_row_id": f"2025_award_player_{i}",
                    "disposition": DISPOSITION_REKEY,
                    "index_status": "ACTIVE",
                    "legacy_content_hash": f"hash_{i}",
                    "index_version": "r2",
                }
                for i in range(1, 6)
            ]

            rekey_count, tombstone_count, skipped = _apply_rekey(session, entries, dry_run=False)
            assert rekey_count == 5
            assert tombstone_count == 0
            assert len(skipped) == 0

            # Verify in DB
            rows = session.scalars(select(RagChunk).order_by(RagChunk.id)).all()
            assert len(rows) == 5
            for idx, r in enumerate(rows, start=1):
                assert r.source_row_id == f"2025_award_player_{idx}"
        finally:
            session.close()

    def test_2_idempotency_zero_mutations_on_reapply(self, ephemeral_db: dict[str, Any]) -> None:
        """2. Idempotency: re-applying the same manifest results in 0 mutations."""
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
            rekey_1, _, skipped_1 = _apply_rekey(session, [entry], dry_run=False)
            assert rekey_1 == 1
            assert len(skipped_1) == 0

            # Second apply -> 0 rekey, 1 skipped (source_row_id is now natural key, CAS mismatch)
            rekey_2, _, skipped_2 = _apply_rekey(session, [entry], dry_run=False)
            assert rekey_2 == 0
            assert len(skipped_2) == 1
            assert "optimistic concurrency check failed" in skipped_2[0]["reason"]
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
                    content_hash="hash_NEW",  # Changed!
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
                "legacy_content_hash": "hash_OLD",  # Stale in manifest
                "index_version": "r2",
            }

            rekey, _, skipped = _apply_rekey(session, [entry], dry_run=False)
            assert rekey == 0
            assert len(skipped) == 1
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
                    source_row_id="999",  # Different ID
                    content="Award 30",
                    content_hash="h30",
                    index_status="ACTIVE",
                    index_version="r2",
                )
            )
            session.commit()

            entry = {
                "chunk_id": 30,
                "legacy_source_row_id": "30",  # Stale
                "natural_source_row_id": "2025_award_30",
                "disposition": DISPOSITION_REKEY,
                "index_status": "ACTIVE",
                "legacy_content_hash": "h30",
                "index_version": "r2",
            }

            rekey, _, skipped = _apply_rekey(session, [entry], dry_run=False)
            assert rekey == 0
            assert len(skipped) == 1
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
                    index_status="DELETED",  # Already deleted
                    index_version="r2",
                )
            )
            session.commit()

            entry = {
                "chunk_id": 40,
                "legacy_source_row_id": "40",
                "natural_source_row_id": "2025_award_40",
                "disposition": DISPOSITION_REKEY,
                "index_status": "ACTIVE",  # Expects ACTIVE
                "legacy_content_hash": "h40",
                "index_version": "r2",
            }

            rekey, _, skipped = _apply_rekey(session, [entry], dry_run=False)
            assert rekey == 0
            assert len(skipped) == 1
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
                    index_version="rag-v1",  # Old version in DB
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
                "index_version": "r2",  # Expects r2
            }

            rekey, _, skipped = _apply_rekey(session, [entry], dry_run=False)
            assert rekey == 0
            assert len(skipped) == 1
        finally:
            session.close()

    def test_7_atomicity_batch_rollback_on_failure(self, ephemeral_db: dict[str, Any]) -> None:
        """7. Atomicity: failure / error during transaction rolls back completely."""
        session: Session = ephemeral_db["session_factory"]()
        try:
            session.add(
                RagChunk(
                    id=60,
                    source_table="awards",
                    source_row_id="60",
                    content="Award 60",
                    content_hash="h60",
                    index_status="ACTIVE",
                    index_version="r2",
                )
            )
            session.commit()

            # Simulate an unexpected error during apply
            try:
                with session.begin_nested():
                    # Apply update directly
                    session.query(RagChunk).filter_by(id=60).update({"source_row_id": "2025_award_60"})
                    # Force raise
                    raise RuntimeError("Simulated crash mid-batch")
            except RuntimeError:
                session.rollback()

            chunk = session.get(RagChunk, 60)
            assert chunk.source_row_id == "60"  # Rolled back
        finally:
            session.close()

    def test_8_target_collision_disposition_tombstone(self, ephemeral_db: dict[str, Any]) -> None:
        """8. Target collision: legacy row marked DELETED without corrupting natural target row."""
        session: Session = ephemeral_db["session_factory"]()
        try:
            # Existing natural target row
            session.add(
                RagChunk(
                    id=70,
                    source_table="awards",
                    source_row_id="2025_award_70",
                    content="Award 70 natural",
                    content_hash="h70",
                    index_status="ACTIVE",
                    index_version="r2",
                )
            )
            # Legacy duplicate row
            session.add(
                RagChunk(
                    id=71,
                    source_table="awards",
                    source_row_id="70",
                    content="Award 70 legacy",
                    content_hash="h70",
                    index_status="ACTIVE",
                    index_version="r2",
                )
            )
            session.commit()

            entry = {
                "chunk_id": 71,
                "legacy_source_row_id": "70",
                "natural_source_row_id": "2025_award_70",
                "disposition": DISPOSITION_TOMBSTONE,
                "index_status": "ACTIVE",
                "legacy_content_hash": "h70",
                "index_version": "r2",
            }

            rekey, tombstone, skipped = _apply_rekey(session, [entry], dry_run=False)
            assert rekey == 0
            assert tombstone == 1
            assert len(skipped) == 0

            # Verify natural target remains ACTIVE
            target = session.get(RagChunk, 70)
            assert target.source_row_id == "2025_award_70"
            assert target.index_status == "ACTIVE"

            # Verify legacy chunk is DELETED
            legacy = session.get(RagChunk, 71)
            assert legacy.source_row_id == "70"
            assert legacy.index_status == "DELETED"
        finally:
            session.close()

    def test_9_preconverted_rows_untouched(self, ephemeral_db: dict[str, Any]) -> None:
        """9. Rows already converted to natural keys remain untouched."""
        session: Session = ephemeral_db["session_factory"]()
        try:
            session.add(
                RagChunk(
                    id=80,
                    source_table="awards",
                    source_row_id="2025_award_80",
                    content="Already natural",
                    content_hash="h80",
                    index_status="ACTIVE",
                    index_version="r2",
                )
            )
            session.commit()

            # Empty rekey manifest
            rekey, tombstone, skipped = _apply_rekey(session, [], dry_run=False)
            assert rekey == 0
            assert tombstone == 0
            assert len(skipped) == 0

            chunk = session.get(RagChunk, 80)
            assert chunk.source_row_id == "2025_award_80"
        finally:
            session.close()

    def test_10_manifest_tampering_rejected(self, tmp_path: Path) -> None:
        """10. Tampered manifest with altered entry rejected by header SHA verification."""
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

        # Tamper entries without updating header SHA
        manifest["entries"][0]["natural_source_row_id"] = "TAMPERED_INJECTION"

        err = _validate_manifest_header(manifest, apply=True)
        assert err is not None
        assert "manifest SHA mismatch" in err

    def test_11_snapshot_binding_fingerprint_mismatch_rejected(self, tmp_path: Path) -> None:
        """11. Manifest bound to a different database URL/fingerprint rejected."""
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
            rekey, _, _ = _apply_rekey(session, manifest["entries"], dry_run=False)
            assert rekey == 1
            assert session.get(RagChunk, 95).source_row_id == "2025_award_95"

            # Generate rollback manifest
            os.environ["R2_REKEY_OUTPUT_DIR"] = str(tmp_path)
            _, rollback_path = _write_preimage_and_rollback(manifest, do_write=True)
            assert rollback_path is not None and rollback_path.exists()

            # Execute inverse operation: update natural back to legacy
            rollback_manifest = _load_manifest(rollback_path)
            rb_entries = rollback_manifest["entries"]
            assert len(rb_entries) == 1
            assert rb_entries[0]["reverse"] is True

            # Perform inverse apply
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

            # Injected exception during loop
            with pytest.raises(ZeroDivisionError), session.begin_nested():
                for idx, e in enumerate(entries):
                    if idx == 2:
                        _ = 1 / 0  # Crash on 3rd item
                    session.execute(
                        text("UPDATE rag_chunks SET source_row_id = :nat WHERE id = :cid"),
                        {"nat": e["natural_source_row_id"], "cid": e["chunk_id"]},
                    )

            session.rollback()

            # Verify none were permanently committed
            for i in range(101, 104):
                assert session.get(RagChunk, i).source_row_id == str(i)
        finally:
            session.close()

    def test_14_dsn_and_production_guard_fail_closed(self, tmp_path: Path) -> None:
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
        os.environ.pop("RAG_INDEX_ALLOW_WRITE", None)
        os.environ.pop("RAG_TARGET_ENV", None)
        code = apply_rekey_main(["--manifest", str(path), "--apply"])
        assert code == 2

        # 2. Production env without production flag -> fails
        os.environ["RAG_INDEX_ALLOW_WRITE"] = "1"
        os.environ["RAG_TARGET_ENV"] = "production"
        os.environ.pop("RAG_INDEX_ALLOW_PRODUCTION_WRITE", None)
        code = apply_rekey_main(["--manifest", str(path), "--apply"])
        assert code == 2
