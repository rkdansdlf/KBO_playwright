"""Phase 105 Gate 4: Multi-Tier Rollback & Preimage Manifest Test Suite.

Certifies:
1. Pre-rehearsal state capture of target chunks (CAS fields).
2. JSON serialization and deserialization roundtrip for PreimageManifest.
3. Verification of state parity before and after rehearsal operations.
4. Detection of mutated row IDs, index status, or content hashes.
5. Priority 2 rollback application restoring exact previous values.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.models.base import Base
from src.models.rag_chunk import RagChunk
from src.services.staging_rollback import (
    PreimageEntry,
    PreimageManifest,
    apply_preimage_rollback,
    capture_pre_rehearsal_state,
    verify_rollback,
)


@pytest.fixture
def ephemeral_session() -> Session:
    """Provide an isolated in-memory database with rag_chunks table."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session = Session(bind=engine)

    # Seed 3 test chunks
    c1 = RagChunk(
        id=101,
        source_table="awards",
        source_row_id="101",
        content="MVP Winner 2024",
        content_hash="hash_101",
        index_version="v1",
        index_status="ACTIVE",
    )
    c2 = RagChunk(
        id=102,
        source_table="awards",
        source_row_id="102",
        content="Rookie of Year 2024",
        content_hash="hash_102",
        index_version="v1",
        index_status="ACTIVE",
    )
    c3 = RagChunk(
        id=501,
        source_table="player_season_batting",
        source_row_id="501",
        content="Batting stats 2024",
        content_hash="hash_501",
        index_version="v1",
        index_status="ACTIVE",
    )
    session.add_all([c1, c2, c3])
    session.commit()

    try:
        yield session
    finally:
        session.close()


class TestStagingRollbackEngine:
    """Certify rollback capture, verification, and preimage application."""

    def test_capture_empty_chunk_ids_returns_empty_manifest(self, ephemeral_session: Session) -> None:
        """Capturing state for an empty chunk list returns a valid empty manifest."""
        manifest = capture_pre_rehearsal_state(ephemeral_session, [])
        assert manifest.chunk_count == 0
        assert len(manifest.entries) == 0

    def test_capture_pre_rehearsal_state_populates_all_cas_fields(self, ephemeral_session: Session) -> None:
        """Preimage captures source_row_id, status, hash, and version accurately."""
        manifest = capture_pre_rehearsal_state(ephemeral_session, [101, 501])
        assert manifest.chunk_count == 2
        entry_map = {e.chunk_id: e for e in manifest.entries}

        assert entry_map[101].source_row_id == "101"
        assert entry_map[101].content_hash == "hash_101"
        assert entry_map[101].index_status == "ACTIVE"

        assert entry_map[501].source_row_id == "501"
        assert entry_map[501].source_table == "player_season_batting"

    def test_manifest_json_roundtrip(self) -> None:
        """PreimageManifest serializes to JSON and deserializes identically."""
        entry = PreimageEntry(
            chunk_id=101,
            source_table="awards",
            source_row_id="101",
            index_status="ACTIVE",
            content_hash="abc123hash",
            index_version="r2",
        )
        manifest = PreimageManifest(
            timestamp="2026-09-02T12:00:00Z",
            chunk_count=1,
            entries=[entry],
        )

        serialized = manifest.to_json()
        restored = PreimageManifest.from_json(serialized)

        assert restored.timestamp == manifest.timestamp
        assert restored.chunk_count == 1
        assert restored.entries[0].chunk_id == 101
        assert restored.entries[0].content_hash == "abc123hash"

    def test_verify_rollback_clean_state_returns_true(self, ephemeral_session: Session) -> None:
        """When DB state matches preimage exactly, verify_rollback returns True."""
        manifest = capture_pre_rehearsal_state(ephemeral_session, [101, 102])
        assert verify_rollback(ephemeral_session, manifest) is True

    def test_verify_rollback_detects_mutated_source_row_id(self, ephemeral_session: Session) -> None:
        """verify_rollback detects if a source_row_id was modified without restoration."""
        manifest = capture_pre_rehearsal_state(ephemeral_session, [101])

        # Mutate chunk 101
        chunk = ephemeral_session.get(RagChunk, 101)
        assert chunk is not None
        chunk.source_row_id = "award:MVP:2024:101"
        ephemeral_session.flush()

        assert verify_rollback(ephemeral_session, manifest) is False

    def test_verify_rollback_detects_mutated_status(self, ephemeral_session: Session) -> None:
        """verify_rollback detects if an index_status was modified (e.g. tombstoned)."""
        manifest = capture_pre_rehearsal_state(ephemeral_session, [102])

        chunk = ephemeral_session.get(RagChunk, 102)
        assert chunk is not None
        chunk.index_status = "DELETED"
        ephemeral_session.flush()

        assert verify_rollback(ephemeral_session, manifest) is False

    def test_apply_preimage_rollback_restores_state_fully(self, ephemeral_session: Session) -> None:
        """apply_preimage_rollback successfully reverses mutations and restores preimage."""
        manifest = capture_pre_rehearsal_state(ephemeral_session, [101, 102])

        # Mutate both chunks
        c1 = ephemeral_session.get(RagChunk, 101)
        c2 = ephemeral_session.get(RagChunk, 102)
        assert c1 is not None and c2 is not None
        c1.source_row_id = "award:MVP:2024:101"
        c2.index_status = "DELETED"
        ephemeral_session.flush()

        assert verify_rollback(ephemeral_session, manifest) is False

        # Apply Priority 2 preimage rollback
        restored = apply_preimage_rollback(ephemeral_session, manifest)
        assert restored == 2

        # Now verification must pass
        assert verify_rollback(ephemeral_session, manifest) is True

        # Check DB values directly
        ephemeral_session.refresh(c1)
        ephemeral_session.refresh(c2)
        assert c1.source_row_id == "101"
        assert c2.index_status == "ACTIVE"
