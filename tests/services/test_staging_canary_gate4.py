"""Phase 105 Gate 4: 6-Archetype Canary Matrix Test Suite.

Certifies:
1. Individual execution of all 6 canonical operational archetypes:
   - SAFE_REKEY (awards domain numeric -> natural key)
   - SAFE_REKEY_STATS (batting domain numeric -> natural key)
   - TARGET_COLLISION_TOMBSTONE (collision handling with status=DELETED)
   - ALREADY_APPLIED_NOOP (idempotent 0-mutation skip)
   - STALE_CAS_REJECT (hash mismatch fail-closed rejection)
   - INVERSE_ROLLBACK_REPLAY (preimage restoration)
2. Full rehearsal matrix execution with automatic Priority 1 rollback.
3. Verification that 0 persistent DB mutations occur during the entire rehearsal run.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.models.base import Base
from src.models.rag_chunk import RagChunk
from src.services.staging_canary import (
    CanaryArchetype,
    execute_archetype_already_applied_noop,
    execute_archetype_collision_tombstone,
    execute_archetype_inverse_rollback_replay,
    execute_archetype_safe_rekey,
    execute_archetype_safe_rekey_stats,
    execute_archetype_stale_cas_reject,
    run_canary_rehearsal_matrix,
)


@pytest.fixture
def canary_session() -> Session:
    """Provide an isolated in-memory DB populated with the 6 canary archetype fixtures."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session = Session(bind=engine)

    # 1. SAFE_REKEY: awards legacy chunk
    c1 = RagChunk(
        id=101,
        source_table="awards",
        source_row_id="101",
        content="MVP Winner Kim 2024",
        content_hash="hash_c1_mvp",
        index_version="r2",
        index_status="ACTIVE",
    )
    # 2. SAFE_REKEY_STATS: batting stats legacy chunk
    c2 = RagChunk(
        id=501,
        source_table="player_season_batting",
        source_row_id="501",
        content="Batting stats 2024",
        content_hash="hash_c2_stats",
        index_version="r2",
        index_status="ACTIVE",
    )
    # 3. TARGET_COLLISION_TOMBSTONE: legacy chunk 102 + existing natural chunk 202
    c3_legacy = RagChunk(
        id=102,
        source_table="awards",
        source_row_id="102",
        content="Rookie of Year Lee 2024",
        content_hash="hash_c3_rookie",
        index_version="r2",
        index_status="ACTIVE",
    )
    c3_natural = RagChunk(
        id=202,
        source_table="awards",
        source_row_id="award:ROOKIE:2024:202",
        content="Rookie of Year Lee 2024",
        content_hash="hash_c3_rookie",
        index_version="r2",
        index_status="ACTIVE",
    )
    # 4. ALREADY_APPLIED_NOOP: chunk 103 already rekeyed
    c4 = RagChunk(
        id=103,
        source_table="awards",
        source_row_id="award:GOLDEN_GLOVE:2024:103",
        content="Golden Glove Park 2024",
        content_hash="hash_c4_gg",
        index_version="r2",
        index_status="ACTIVE",
    )
    # 5. STALE_CAS_REJECT: chunk 601 with hash in DB
    c5 = RagChunk(
        id=601,
        source_table="player_season_pitching",
        source_row_id="601",
        content="Pitching stats 2024",
        content_hash="db_hash_current_different",
        index_version="r2",
        index_status="ACTIVE",
    )
    session.add_all([c1, c2, c3_legacy, c3_natural, c4, c5])
    session.commit()

    try:
        yield session
    finally:
        session.close()


class TestCanaryArchetypesIndividual:
    """Certify each of the 6 archetypes individually."""

    def test_archetype_1_safe_rekey(self, canary_session: Session) -> None:
        """Archetype 1 rekeys legacy awards chunk to natural key when CAS matches."""
        outcome = execute_archetype_safe_rekey(
            canary_session,
            chunk_id=101,
            target_natural_key="award:MVP:2024:101",
            expected_content_hash="hash_c1_mvp",
        )
        assert outcome.status == "PASS"
        assert outcome.action_taken == "UPDATE"
        assert outcome.mutations == 1

        chunk = canary_session.get(RagChunk, 101)
        assert chunk is not None
        assert chunk.source_row_id == "award:MVP:2024:101"

    def test_archetype_2_safe_rekey_stats(self, canary_session: Session) -> None:
        """Archetype 2 rekeys legacy batting stats chunk to natural key when CAS matches."""
        outcome = execute_archetype_safe_rekey_stats(
            canary_session,
            chunk_id=501,
            target_natural_key="batting:2024:62931:LT:REGULAR:1군",
            expected_content_hash="hash_c2_stats",
        )
        assert outcome.status == "PASS"
        assert outcome.action_taken == "UPDATE"
        assert outcome.mutations == 1

        chunk = canary_session.get(RagChunk, 501)
        assert chunk is not None
        assert chunk.source_row_id == "batting:2024:62931:LT:REGULAR:1군"

    def test_archetype_3_target_collision_tombstone(self, canary_session: Session) -> None:
        """Archetype 3 tombstones legacy chunk when natural key collision is detected."""
        outcome = execute_archetype_collision_tombstone(
            canary_session,
            legacy_chunk_id=102,
            target_natural_key="award:ROOKIE:2024:202",
        )
        assert outcome.status == "PASS"
        assert outcome.action_taken == "TOMBSTONE"
        assert outcome.mutations == 1

        legacy = canary_session.get(RagChunk, 102)
        natural = canary_session.get(RagChunk, 202)
        assert legacy is not None and natural is not None
        assert legacy.index_status == "DELETED"
        assert natural.index_status == "ACTIVE"

    def test_archetype_4_already_applied_noop(self, canary_session: Session) -> None:
        """Archetype 4 detects chunk already rekeyed and applies 0 mutations."""
        outcome = execute_archetype_already_applied_noop(
            canary_session,
            chunk_id=103,
            natural_key="award:GOLDEN_GLOVE:2024:103",
        )
        assert outcome.status == "PASS"
        assert outcome.action_taken == "NOOP"
        assert outcome.mutations == 0

    def test_archetype_5_stale_cas_reject(self, canary_session: Session) -> None:
        """Archetype 5 detects hash mismatch, aborts update, and leaves row unchanged."""
        outcome = execute_archetype_stale_cas_reject(
            canary_session,
            chunk_id=601,
            manifest_expected_hash="stale_manifest_hash_different",
            attempted_new_key="pitching:2024:1111:OB:REGULAR",
        )
        assert outcome.status == "PASS"
        assert outcome.action_taken == "REJECT"
        assert outcome.mutations == 0

        chunk = canary_session.get(RagChunk, 601)
        assert chunk is not None
        assert chunk.source_row_id == "601"

    def test_archetype_6_inverse_rollback_replay(self, canary_session: Session) -> None:
        """Archetype 6 restores chunk back to legacy ID via preimage application."""
        # Mutate chunk 101 first
        chunk = canary_session.get(RagChunk, 101)
        assert chunk is not None
        chunk.source_row_id = "award:MVP:2024:101"
        canary_session.flush()

        outcome = execute_archetype_inverse_rollback_replay(
            canary_session,
            chunk_id=101,
            preimage_row_id="101",
            preimage_status="ACTIVE",
        )
        assert outcome.status == "PASS"
        assert outcome.action_taken == "RESTORE"
        assert outcome.mutations == 1

        canary_session.refresh(chunk)
        assert chunk.source_row_id == "101"


class TestCanaryMatrixRehearsal:
    """Certify full canary matrix run with Priority 1 Rollback."""

    def test_full_canary_rehearsal_matrix_passes_and_rolls_back_cleanly(self, canary_session: Session) -> None:
        """Full matrix runs all 6 archetypes, passes 100%, and rolls back all mutations."""
        configs = {
            CanaryArchetype.SAFE_REKEY: {
                "chunk_id": 101,
                "target_natural_key": "award:MVP:2024:101",
                "expected_content_hash": "hash_c1_mvp",
            },
            CanaryArchetype.SAFE_REKEY_STATS: {
                "chunk_id": 501,
                "target_natural_key": "batting:2024:62931:LT:REGULAR:1군",
                "expected_content_hash": "hash_c2_stats",
            },
            CanaryArchetype.TARGET_COLLISION_TOMBSTONE: {
                "legacy_chunk_id": 102,
                "target_natural_key": "award:ROOKIE:2024:202",
                "chunk_id": 102,
            },
            CanaryArchetype.ALREADY_APPLIED_NOOP: {
                "chunk_id": 103,
                "natural_key": "award:GOLDEN_GLOVE:2024:103",
            },
            CanaryArchetype.STALE_CAS_REJECT: {
                "chunk_id": 601,
                "manifest_expected_hash": "stale_hash_from_manifest",
                "attempted_new_key": "pitching:2024:601:NC:REGULAR",
            },
            CanaryArchetype.INVERSE_ROLLBACK_REPLAY: {
                "chunk_id": 101,
                "preimage_row_id": "101",
                "preimage_status": "ACTIVE",
            },
        }

        result = run_canary_rehearsal_matrix(canary_session, configs)

        assert result.passed is True
        assert result.total_canaries == 6
        assert result.pass_count == 6
        assert result.rollback_performed is True
        assert result.error is None

        # Verify Priority 1 Rollback invariant: chunk 101 must remain "101"
        c1 = canary_session.get(RagChunk, 101)
        assert c1 is not None
        assert c1.source_row_id == "101"

        # chunk 102 must remain "ACTIVE"
        c2 = canary_session.get(RagChunk, 102)
        assert c2 is not None
        assert c2.index_status == "ACTIVE"
