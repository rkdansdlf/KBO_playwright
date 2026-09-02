"""Phase 105 Gate 4: Deterministic 6-Archetype Canary Test Matrix.

Executes the 6 canonical staging rehearsal archetypes across multiple source domains:
1. SAFE_REKEY: Numeric legacy ID -> Natural key (awards)
2. SAFE_REKEY_STATS: Numeric legacy ID -> Natural key (player_season_batting)
3. TARGET_COLLISION_TOMBSTONE: Duplicate natural key exists -> Tombstone legacy chunk
4. ALREADY_APPLIED_NOOP: Chunk already rekeyed -> Clean skip with 0 mutations
5. STALE_CAS_REJECT: DB hash != manifest hash -> Reject mutation, fail-closed
6. INVERSE_ROLLBACK_REPLAY: Restore legacy key from preimage manifest

All canaries run inside a transaction with automatic Priority 1 rollback
ensuring 0 permanent database mutations during the rehearsal.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from sqlalchemy import select, update
from sqlalchemy.exc import SQLAlchemyError

from src.models.rag_chunk import RagChunk
from src.services.staging_rollback import (
    apply_preimage_rollback,
    capture_pre_rehearsal_state,
    verify_rollback,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

CANARY_ARCHETYPE_COUNT = 6


class CanaryArchetype(StrEnum):
    """The 6 canonical operational archetypes for staging rehearsal."""

    SAFE_REKEY = "SAFE_REKEY"
    SAFE_REKEY_STATS = "SAFE_REKEY_STATS"
    TARGET_COLLISION_TOMBSTONE = "TARGET_COLLISION_TOMBSTONE"
    ALREADY_APPLIED_NOOP = "ALREADY_APPLIED_NOOP"
    STALE_CAS_REJECT = "STALE_CAS_REJECT"
    INVERSE_ROLLBACK_REPLAY = "INVERSE_ROLLBACK_REPLAY"


@dataclass
class CanaryOutcome:
    """Detailed verification record for a single canary test case."""

    archetype: CanaryArchetype
    source_table: str
    chunk_id: int
    status: str  # "PASS" | "FAIL"
    action_taken: str  # "UPDATE" | "TOMBSTONE" | "NOOP" | "REJECT" | "RESTORE"
    mutations: int
    detail: str


@dataclass
class CanaryResult:
    """Overall outcome of the 6-archetype canary test matrix."""

    passed: bool
    outcomes: list[CanaryOutcome]
    rollback_performed: bool
    error: str | None = None

    @property
    def total_canaries(self) -> int:
        """Return total number of canary outcomes."""
        return len(self.outcomes)

    @property
    def pass_count(self) -> int:
        """Return total count of passing canaries."""
        return sum(1 for o in self.outcomes if o.status == "PASS")


def execute_archetype_safe_rekey(
    session: Session,
    chunk_id: int,
    target_natural_key: str,
    expected_content_hash: str,
) -> CanaryOutcome:
    """Archetype 1: Numeric legacy ID -> Natural key (awards)."""
    chunk = session.get(RagChunk, chunk_id)
    if not chunk:
        return CanaryOutcome(
            archetype=CanaryArchetype.SAFE_REKEY,
            source_table="awards",
            chunk_id=chunk_id,
            status="FAIL",
            action_taken="NONE",
            mutations=0,
            detail=f"Target chunk {chunk_id} does not exist",
        )

    if chunk.content_hash != expected_content_hash:
        return CanaryOutcome(
            archetype=CanaryArchetype.SAFE_REKEY,
            source_table=chunk.source_table,
            chunk_id=chunk_id,
            status="FAIL",
            action_taken="NONE",
            mutations=0,
            detail="CAS check failed: content hash mismatch",
        )

    stmt = (
        update(RagChunk)
        .where(RagChunk.id == chunk_id, RagChunk.content_hash == expected_content_hash)
        .values(source_row_id=target_natural_key)
    )
    res = session.execute(stmt)
    session.flush()

    passed = res.rowcount == 1
    return CanaryOutcome(
        archetype=CanaryArchetype.SAFE_REKEY,
        source_table=chunk.source_table,
        chunk_id=chunk_id,
        status="PASS" if passed else "FAIL",
        action_taken="UPDATE",
        mutations=res.rowcount,
        detail=f"Rekeyed to {target_natural_key}",
    )


def execute_archetype_safe_rekey_stats(
    session: Session,
    chunk_id: int,
    target_natural_key: str,
    expected_content_hash: str,
) -> CanaryOutcome:
    """Archetype 2: Numeric legacy ID -> Natural key (player_season_batting)."""
    chunk = session.get(RagChunk, chunk_id)
    if not chunk:
        return CanaryOutcome(
            archetype=CanaryArchetype.SAFE_REKEY_STATS,
            source_table="player_season_batting",
            chunk_id=chunk_id,
            status="FAIL",
            action_taken="NONE",
            mutations=0,
            detail=f"Target chunk {chunk_id} does not exist",
        )

    stmt = (
        update(RagChunk)
        .where(RagChunk.id == chunk_id, RagChunk.content_hash == expected_content_hash)
        .values(source_row_id=target_natural_key)
    )
    res = session.execute(stmt)
    session.flush()

    passed = res.rowcount == 1
    return CanaryOutcome(
        archetype=CanaryArchetype.SAFE_REKEY_STATS,
        source_table=chunk.source_table,
        chunk_id=chunk_id,
        status="PASS" if passed else "FAIL",
        action_taken="UPDATE",
        mutations=res.rowcount,
        detail=f"Rekeyed to stats natural key {target_natural_key}",
    )


def execute_archetype_collision_tombstone(
    session: Session,
    legacy_chunk_id: int,
    target_natural_key: str,
) -> CanaryOutcome:
    """Archetype 3: Natural key already exists with same content -> Tombstone legacy."""
    legacy_chunk = session.get(RagChunk, legacy_chunk_id)
    if not legacy_chunk:
        return CanaryOutcome(
            archetype=CanaryArchetype.TARGET_COLLISION_TOMBSTONE,
            source_table="awards",
            chunk_id=legacy_chunk_id,
            status="FAIL",
            action_taken="NONE",
            mutations=0,
            detail=f"Legacy chunk {legacy_chunk_id} not found",
        )

    natural_stmt = select(RagChunk).where(
        RagChunk.source_table == legacy_chunk.source_table,
        RagChunk.source_row_id == target_natural_key,
    )
    natural_chunk = session.execute(natural_stmt).scalar_one_or_none()

    if not natural_chunk:
        return CanaryOutcome(
            archetype=CanaryArchetype.TARGET_COLLISION_TOMBSTONE,
            source_table=legacy_chunk.source_table,
            chunk_id=legacy_chunk_id,
            status="FAIL",
            action_taken="NONE",
            mutations=0,
            detail=f"Collision target {target_natural_key} not present to justify tombstone",
        )

    stmt = update(RagChunk).where(RagChunk.id == legacy_chunk_id).values(index_status="DELETED")
    res = session.execute(stmt)
    session.flush()

    return CanaryOutcome(
        archetype=CanaryArchetype.TARGET_COLLISION_TOMBSTONE,
        source_table=legacy_chunk.source_table,
        chunk_id=legacy_chunk_id,
        status="PASS" if res.rowcount == 1 else "FAIL",
        action_taken="TOMBSTONE",
        mutations=res.rowcount,
        detail=f"Legacy chunk tombstoned; natural chunk {natural_chunk.id} preserved",
    )


def execute_archetype_already_applied_noop(
    session: Session,
    chunk_id: int,
    natural_key: str,
) -> CanaryOutcome:
    """Archetype 4: Chunk already has target natural key -> 0 mutation skip."""
    chunk = session.get(RagChunk, chunk_id)
    if not chunk:
        return CanaryOutcome(
            archetype=CanaryArchetype.ALREADY_APPLIED_NOOP,
            source_table="awards",
            chunk_id=chunk_id,
            status="FAIL",
            action_taken="NONE",
            mutations=0,
            detail=f"Chunk {chunk_id} not found",
        )

    if chunk.source_row_id == natural_key:
        return CanaryOutcome(
            archetype=CanaryArchetype.ALREADY_APPLIED_NOOP,
            source_table=chunk.source_table,
            chunk_id=chunk_id,
            status="PASS",
            action_taken="NOOP",
            mutations=0,
            detail="Already possesses target natural key; 0 mutation applied",
        )

    return CanaryOutcome(
        archetype=CanaryArchetype.ALREADY_APPLIED_NOOP,
        source_table=chunk.source_table,
        chunk_id=chunk_id,
        status="FAIL",
        action_taken="NONE",
        mutations=0,
        detail=f"Chunk row_id {chunk.source_row_id!r} != {natural_key!r}",
    )


def execute_archetype_stale_cas_reject(
    session: Session,
    chunk_id: int,
    manifest_expected_hash: str,
    attempted_new_key: str,
) -> CanaryOutcome:
    """Archetype 5: DB hash != expected manifest hash -> Fail-closed abort."""
    chunk = session.get(RagChunk, chunk_id)
    if not chunk:
        return CanaryOutcome(
            archetype=CanaryArchetype.STALE_CAS_REJECT,
            source_table="player_season_pitching",
            chunk_id=chunk_id,
            status="FAIL",
            action_taken="NONE",
            mutations=0,
            detail=f"Chunk {chunk_id} not found",
        )

    stmt = (
        update(RagChunk)
        .where(RagChunk.id == chunk_id, RagChunk.content_hash == manifest_expected_hash)
        .values(source_row_id=attempted_new_key)
    )
    res = session.execute(stmt)
    session.flush()

    if res.rowcount == 0:
        return CanaryOutcome(
            archetype=CanaryArchetype.STALE_CAS_REJECT,
            source_table=chunk.source_table,
            chunk_id=chunk_id,
            status="PASS",
            action_taken="REJECT",
            mutations=0,
            detail="CAS mismatch detected: update rejected safely (0 mutations)",
        )

    return CanaryOutcome(
        archetype=CanaryArchetype.STALE_CAS_REJECT,
        source_table=chunk.source_table,
        chunk_id=chunk_id,
        status="FAIL",
        action_taken="UNEXPECTED_UPDATE",
        mutations=res.rowcount,
        detail="Stale CAS update unexpectedly mutated row",
    )


def execute_archetype_inverse_rollback_replay(
    session: Session,
    chunk_id: int,
    preimage_row_id: str,
    preimage_status: str,
) -> CanaryOutcome:
    """Archetype 6: Apply inverse preimage to restore initial state."""
    from src.services.staging_rollback import PreimageEntry, PreimageManifest

    chunk = session.get(RagChunk, chunk_id)
    if not chunk:
        return CanaryOutcome(
            archetype=CanaryArchetype.INVERSE_ROLLBACK_REPLAY,
            source_table="awards",
            chunk_id=chunk_id,
            status="FAIL",
            action_taken="NONE",
            mutations=0,
            detail=f"Chunk {chunk_id} not found",
        )

    preimage = PreimageManifest(
        timestamp="2026-09-02T00:00:00Z",
        chunk_count=1,
        entries=[
            PreimageEntry(
                chunk_id=chunk_id,
                source_table=chunk.source_table,
                source_row_id=preimage_row_id,
                index_status=preimage_status,
                content_hash=chunk.content_hash,
                index_version=chunk.index_version,
            )
        ],
    )

    restored = apply_preimage_rollback(session, preimage)
    verified = verify_rollback(session, preimage)

    passed = restored == 1 and verified
    return CanaryOutcome(
        archetype=CanaryArchetype.INVERSE_ROLLBACK_REPLAY,
        source_table=chunk.source_table,
        chunk_id=chunk_id,
        status="PASS" if passed else "FAIL",
        action_taken="RESTORE",
        mutations=restored,
        detail=f"Preimage restored source_row_id to {preimage_row_id} and verified={verified}",
    )


def run_canary_rehearsal_matrix(
    session: Session,
    canary_configs: dict[CanaryArchetype, dict[str, Any]],
) -> CanaryResult:
    """Execute the full 6-archetype canary test matrix with Priority 1 Rollback.

    Always issues session.rollback() at the conclusion of the test run to guarantee
    0 persistent changes during staging rehearsal.
    """
    outcomes: list[CanaryOutcome] = []
    chunk_ids = [cfg["chunk_id"] for cfg in canary_configs.values() if "chunk_id" in cfg]
    preimage_manifest = capture_pre_rehearsal_state(session, chunk_ids)

    try:
        # Archetype 1: SAFE_REKEY
        c1 = canary_configs[CanaryArchetype.SAFE_REKEY]
        o1 = execute_archetype_safe_rekey(
            session,
            c1["chunk_id"],
            c1["target_natural_key"],
            c1["expected_content_hash"],
        )
        outcomes.append(o1)

        # Archetype 2: SAFE_REKEY_STATS
        c2 = canary_configs[CanaryArchetype.SAFE_REKEY_STATS]
        o2 = execute_archetype_safe_rekey_stats(
            session,
            c2["chunk_id"],
            c2["target_natural_key"],
            c2["expected_content_hash"],
        )
        outcomes.append(o2)

        # Archetype 3: TARGET_COLLISION_TOMBSTONE
        c3 = canary_configs[CanaryArchetype.TARGET_COLLISION_TOMBSTONE]
        o3 = execute_archetype_collision_tombstone(
            session,
            c3["legacy_chunk_id"],
            c3["target_natural_key"],
        )
        outcomes.append(o3)

        # Archetype 4: ALREADY_APPLIED_NOOP
        c4 = canary_configs[CanaryArchetype.ALREADY_APPLIED_NOOP]
        o4 = execute_archetype_already_applied_noop(
            session,
            c4["chunk_id"],
            c4["natural_key"],
        )
        outcomes.append(o4)

        # Archetype 5: STALE_CAS_REJECT
        c5 = canary_configs[CanaryArchetype.STALE_CAS_REJECT]
        o5 = execute_archetype_stale_cas_reject(
            session,
            c5["chunk_id"],
            c5["manifest_expected_hash"],
            c5["attempted_new_key"],
        )
        outcomes.append(o5)

        # Archetype 6: INVERSE_ROLLBACK_REPLAY
        c6 = canary_configs[CanaryArchetype.INVERSE_ROLLBACK_REPLAY]
        o6 = execute_archetype_inverse_rollback_replay(
            session,
            c6["chunk_id"],
            c6["preimage_row_id"],
            c6["preimage_status"],
        )
        outcomes.append(o6)

    except (SQLAlchemyError, RuntimeError, ValueError, OSError) as e:
        logger.exception("Unexpected error during canary execution")
        session.rollback()
        return CanaryResult(
            passed=False,
            outcomes=outcomes,
            rollback_performed=True,
            error=str(e),
        )
    finally:
        # Priority 1: Session Rollback guarantee
        session.rollback()
        rollback_clean = verify_rollback(session, preimage_manifest)
        if not rollback_clean:
            logger.error("Priority 1 rollback verification failed!")
        else:
            logger.info("Priority 1: Session rollback verified cleanly (0 persistent staging mutations).")

    all_passed = len(outcomes) == CANARY_ARCHETYPE_COUNT and all(o.status == "PASS" for o in outcomes)
    return CanaryResult(
        passed=all_passed,
        outcomes=outcomes,
        rollback_performed=True,
        error=None,
    )
