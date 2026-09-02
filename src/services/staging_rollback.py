"""Phase 105 Gate 4: Multi-Tier Rollback & Preimage State Engine.

Implements the multi-tier rollback priority:
- Priority 1: In-Transaction session.rollback()
- Priority 2: Deterministic Inverse Preimage Manifest Application & Verification
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select, update

from src.models.rag_chunk import RagChunk

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


@dataclass
class PreimageEntry:
    """Snapshot of a single chunk's identity and CAS fields prior to rehearsal."""

    chunk_id: int
    source_table: str
    source_row_id: str
    index_status: str
    content_hash: str | None
    index_version: str | None

    def to_dict(self) -> dict[str, Any]:
        """Convert entry to dict."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PreimageEntry:
        """Construct entry from dict."""
        return cls(
            chunk_id=data["chunk_id"],
            source_table=data["source_table"],
            source_row_id=data["source_row_id"],
            index_status=data["index_status"],
            content_hash=data.get("content_hash"),
            index_version=data.get("index_version"),
        )


@dataclass
class PreimageManifest:
    """Collection of preimage entries capturing the complete pre-rehearsal state."""

    timestamp: str
    chunk_count: int
    entries: list[PreimageEntry]

    def to_json(self) -> str:
        """Serialize manifest to JSON string."""
        return json.dumps(
            {
                "timestamp": self.timestamp,
                "chunk_count": self.chunk_count,
                "entries": [e.to_dict() for e in self.entries],
            },
            indent=2,
            ensure_ascii=False,
        )

    @classmethod
    def from_json(cls, json_str: str) -> PreimageManifest:
        """Deserialize manifest from JSON string."""
        data = json.loads(json_str)
        return cls(
            timestamp=data["timestamp"],
            chunk_count=data["chunk_count"],
            entries=[PreimageEntry.from_dict(e) for e in data["entries"]],
        )


def capture_pre_rehearsal_state(session: Session, chunk_ids: list[int]) -> PreimageManifest:
    """Capture preimage snapshot of target chunks before any rehearsal operations."""
    if not chunk_ids:
        return PreimageManifest(
            timestamp=datetime.now(UTC).isoformat(),
            chunk_count=0,
            entries=[],
        )

    stmt = (
        select(
            RagChunk.id,
            RagChunk.source_table,
            RagChunk.source_row_id,
            RagChunk.index_status,
            RagChunk.content_hash,
            RagChunk.index_version,
        )
        .where(RagChunk.id.in_(chunk_ids))
        .order_by(RagChunk.id.asc())
    )

    rows = session.execute(stmt).all()
    entries = [
        PreimageEntry(
            chunk_id=r[0],
            source_table=r[1],
            source_row_id=r[2],
            index_status=r[3],
            content_hash=r[4],
            index_version=r[5],
        )
        for r in rows
    ]

    return PreimageManifest(
        timestamp=datetime.now(UTC).isoformat(),
        chunk_count=len(entries),
        entries=entries,
    )


def apply_preimage_rollback(session: Session, preimage: PreimageManifest) -> int:
    """Restore chunk states using the preimage manifest (Priority 2 Rollback)."""
    restored_count = 0
    for entry in preimage.entries:
        stmt = (
            update(RagChunk)
            .where(RagChunk.id == entry.chunk_id)
            .values(
                source_row_id=entry.source_row_id,
                index_status=entry.index_status,
                content_hash=entry.content_hash,
                index_version=entry.index_version,
            )
        )
        result = session.execute(stmt)
        restored_count += result.rowcount

    session.flush()
    return restored_count


def verify_rollback(session: Session, preimage: PreimageManifest) -> bool:
    """Verify that current DB state matches the preimage manifest exactly."""
    if not preimage.entries:
        return True

    chunk_ids = [e.chunk_id for e in preimage.entries]
    stmt = (
        select(
            RagChunk.id,
            RagChunk.source_table,
            RagChunk.source_row_id,
            RagChunk.index_status,
            RagChunk.content_hash,
            RagChunk.index_version,
        )
        .where(RagChunk.id.in_(chunk_ids))
        .order_by(RagChunk.id.asc())
    )

    rows = session.execute(stmt).all()
    current_map = {r[0]: r for r in rows}

    for expected in preimage.entries:
        current = current_map.get(expected.chunk_id)
        if not current:
            logger.error("Rollback verification failed: chunk_id %d missing", expected.chunk_id)
            return False

        if (
            current[1] != expected.source_table
            or current[2] != expected.source_row_id
            or current[3] != expected.index_status
            or current[4] != expected.content_hash
            or current[5] != expected.index_version
        ):
            logger.error(
                "Rollback verification mismatch for chunk_id %d: expected %s, got %s",
                expected.chunk_id,
                expected,
                current,
            )
            return False

    return True
