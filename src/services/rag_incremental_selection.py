"""Select RAG chunks that require an embedding refresh."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final

from src.constants import RAG_EMBEDDING_DIMENSION
from src.services.rag_index_identity import chunk_content_hash

TERMINAL_INDEX_STATUSES: Final[frozenset[str]] = frozenset({"DELETED", "TOMBSTONED", "PURGED", "DELETE_PENDING"})
REPAIRABLE_INDEX_STATUSES: Final[frozenset[str]] = frozenset({"PENDING", "STALE", "REINDEX_REQUIRED"})


@dataclass(frozen=True, slots=True)
class RagIndexState:
    """Store the non-vector fields needed for an incremental decision."""

    source_table: str
    source_row_id: str
    content_hash: str | None = None
    index_version: str | None = None
    index_status: str = "ACTIVE"
    embedding_present: bool = False
    embedding_fingerprint: str | None = None

    @property
    def key(self) -> tuple[str, str]:
        """Return the canonical source identity tuple."""
        return self.source_table, self.source_row_id


@dataclass(frozen=True, slots=True)
class IncrementalDecisionOptions:
    """Describe the target contract for one candidate decision."""

    desired_index_version: str
    desired_embedding_fingerprint: str | None = None
    require_vector: bool = False
    require_primary_embedding: bool = True


@dataclass(frozen=True, slots=True)
class RagCandidateDecision:
    """Describe whether one generated chunk should be embedded or skipped."""

    source_key: str
    should_reembed: bool
    reason: str
    blocked: bool = False
    metadata_repair: bool = False

    @property
    def action(self) -> str:
        """Return the operational action represented by the decision."""
        if self.blocked:
            return "BLOCK"
        if self.should_reembed:
            return "REEMBED"
        if self.metadata_repair:
            return "METADATA_GAP"
        return "SKIP"


def _text(value: object) -> str:
    """Normalize a dynamic value to stripped text."""
    return str(value).strip() if value is not None else ""


def _optional_text(value: object) -> str | None:
    """Normalize a nullable dynamic value to text."""
    text = _text(value)
    return text or None


def index_state_from_projection(row: Sequence[object]) -> RagIndexState:
    """Build a normalized index state from a database projection."""
    return RagIndexState(
        source_table=_text(row[0]),
        source_row_id=_text(row[1]),
        content_hash=_optional_text(row[2]),
        index_version=_optional_text(row[3]),
        index_status=(_text(row[4]) or "ACTIVE").upper(),
        embedding_present=bool(row[5]),
        embedding_fingerprint=embedding_fingerprint_from_metadata(row[6]),
    )


def embedding_fingerprint(
    model: object,
    dimension: object = RAG_EMBEDDING_DIMENSION,
    chunking_version: object = "rag-v1",
) -> str:
    """Build the persisted model, dimension, and chunking fingerprint."""
    return f"{_text(model)}:{_text(dimension)}:{_text(chunking_version)}"


def embedding_fingerprint_from_metadata(metadata: object) -> str | None:
    """Read a fingerprint from explicit or legacy RAG metadata."""
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            return None
    if not isinstance(metadata, Mapping):
        return None
    explicit = _optional_text(metadata.get("embedding_fingerprint"))
    if explicit:
        return explicit
    model = _optional_text(metadata.get("embedding_model"))
    dimension = _optional_text(metadata.get("embedding_dim"))
    chunking_version = _optional_text(metadata.get("chunking_version"))
    if model and dimension and chunking_version:
        return embedding_fingerprint(model, dimension, chunking_version)
    return None


def _blocked(source_key: str, reason: str) -> RagCandidateDecision:
    """Build a blocked candidate decision."""
    return RagCandidateDecision(source_key, should_reembed=False, reason=reason, blocked=True)


def _reembed(source_key: str, reason: str, *, metadata_repair: bool = False) -> RagCandidateDecision:
    """Build a re-embedding candidate decision."""
    return RagCandidateDecision(
        source_key,
        should_reembed=True,
        reason=reason,
        metadata_repair=metadata_repair,
    )


def _is_terminal(state: RagIndexState) -> bool:
    """Return whether an index row must not be reactivated automatically."""
    return state.index_status in TERMINAL_INDEX_STATUSES


def _needs_status_repair(state: RagIndexState) -> bool:
    """Return whether a non-terminal lifecycle state requires rebuilding."""
    return state.index_status in REPAIRABLE_INDEX_STATUSES


def _existing_candidate_reason(
    state: RagIndexState,
    *,
    desired_hash: str,
    desired_index_version: str,
    require_primary_embedding: bool,
) -> str | None:
    """Return the first canonical-state reason requiring re-embedding."""
    if _needs_status_repair(state):
        return "status_repair"
    if not state.content_hash:
        return "content_hash_missing"
    if state.content_hash != desired_hash:
        return "content_changed"
    if state.index_version != desired_index_version:
        return "index_version_stale"
    if require_primary_embedding and not state.embedding_present:
        return "embedding_missing"
    return None


def _vector_candidate_reason(
    state: RagIndexState | None,
    *,
    desired_hash: str,
    desired_index_version: str,
) -> str | None:
    """Return the first vector-state reason requiring re-embedding."""
    if state is None:
        reason = "vector_missing"
    elif _needs_status_repair(state):
        reason = "vector_status_repair"
    elif not state.content_hash:
        reason = "vector_content_hash_missing"
    elif state.content_hash != desired_hash:
        reason = "vector_content_hash_mismatch"
    elif state.index_version != desired_index_version:
        reason = "vector_index_version_stale"
    elif not state.embedding_present:
        reason = "vector_embedding_missing"
    else:
        reason = None
    return reason


def _fingerprint_candidate(
    existing: RagIndexState,
    vector: RagIndexState | None,
    desired_fingerprint: str,
    *,
    require_vector: bool,
) -> tuple[str | None, bool]:
    """Return a fingerprint reason and whether legacy metadata needs repair."""
    metadata_repair = False
    reason: str | None = None
    if existing.embedding_fingerprint is None:
        metadata_repair = True
    elif existing.embedding_fingerprint != desired_fingerprint:
        reason = "embedding_fingerprint_stale"
    if require_vector and vector is not None:
        if vector.embedding_fingerprint is None:
            metadata_repair = True
        elif vector.embedding_fingerprint != desired_fingerprint:
            reason = reason or "vector_embedding_fingerprint_stale"
    return reason, metadata_repair


def decide_incremental_candidate(
    chunk: Mapping[str, object],
    existing: RagIndexState | None,
    vector: RagIndexState | None,
    options: IncrementalDecisionOptions,
) -> RagCandidateDecision:
    """Decide whether a generated chunk needs a fresh embedding."""
    source_table = _text(chunk.get("source_table"))
    source_row_id = _text(chunk.get("source_row_id"))
    if not source_table or not source_row_id:
        return _blocked("<invalid>", "invalid_identity")

    source_key = f"{source_table}:{source_row_id}"
    if vector is not None and _is_terminal(vector):
        return _blocked(source_key, "vector_terminal_status")
    if existing is not None and _is_terminal(existing):
        return _blocked(source_key, "terminal_status")
    if existing is None:
        return _reembed(source_key, "primary_missing" if vector is not None else "new")

    desired_hash = chunk_content_hash(
        _optional_text(chunk.get("title")),
        _text(chunk.get("content")),
    )
    reason = _existing_candidate_reason(
        existing,
        desired_hash=desired_hash,
        desired_index_version=options.desired_index_version,
        require_primary_embedding=options.require_primary_embedding,
    )
    if reason is None and options.require_vector:
        reason = _vector_candidate_reason(
            vector,
            desired_hash=desired_hash,
            desired_index_version=options.desired_index_version,
        )

    metadata_repair = False
    if reason is None and options.desired_embedding_fingerprint:
        reason, metadata_repair = _fingerprint_candidate(
            existing,
            vector,
            options.desired_embedding_fingerprint,
            require_vector=options.require_vector,
        )

    if reason is not None:
        return _reembed(source_key, reason, metadata_repair=metadata_repair)
    return RagCandidateDecision(
        source_key,
        should_reembed=False,
        reason="healthy",
        metadata_repair=metadata_repair,
    )
