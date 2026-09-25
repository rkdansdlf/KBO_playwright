"""Tests for content-aware RAG incremental candidate selection."""

from __future__ import annotations

from src.services.rag_incremental_selection import (
    IncrementalDecisionOptions,
    RagIndexState,
    decide_incremental_candidate,
    embedding_fingerprint,
    embedding_fingerprint_from_metadata,
    index_state_from_projection,
)
from src.services.rag_index_identity import chunk_content_hash


def _chunk(*, title: str = "제목", content: str = "본문", source_row_id: str = "1") -> dict[str, str]:
    return {
        "source_table": "game",
        "source_row_id": source_row_id,
        "title": title,
        "content": content,
    }


def _state(
    *,
    content: str = "본문",
    version: str = "rag-v1",
    status: str = "ACTIVE",
    embedding_present: bool = True,
    fingerprint: str | None = None,
) -> RagIndexState:
    return RagIndexState(
        source_table="game",
        source_row_id="1",
        content_hash=chunk_content_hash("제목", content),
        index_version=version,
        index_status=status,
        embedding_present=embedding_present,
        embedding_fingerprint=fingerprint,
    )


def _options(
    *,
    version: str = "rag-v1",
    fingerprint: str | None = None,
    require_vector: bool = False,
    require_primary_embedding: bool = True,
) -> IncrementalDecisionOptions:
    return IncrementalDecisionOptions(
        desired_index_version=version,
        desired_embedding_fingerprint=fingerprint,
        require_vector=require_vector,
        require_primary_embedding=require_primary_embedding,
    )


def test_new_identity_requires_embedding() -> None:
    decision = decide_incremental_candidate(_chunk(), None, None, _options())

    assert decision.action == "REEMBED"
    assert decision.reason == "new"


def test_changed_content_is_selected_even_when_vector_exists() -> None:
    decision = decide_incremental_candidate(
        _chunk(content="변경된 본문"),
        _state(content="기존 본문"),
        None,
        _options(),
    )

    assert decision.action == "REEMBED"
    assert decision.reason == "content_changed"


def test_stale_index_version_is_selected() -> None:
    decision = decide_incremental_candidate(
        _chunk(),
        _state(version="rag-v0"),
        None,
        _options(),
    )

    assert decision.action == "REEMBED"
    assert decision.reason == "index_version_stale"


def test_missing_embedding_is_selected() -> None:
    decision = decide_incremental_candidate(
        _chunk(),
        _state(embedding_present=False),
        None,
        _options(),
    )

    assert decision.action == "REEMBED"
    assert decision.reason == "embedding_missing"


def test_missing_vector_row_is_selected_in_split_store() -> None:
    decision = decide_incremental_candidate(
        _chunk(),
        _state(),
        None,
        _options(require_vector=True),
    )

    assert decision.action == "REEMBED"
    assert decision.reason == "vector_missing"


def test_split_store_does_not_require_sparse_embedding_column() -> None:
    vector = RagIndexState(
        source_table="game",
        source_row_id="1",
        content_hash=chunk_content_hash("제목", "본문"),
        index_version="rag-v1",
        embedding_present=True,
    )
    decision = decide_incremental_candidate(
        _chunk(),
        _state(embedding_present=False),
        vector,
        _options(require_vector=True, require_primary_embedding=False),
    )

    assert decision.action == "SKIP"
    assert decision.reason == "healthy"


def test_missing_vector_embedding_is_selected_in_split_store() -> None:
    vector = RagIndexState(
        source_table="game",
        source_row_id="1",
        content_hash=chunk_content_hash("제목", "본문"),
        index_version="rag-v1",
        embedding_present=False,
    )
    decision = decide_incremental_candidate(
        _chunk(),
        _state(),
        vector,
        _options(require_vector=True),
    )

    assert decision.action == "REEMBED"
    assert decision.reason == "vector_embedding_missing"


def test_terminal_status_blocks_automatic_reactivation() -> None:
    decision = decide_incremental_candidate(
        _chunk(),
        _state(status="DELETED", embedding_present=False),
        None,
        _options(),
    )

    assert decision.action == "BLOCK"
    assert decision.reason == "terminal_status"


def test_healthy_identity_is_skipped() -> None:
    decision = decide_incremental_candidate(
        _chunk(),
        _state(),
        None,
        _options(),
    )

    assert decision.action == "SKIP"
    assert decision.reason == "healthy"


def test_fingerprint_mismatch_is_selected_without_forcing_legacy_metadata_repair() -> None:
    current = embedding_fingerprint("model-a", 1536, "chunk-v2")
    stale = embedding_fingerprint("model-b", 1536, "chunk-v1")
    decision = decide_incremental_candidate(
        _chunk(),
        _state(fingerprint=stale),
        None,
        _options(fingerprint=current),
    )

    assert decision.action == "REEMBED"
    assert decision.reason == "embedding_fingerprint_stale"


def test_legacy_missing_fingerprint_is_reported_as_metadata_repair() -> None:
    decision = decide_incremental_candidate(
        _chunk(),
        _state(),
        None,
        _options(fingerprint=embedding_fingerprint("model-a")),
    )

    assert decision.action == "METADATA_GAP"
    assert decision.metadata_repair is True


def test_legacy_metadata_fields_are_normalized_to_fingerprint() -> None:
    assert embedding_fingerprint_from_metadata(
        {"embedding_model": "model-a", "embedding_dim": 1536, "chunking_version": "chunk-v1"}
    ) == embedding_fingerprint("model-a", 1536, "chunk-v1")


def test_json_metadata_is_normalized_to_fingerprint() -> None:
    assert embedding_fingerprint_from_metadata(
        '{"embedding_model": "model-a", "embedding_dim": 1536, "chunking_version": "chunk-v1"}'
    ) == embedding_fingerprint("model-a", 1536, "chunk-v1")


def test_projection_reads_json_metadata_fingerprint() -> None:
    state = index_state_from_projection(
        [
            "game",
            "1",
            "hash",
            "rag-v1",
            "ACTIVE",
            True,
            '{"embedding_model": "model-a", "embedding_dim": 1536, "chunking_version": "chunk-v1"}',
        ]
    )

    assert state.embedding_fingerprint == embedding_fingerprint("model-a", 1536, "chunk-v1")
