"""Tests for backfilling the identities the Oracle copy could not supply.

The two things that would go wrong quietly are pinned here. A provider outage
answers with 1536 zeros rather than raising, so without the guard a failed
backfill records as success; and a row embedded with anything other than
``content`` lands in a different neighbourhood from the 222,987 rows the copy
brought over, which no count would reveal.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest
from src.constants import RAG_EMBEDDING_DIMENSION
from src.services.rag_vector_backfill import (
    _SOURCE_SQL,
    BackfillReport,
    ZeroVectorError,
    _embed_batch,
    _fingerprint,
    iter_gap_chunks,
    load_target_keys,
    require_usable_embedding,
)
from src.services.rag_vector_migration import chunk_from_row


def _row(**overrides: Any) -> dict[str, Any]:
    """Return a sparse-store row shaped like the backfill projection."""
    row: dict[str, Any] = {
        "id": 7,
        "season_year": 2026,
        "season_id": None,
        "league_type_code": 0,
        "team_id": "HT",
        "player_id": None,
        "source_table": "game",
        "source_row_id": "20260823LTOB0",
        "title": "경기 결과",
        "content": "한밭에서 HT가 승리했다.",
        "content_hash": "old-hash",
        "index_version": "rag-v1",
        "index_status": "ACTIVE",
        "indexed_at": datetime(2026, 9, 24, 22, 15),
        "created_at": datetime(2026, 8, 23, 21, 0),
        "updated_at": datetime(2026, 9, 24, 22, 15),
        "meta": '{"document_type": "game_result", "game_date": "2026-08-23"}',
        "embedding_vector": None,
    }
    row.update(overrides)
    return row


class _StubResult:
    """Answer both shapes the backfill asks a result for.

    ``load_target_keys`` calls ``.all()`` and ``iter_gap_chunks`` calls
    ``.mappings()``, so a stub that only offers one of them hides which call
    site a change landed in.
    """

    def __init__(self, keys: list[tuple[str, str]], chunks: list[dict[str, Any]]) -> None:
        """Store the rows this result will hand out per accessor."""
        self._keys = keys
        self._chunks = chunks

    def all(self) -> list[tuple[str, str]]:
        """Return identity rows."""
        return self._keys

    def mappings(self) -> Any:
        """Return projected chunk rows as mappings."""
        return iter(self._chunks)


class _StubConnection:
    """Return canned rows for the two statements the backfill runs."""

    def __init__(self, keys: list[tuple[str, str]] | None = None, chunks: list[dict[str, Any]] | None = None) -> None:
        """Store the identity rows and the projected chunk rows."""
        self._keys = keys or []
        self._chunks = chunks or []

    def __enter__(self) -> _StubConnection:
        """Enter the context."""
        return self

    def __exit__(self, *_exc: object) -> None:
        """Leave the context."""

    def execute(self, statement: object) -> _StubResult:
        """Answer the identity query or the chunk query, whichever was issued."""
        if statement is _SOURCE_SQL:
            return _StubResult([], self._chunks)
        return _StubResult(self._keys, [])


class _StubEngine:
    """Return a fixed connection for every ``connect()``."""

    def __init__(self, connection: _StubConnection) -> None:
        """Store the connection to hand out."""
        self._connection = connection

    def connect(self) -> _StubConnection:
        """Return the canned connection."""
        return self._connection


class _StubEmbeddingService:
    """Return canned vectors and record the texts it was asked to embed."""

    def __init__(self, vectors: list[list[float]]) -> None:
        """Store the vectors to return."""
        self._vectors = vectors
        self.requested: list[list[str]] = []

    def get_embeddings_batch(self, texts: list[str]) -> list[list[float]]:
        """Record the request and return the canned vectors."""
        self.requested.append(list(texts))
        return self._vectors

    def model_name(self) -> str:
        """Return the model the fingerprint should record."""
        return "perplexity/pplx-embed-v1-4b"


class TestGapDetection:
    """Pin that only rows the vector store is missing are considered."""

    def test_loads_identity_keys_from_the_vector_store(self) -> None:
        """Read existing identities as colon-joined keys."""
        connection = _StubConnection([("game", "1"), ("player_basic", "2")])
        assert load_target_keys(_StubEngine(connection)) == {"game:1", "player_basic:2"}

    def test_yields_only_identities_the_store_lacks(self) -> None:
        """Skip rows already present and return the rest."""
        connection = _StubConnection(chunks=[_row(), _row(source_row_id="20260823LTOB1")])
        chunks = list(iter_gap_chunks(_StubEngine(connection), {"game:20260823LTOB0"}))
        assert [chunk.key for chunk in chunks] == ["game:20260823LTOB1"]


class TestProviderGuard:
    """Pin that an unusable provider answer refuses the batch."""

    def test_accepts_real_vectors(self) -> None:
        """Allow a full-length non-zero batch."""
        require_usable_embedding([[0.5] * RAG_EMBEDDING_DIMENSION], 1)

    def test_rejects_a_count_mismatch(self) -> None:
        """Refuse when the provider dropped a row."""
        with pytest.raises(ZeroVectorError, match="returned 0 vectors for 1 chunks"):
            require_usable_embedding([], 1)

    def test_rejects_a_zero_vector(self) -> None:
        """Refuse the silent fallback the provider returns on failure."""
        with pytest.raises(ZeroVectorError, match="zero vector"):
            require_usable_embedding([[0.0] * RAG_EMBEDDING_DIMENSION], 1)

    def test_rejects_a_wrong_dimension(self) -> None:
        """Refuse a vector the target column could not store."""
        with pytest.raises(ZeroVectorError, match="expected 1536"):
            require_usable_embedding([[0.5] * 8], 1)


class TestEmbeddingContract:
    """Pin that a backfilled row matches how the builder wrote the rest."""

    def test_embeds_content_only(self) -> None:
        """Send ``content`` alone, as ``build_rag_index`` does."""
        service = _StubEmbeddingService([[0.5] * RAG_EMBEDDING_DIMENSION])
        _embed_batch([chunk_from_row(_row())], service, "fp")
        assert service.requested == [["한밭에서 HT가 승리했다."]]

    def test_records_the_builder_fingerprint_and_fresh_hash(self) -> None:
        """Persist the fingerprint, recompute the hash, and stamp the version."""
        service = _StubEmbeddingService([[0.5] * RAG_EMBEDDING_DIMENSION])
        embedded = _embed_batch([chunk_from_row(_row())], service, "fp:1536:rag-v1")
        chunk = embedded[0]
        assert chunk.meta["embedding_fingerprint"] == "fp:1536:rag-v1"
        assert chunk.content_hash != "old-hash"
        assert len(chunk.content_hash) == 64
        assert chunk.index_version == "rag-v1"
        assert len(chunk.embedding) == RAG_EMBEDDING_DIMENSION

    def test_keeps_the_promoted_meta_fields(self) -> None:
        """Not drop document_type and game_date on the way through."""
        service = _StubEmbeddingService([[0.5] * RAG_EMBEDDING_DIMENSION])
        embedded = _embed_batch([chunk_from_row(_row())], service, "fp")
        assert embedded[0].document_type == "game_result"
        assert embedded[0].game_date is not None

    def test_builds_the_fingerprint_from_the_service(self) -> None:
        """Read the model from the service when it exposes one."""
        service = _StubEmbeddingService([])
        assert _fingerprint(service) == f"perplexity/pplx-embed-v1-4b:{RAG_EMBEDDING_DIMENSION}:rag-v1"


class TestBackfillReport:
    """Pin the count a run reports."""

    def test_summary_names_what_happened(self) -> None:
        """Render the counters a CLI line needs."""
        report = BackfillReport(source_rows=10, gap_rows=2, embedded=2, copied=2, batches=1)
        assert "gap=2" in report.summary
        assert "copied=2" in report.summary
