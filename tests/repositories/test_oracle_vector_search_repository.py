"""Tests for Oracle AI Vector Search repository behavior."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.dialects import oracle

from src.models.rag_chunk import OracleVectorType, RagChunk
from src.repositories.oracle_vector_search_repository import OracleVectorSearchRepository


def test_rag_chunk_uses_native_oracle_vector_column() -> None:
    """Compile the canonical embedding column as Oracle VECTOR."""
    column_type = RagChunk.__table__.c.embedding_vector.type.compile(dialect=oracle.dialect())

    assert column_type == "VECTOR(1536,FLOAT32,DENSE)"


def test_distance_expression_uses_oracle_vector_distance() -> None:
    """Use Oracle's VECTOR_DISTANCE function rather than pgvector operators."""
    expression = OracleVectorSearchRepository._distance_expression([0.1] * 1536)
    statement = select(RagChunk).order_by(expression)

    rendered = str(statement.compile(dialect=oracle.dialect()))

    assert "vector_distance" in rendered.lower()
    assert "cosine" in rendered.lower()
    assert "<=>" not in rendered


def test_oracle_vector_type_converts_lists_to_float32_arrays() -> None:
    """Convert ORM and query bind values into the representation oracledb accepts."""
    vector_type = OracleVectorType()

    bound = vector_type.process_bind_param([0.1, 0.2], oracle.dialect())

    assert bound.typecode == "f"
    assert list(bound) == pytest.approx([0.1, 0.2])


def test_render_rows_maps_metadata_to_dense_result() -> None:
    """Map Oracle's single-table metadata shape to the shared search payload."""
    chunk = SimpleNamespace(
        id=1,
        source_table="player_basic",
        source_row_id="78224",
        title="김도영",
        content="KIA 선수",
        team_id=None,
        player_id=None,
        season_year=None,
        content_hash="hash",
        index_version="rag-v1",
        index_status="ACTIVE",
        indexed_at=None,
        meta={
            "team_id": "KIA",
            "player_id": "78224",
            "document_type": "player_profile",
            "language": "ko",
        },
    )

    result = OracleVectorSearchRepository._render_rows(
        [(chunk, 0.1)],
        document_type="player_profile",
        game_date=None,
        top_k=5,
    )

    assert result[0]["chunk_id"] == "player_basic:78224"
    assert result[0]["team_id"] == "KIA"
    assert result[0]["score"] == 0.9


def test_upsert_delegates_to_canonical_rag_repository() -> None:
    """Persist Oracle vectors through the same repository as sparse chunks."""
    session = MagicMock()
    payload = {"source_table": "game", "source_row_id": "g1", "content": "경기", "embedding": [0.1]}

    with patch("src.repositories.oracle_vector_search_repository.RagChunkRepository") as repository_cls:
        OracleVectorSearchRepository().upsert_chunk(session, payload)

    repository_cls.assert_called_once_with(session)
    repository_cls.return_value.upsert_chunks.assert_called_once_with(session, [payload])
