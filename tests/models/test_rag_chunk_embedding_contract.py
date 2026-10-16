"""Contract tests for the canonical ``rag_chunks.embedding`` column mapping.

Both PostgreSQL stores name the dense column ``embedding`` as a pgvector ``vector(1536)``.
The ORM mapping was the only place still calling it ``embedding_vector`` as JSON, which made
every ``RagChunk`` read and write fail with ``UndefinedColumn`` on PostgreSQL.
"""

from __future__ import annotations

from sqlalchemy import create_engine, inspect, select
from sqlalchemy.dialects import postgresql, sqlite

from src.constants import RAG_EMBEDDING_DIMENSION
from src.models.rag_chunk import RagChunk


def test_sparse_model_exposes_the_canonical_column_name() -> None:
    """Map the attribute to the physical name both stores actually have."""
    table = RagChunk.__table__

    assert "embedding" in table.c
    assert "embedding_vector" not in table.c
    assert table.c.embedding.name == "embedding"


def test_postgresql_compiles_the_column_as_pgvector() -> None:
    """PostgreSQL stores vector(1536), so the mapping must not degrade to JSON."""
    compiled = RagChunk.__table__.c.embedding.type.compile(dialect=postgresql.dialect())

    assert compiled == f"VECTOR({RAG_EMBEDDING_DIMENSION})"


def test_postgresql_statements_do_not_name_a_missing_column() -> None:
    """Entity selects and the incremental identity projection must compile."""
    entity = str(select(RagChunk).compile(dialect=postgresql.dialect()))
    projection = str(
        select(
            RagChunk.source_table,
            RagChunk.embedding.is_not(None).label("embedding_present"),
        ).compile(dialect=postgresql.dialect())
    )

    assert "rag_chunks.embedding" in entity
    assert "embedding_vector" not in entity
    assert "embedding_vector" not in projection


def test_sqlite_keeps_json_storage_under_the_canonical_name() -> None:
    """Local SQLite keeps JSON storage, so only the column name is shared."""
    engine = create_engine("sqlite://")
    try:
        RagChunk.__table__.create(engine)
        columns = {column["name"] for column in inspect(engine).get_columns("rag_chunks")}
    finally:
        engine.dispose()

    assert "embedding" in columns
    assert "embedding_vector" not in columns
    assert RagChunk.__table__.c.embedding.type.compile(dialect=sqlite.dialect()) == "JSON"
