"""Static safety checks for the Oracle native RAG vector migration."""

from pathlib import Path


MIGRATION = Path("migrations/oracle/067_add_rag_vector_search.sql")


def test_oracle_vector_migration_adds_native_column_without_replacing_legacy_data() -> None:
    """Keep the legacy JSON column while adding the native vector projection."""
    sql = MIGRATION.read_text(encoding="utf-8").upper()

    assert "EMBEDDING_VECTOR VECTOR(1536, FLOAT32, DENSE)" in sql
    assert "ALTER TABLE RAG_CHUNKS ADD" in sql
    assert "EMBEDDING VECTOR" not in sql


def test_oracle_vector_migration_creates_idempotent_hnsw_index() -> None:
    """Create the Oracle cosine HNSW index only when absent."""
    sql = MIGRATION.read_text(encoding="utf-8").upper()

    assert "FROM USER_INDEXES" in sql
    assert "IDX_RAG_CHUNKS_EMBEDDING_HNSW" in sql
    assert "CREATE VECTOR INDEX" in sql
    assert "TYPE HNSW" in sql
    assert "DISTANCE COSINE" in sql
    assert "SQLCODE != -955" in sql
