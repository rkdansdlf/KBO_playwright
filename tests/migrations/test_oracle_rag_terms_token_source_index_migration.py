"""Static safety checks for the rag_chunk_terms token+source slice index."""

from pathlib import Path


def test_token_source_index_migration_is_idempotent() -> None:
    """Create the source-scoped postings slice index only when missing."""
    sql = (
        Path("migrations/oracle/072_create_rag_chunk_terms_token_source_index.sql").read_text(encoding="utf-8").upper()
    )

    assert sql.count("FROM USER_INDEXES") == 1
    assert sql.count("IF V_EXISTS = 0") == 1
    assert "IDX_RAG_CHUNK_TERMS_TOKEN_SOURCE" in sql
    assert "(TOKEN, SOURCE_TABLE, RAG_CHUNK_ID)" in sql
    assert "ONLINE" in sql
