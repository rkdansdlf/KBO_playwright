"""Static safety checks for the Oracle sparse term migration."""

from pathlib import Path


MIGRATION = Path("migrations/oracle/068_create_rag_chunk_terms.sql")


def test_oracle_terms_migration_creates_postings_table() -> None:
    """Create term counts without changing canonical chunk data."""
    sql = MIGRATION.read_text(encoding="utf-8").upper()

    assert "CREATE TABLE RAG_CHUNK_TERMS" in sql
    assert "RAG_CHUNK_ID NUMBER(19)" in sql
    assert "TOKEN VARCHAR2(128 CHAR)" in sql
    assert "GAME_DATE VARCHAR2(10 CHAR)" in sql
    assert "FOREIGN KEY (RAG_CHUNK_ID)" in sql


def test_oracle_terms_migration_creates_lookup_indexes_idempotently() -> None:
    """Create token and date lookup indexes only when missing."""
    sql = MIGRATION.read_text(encoding="utf-8").upper()

    assert sql.count("FROM USER_INDEXES") == 2
    assert "IDX_RAG_CHUNK_TERMS_TOKEN_CHUNK" in sql
    assert "IDX_RAG_CHUNK_TERMS_GAME_DATE" in sql
    assert sql.count("IF V_EXISTS = 0") == 3


def test_oracle_terms_scope_migration_adds_source_lookup_columns() -> None:
    """Add denormalized source scope for selective token lookup."""
    sql = Path("migrations/oracle/069_add_rag_chunk_term_source_scope.sql").read_text(encoding="utf-8").upper()

    assert "SOURCE_TABLE VARCHAR2(100 CHAR)" in sql
    assert "IDX_RAG_CHUNK_TERMS_SOURCE_TOKEN" in sql
    assert "IDX_RAG_CHUNK_TERMS_SOURCE_DATE" in sql
    assert "TRUNCATE TABLE RAG_CHUNK_TERMS" in sql
