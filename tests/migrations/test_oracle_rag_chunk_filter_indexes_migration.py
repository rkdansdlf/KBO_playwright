"""Static safety checks for the rag_chunks filter index migration."""

from pathlib import Path


def test_filter_index_migration_creates_resolver_columns_idempotently() -> None:
    """Create team/season/player lookup indexes only when missing."""
    sql = Path("migrations/oracle/070_create_rag_chunk_filter_indexes.sql").read_text(encoding="utf-8").upper()

    assert sql.count("FROM USER_INDEXES") == 3
    assert sql.count("IF V_EXISTS = 0") == 3
    assert "CREATE INDEX IDX_RAG_CHUNKS_TEAM_ID ON RAG_CHUNKS (TEAM_ID)" in sql
    assert "CREATE INDEX IDX_RAG_CHUNKS_SEASON_YEAR ON RAG_CHUNKS (SEASON_YEAR)" in sql
    assert "CREATE INDEX IDX_RAG_CHUNKS_PLAYER_ID ON RAG_CHUNKS (PLAYER_ID)" in sql
