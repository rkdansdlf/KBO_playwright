"""Static safety checks for the rag_chunks team+season composite index."""

from pathlib import Path


def test_team_season_index_migration_is_idempotent() -> None:
    """Create the composite lookup index only when missing."""
    sql = Path("migrations/oracle/071_create_rag_chunks_team_season_index.sql").read_text(encoding="utf-8").upper()

    assert sql.count("FROM USER_INDEXES") == 1
    assert sql.count("IF V_EXISTS = 0") == 1
    assert "CREATE INDEX IDX_RAG_CHUNKS_TEAM_SEASON ON RAG_CHUNKS (TEAM_ID, SEASON_YEAR)" in sql
