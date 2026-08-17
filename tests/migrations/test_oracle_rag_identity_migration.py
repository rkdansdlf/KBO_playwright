from pathlib import Path


MIGRATION = Path("migrations/oracle/066_restore_rag_chunk_source_identity_unique.sql")


def test_oracle_rag_identity_migration_restores_model_unique_constraint() -> None:
    """Keep the RAG source identity unique constraint aligned with the model."""
    sql = MIGRATION.read_text(encoding="utf-8").upper()

    assert "FROM USER_CONSTRAINTS" in sql
    assert "UQ_RAG_CHUNKS_SOURCE_IDENTITY" in sql
    assert "UNIQUE (SOURCE_TABLE, SOURCE_ROW_ID)" in sql
    assert "SQLCODE NOT IN (-2261, -1408)" in sql
