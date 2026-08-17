from pathlib import Path


MIGRATION = Path("migrations/oracle/065_reconcile_model_indexes.sql")


def test_oracle_schema_index_migration_is_idempotent_by_name() -> None:
    """Guard the migration's object-existence and duplicate-index handling."""
    sql = MIGRATION.read_text(encoding="utf-8").upper()

    assert "FROM USER_INDEXES" in sql
    assert "IF V_EXISTS = 0" in sql
    assert "SQLCODE != -1408" in sql


def test_oracle_schema_index_migration_restores_the_three_model_indexes() -> None:
    """Keep the reconciled index names and columns aligned with the models."""
    sql = MIGRATION.read_text(encoding="utf-8").upper()

    assert "IX_KBO_PRESS_RELEASES_PUBLISHED_DATE" in sql
    assert "ON KBO_PRESS_RELEASES (PUBLISHED_DATE)" in sql
    assert "IDX_SC_STADIUM_MEASURED" in sql
    assert "ON STADIUM_CONGESTION (STADIUM_CODE, MEASURED_AT)" in sql
    assert "IDX_SC_STADIUM_GAME_DATE" in sql
    assert "ON STADIUM_CONGESTION (STADIUM_CODE, GAME_DATE)" in sql
