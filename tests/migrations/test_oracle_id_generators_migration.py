from pathlib import Path


MIGRATION = Path("migrations/oracle/060_oracle_id_generators.sql")


def test_oracle_id_generator_migration_covers_baseline_insert_tables() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")

    normalized = sql.upper()

    assert "USER_TAB_IDENTITY_COLS" in normalized
    assert "CREATE SEQUENCE" in normalized
    assert "CREATE OR REPLACE TRIGGER" in normalized
    assert "RAW_SOURCE_SNAPSHOTS" in normalized
    assert "RAG_CHUNKS" in normalized
    assert "WHEN (NEW.ID IS NULL)" in normalized


def test_oracle_id_generator_migration_preserves_explicit_source_ids() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")

    normalized = sql.upper()

    assert "ON NULL" not in normalized
    assert "WHEN (NEW.ID IS NULL)" in normalized


def test_rag_lookup_key_migration_converts_oracle_clobs() -> None:
    sql = Path("migrations/oracle/061_rag_lookup_key_types.sql").read_text(encoding="utf-8").upper()

    assert "DBMS_LOB.SUBSTR" in sql
    assert "DROP COLUMN ' || P_OLD_COLUMN" in sql
    assert "RENAME COLUMN ' || P_NEW_COLUMN" in sql


def test_oracle_id_generator_alignment_migration_advances_sequences() -> None:
    sql = Path("migrations/oracle/062_align_oracle_id_generators.sql").read_text(encoding="utf-8").upper()

    assert "REGEXP_SUBSTR" in sql
    assert "MAX(ID)" in sql
    assert "ALTER SEQUENCE" in sql
    assert "NEXTVAL" in sql


def test_oracle_id_generator_retry_uses_actual_next_value() -> None:
    sql = Path("migrations/oracle/063_align_oracle_id_generators_again.sql").read_text(encoding="utf-8").upper()

    assert "REGEXP_SUBSTR(GENERATOR_ROW.TRIGGER_NAME" in sql
    assert "CURRENT_VALUE" in sql
    assert "MAX(ID)" in sql
    assert "NEXTVAL" in sql


def test_oracle_id_generator_final_migration_fetches_advanced_value() -> None:
    sql = Path("migrations/oracle/064_align_oracle_id_generators_final.sql").read_text(encoding="utf-8").upper()

    assert "INTO V_CURRENT_VALUE" in sql
    assert "INTO V_ADVANCED_VALUE" in sql
