from pathlib import Path
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from scripts.maintenance.apply_oci_migrations import (
    _ensure_metadata_table,
    _file_checksum,
    _execute_migration_statements,
    _migration_files,
    _split_migration_statements,
    refresh_checksums,
)


def test_oracle_migration_statements_split_on_slash_delimiters() -> None:
    sql = """
    DECLARE
        v_count NUMBER;
    BEGIN
        NULL;
    END;
    /
    CREATE INDEX IDX_SAMPLE ON SAMPLE_TABLE (ID);
    """

    statements = _split_migration_statements(sql, "oracle")

    assert len(statements) == 2
    assert statements[0].startswith("DECLARE")
    assert statements[1].startswith("CREATE INDEX")


def test_postgres_migration_keeps_complete_file_as_one_statement() -> None:
    sql = "CREATE TABLE sample (id INTEGER);\nCREATE INDEX sample_id ON sample (id);"

    assert _split_migration_statements(sql, "postgres") == [sql]


def test_migration_files_use_oracle_bundle() -> None:
    files = _migration_files("oracle")

    assert Path("migrations/oracle/022_add_source_to_advanced_stats.sql") in files
    assert all(path.parent == Path("migrations/oracle") for path in files)


def test_oracle_schema_gap_bootstrap_precedes_source_migrations() -> None:
    files = _migration_files("oracle")

    assert files[0].name == "000_oracle_team_daily_roster_bootstrap.sql"
    assert files.index(Path("migrations/oracle/000_oracle_team_daily_roster_bootstrap.sql")) < files.index(
        Path("migrations/oracle/024_game_stat_partial_unique_indexes.sql")
    )


def test_migration_files_can_select_an_ordered_batch() -> None:
    files = _migration_files(
        "oracle",
        only={
            "000_oracle_team_daily_roster_bootstrap.sql",
            "022_add_source_to_advanced_stats.sql",
        },
    )

    assert [path.name for path in files] == [
        "000_oracle_team_daily_roster_bootstrap.sql",
        "022_add_source_to_advanced_stats.sql",
    ]


def test_migration_files_reject_unknown_selected_file() -> None:
    with pytest.raises(ValueError, match=r"unknown\.sql"):
        _migration_files("oracle", only={"unknown.sql"})


def test_refresh_checksums_requires_explicit_selection(monkeypatch) -> None:
    monkeypatch.setenv("OCI_DB_URL", "oracle+oracledb://placeholder")

    with pytest.raises(ValueError, match="requires at least one"):
        refresh_checksums("oracle")


def test_oracle_roster_bootstrap_matches_model_schema_contract() -> None:
    sql = Path("migrations/oracle/000_oracle_team_daily_roster_bootstrap.sql").read_text()

    assert "CREATE TABLE TEAM_DAILY_ROSTER" in sql
    for column in (
        "ROSTER_DATE DATE NOT NULL",
        "TEAM_CODE VARCHAR2(10) NOT NULL",
        "PLAYER_ID NUMBER(10) NOT NULL",
        "PLAYER_BASIC_ID NUMBER(10)",
        "PERSON_TYPE VARCHAR2(16) DEFAULT 'player' NOT NULL",
        "PLAYER_NAME VARCHAR2(50) NOT NULL",
        "POSITION VARCHAR2(20)",
        "BACK_NUMBER VARCHAR2(10)",
    ):
        assert column in sql
    assert "REFERENCES TEAMS (TEAM_ID)" in sql
    assert "REFERENCES PLAYER_BASIC (PLAYER_ID)" in sql


def test_oracle_metadata_table_uses_oracle_ddl() -> None:
    session = MagicMock()

    _ensure_metadata_table(session, "oracle")

    statement = "\n".join(str(call.args[0]) for call in session.execute.call_args_list)
    assert "KBO_SCHEMA_MIGRATIONS" in statement
    assert "VARCHAR2(255)" in statement
    assert "CHECKSUM VARCHAR2(64)" in statement


def test_file_checksum_is_sha256() -> None:
    path = Path("migrations/oracle/030_create_hnsw_vector_index.sql")

    checksum = _file_checksum(path)

    assert len(checksum) == 64
    assert checksum == _file_checksum(path)


def test_failed_migration_statement_reports_position_and_rolls_back() -> None:
    session = MagicMock()
    session.execute.side_effect = [None, SQLAlchemyError("invalid DDL")]

    with pytest.raises(RuntimeError, match=r"sample\.sql statement 2/2 failed"):
        _execute_migration_statements(session, "sample.sql", ["BEGIN", "BAD DDL"])

    session.rollback.assert_called_once_with()
