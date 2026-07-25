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
