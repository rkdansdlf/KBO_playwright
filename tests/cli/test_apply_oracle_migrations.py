from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.cli.apply_oracle_migrations import (
    _is_already_exists_error,
    _execute_migration,
    _migration_paths,
    main,
)


def test_migration_paths_are_sorted_by_numeric_prefix(tmp_path) -> None:
    (tmp_path / "010_later.sql").write_text("SELECT 1", encoding="utf-8")
    (tmp_path / "002_earlier.sql").write_text("SELECT 1", encoding="utf-8")
    (tmp_path / "README.md").write_text("ignored", encoding="utf-8")

    assert [path.name for path in _migration_paths(tmp_path)] == ["002_earlier.sql", "010_later.sql"]


def test_migration_paths_use_filename_as_stable_tiebreaker(tmp_path) -> None:
    """Test deterministic ordering when multiple migrations share a prefix."""
    (tmp_path / "020_zeta.sql").write_text("SELECT 1", encoding="utf-8")
    (tmp_path / "020_alpha.sql").write_text("SELECT 1", encoding="utf-8")

    assert [path.name for path in _migration_paths(tmp_path)] == ["020_alpha.sql", "020_zeta.sql"]


def test_migration_paths_exclude_safety_gated_files_by_default(tmp_path) -> None:
    """Require explicit opt-in before data-rewrite migrations are included."""
    (tmp_path / "024_deletion_anomaly_integrity.sql").write_text("SELECT 1", encoding="utf-8")
    (tmp_path / "065_reconcile_model_indexes.sql").write_text("SELECT 1", encoding="utf-8")

    assert [path.name for path in _migration_paths(tmp_path)] == ["065_reconcile_model_indexes.sql"]
    assert [path.name for path in _migration_paths(tmp_path, include_safety_gated=True)] == [
        "024_deletion_anomaly_integrity.sql",
        "065_reconcile_model_indexes.sql",
    ]


def test_execute_migration_preserves_plsql_block_terminator(tmp_path) -> None:
    """Keep the final semicolon required by Oracle anonymous PL/SQL blocks."""
    migration = tmp_path / "065_plsql.sql"
    migration.write_text("-- comment\nDECLARE\nBEGIN\n    NULL;\nEND;\n/\n", encoding="utf-8")
    connection = MagicMock()

    _execute_migration(connection, migration)

    statement = connection.exec_driver_sql.call_args.args[0]
    assert statement.endswith("END;")


def test_already_exists_error_recognizes_oracle_object_exists_code() -> None:
    error = SQLAlchemyError("object exists")
    error.orig = SimpleNamespace(code=955)  # type: ignore[attr-defined]

    assert _is_already_exists_error(error) is True


def test_main_requires_oracle_url() -> None:
    with pytest.raises(SystemExit):
        main(["--url", "sqlite:///./data/kbo_dev.db"])


def test_main_bootstraps_and_checks_oracle_schema() -> None:
    fake_engine = MagicMock()
    with (
        patch("src.cli.apply_oracle_migrations.create_engine_for_url", return_value=fake_engine),
        patch("src.cli.apply_oracle_migrations._bootstrap_orm_schema") as bootstrap,
        patch("src.cli.apply_oracle_migrations.apply_migrations", return_value=[]),
    ):
        assert main(["--url", "oracle+oracledb://user:pass@db/service"]) == 0

    bootstrap.assert_called_once_with(fake_engine)
    fake_engine.dispose.assert_called_once()
