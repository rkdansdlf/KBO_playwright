from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.cli.apply_oracle_migrations import (
    _is_already_exists_error,
    _migration_paths,
    main,
)


def test_migration_paths_are_sorted_by_numeric_prefix(tmp_path) -> None:
    (tmp_path / "010_later.sql").write_text("SELECT 1", encoding="utf-8")
    (tmp_path / "002_earlier.sql").write_text("SELECT 1", encoding="utf-8")
    (tmp_path / "README.md").write_text("ignored", encoding="utf-8")

    assert [path.name for path in _migration_paths(tmp_path)] == ["002_earlier.sql", "010_later.sql"]


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
