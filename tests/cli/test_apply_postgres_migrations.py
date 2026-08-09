from __future__ import annotations

import pytest
from sqlalchemy import create_engine, inspect, text

from src.cli.apply_postgres_migrations import apply_migrations


def _create_baseline(engine) -> None:
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE game (game_id TEXT PRIMARY KEY)"))
        connection.execute(text("CREATE TABLE kbo_seasons (season_id INTEGER PRIMARY KEY)"))


def test_postgres_migrations_are_idempotent(tmp_path):
    migration = tmp_path / "001_test.sql"
    migration.write_text(
        "CREATE TABLE IF NOT EXISTS migration_fixture (id INTEGER PRIMARY KEY);",
        encoding="utf-8",
    )
    engine = create_engine("sqlite:///:memory:")
    _create_baseline(engine)

    assert apply_migrations(engine, directory=tmp_path) == ["001_test.sql"]
    assert apply_migrations(engine, directory=tmp_path) == []
    assert apply_migrations(engine, directory=tmp_path, check=True) == []


def test_postgres_migrations_check_does_not_create_tracking_table(tmp_path):
    migration = tmp_path / "001_test.sql"
    migration.write_text("CREATE TABLE migration_fixture (id INTEGER PRIMARY KEY);", encoding="utf-8")
    engine = create_engine("sqlite:///:memory:")
    _create_baseline(engine)

    assert apply_migrations(engine, directory=tmp_path, check=True) == ["001_test.sql"]
    assert not inspect(engine).has_table("schema_migrations")


def test_postgres_migrations_require_baseline(tmp_path):
    migration = tmp_path / "001_test.sql"
    migration.write_text("CREATE TABLE migration_fixture (id INTEGER PRIMARY KEY);", encoding="utf-8")

    with pytest.raises(RuntimeError, match="ORM baseline schema"):
        apply_migrations(create_engine("sqlite:///:memory:"), directory=tmp_path)
