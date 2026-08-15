from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, inspect, text

from src.cli.apply_postgres_migrations import ADOPTABLE_MIGRATIONS, adopt_existing_schema, apply_migrations


def _create_baseline(engine) -> None:
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE game (game_id TEXT PRIMARY KEY)"))
        connection.execute(text("CREATE TABLE kbo_seasons (season_id INTEGER PRIMARY KEY)"))


def _create_adoptable_schema(engine) -> None:
    _create_baseline(engine)
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE awards (id INTEGER PRIMARY KEY, player_id INTEGER, team_code VARCHAR(20))"),
        )
        connection.execute(text("CREATE INDEX idx_award_player_id ON awards(player_id)"))
        connection.execute(
            text(
                "CREATE TABLE quarantined_records (id INTEGER PRIMARY KEY, game_id TEXT,"
                " entity_type TEXT NOT NULL, entity_id TEXT, rule_id TEXT NOT NULL,"
                " severity TEXT NOT NULL, failure_reason TEXT NOT NULL, raw_payload JSON NOT NULL,"
                " source TEXT NOT NULL, status TEXT NOT NULL, retry_count INTEGER NOT NULL,"
                " resolved_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"
            ),
        )
        connection.execute(
            text(
                "CREATE TABLE correction_audit_trail (id INTEGER PRIMARY KEY, game_id TEXT,"
                " entity_type TEXT NOT NULL, entity_id TEXT, field_name TEXT NOT NULL, raw_value TEXT,"
                " raw_source TEXT NOT NULL, corrected_value TEXT, corrected_source TEXT NOT NULL,"
                " correction_reason TEXT NOT NULL, confidence FLOAT NOT NULL, extra_metadata JSON,"
                " created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"
            ),
        )
        connection.execute(
            text(
                "CREATE TABLE player_projections (id INTEGER PRIMARY KEY, target_season INTEGER NOT NULL,"
                " player_id INTEGER NOT NULL, player_name TEXT NOT NULL, team_code TEXT,"
                " position_type TEXT NOT NULL, age INTEGER, projected_pa FLOAT, projected_ip NUMERIC(6,2),"
                " projected_avg FLOAT, projected_obp FLOAT, projected_slg FLOAT, projected_ops FLOAT,"
                " projected_woba FLOAT, projected_era FLOAT, projected_fip FLOAT, projected_whip FLOAT,"
                " projected_stats JSON NOT NULL, weights_used JSON NOT NULL, regression_params JSON NOT NULL,"
                " version TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"
            ),
        )


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


def test_adopt_existing_schema_records_current_baseline_without_running_sql() -> None:
    engine = create_engine("sqlite:///:memory:")
    _create_adoptable_schema(engine)

    assert adopt_existing_schema(engine) == sorted(ADOPTABLE_MIGRATIONS)
    with engine.connect() as connection:
        applied = connection.execute(text("SELECT version FROM schema_migrations ORDER BY version")).scalars().all()
    assert applied == sorted(ADOPTABLE_MIGRATIONS)
    assert adopt_existing_schema(engine) == []


def test_adopt_existing_schema_rejects_missing_award_link_shape() -> None:
    engine = create_engine("sqlite:///:memory:")
    _create_baseline(engine)

    with pytest.raises(RuntimeError, match="awards table"):
        adopt_existing_schema(engine)

    assert not inspect(engine).has_table("schema_migrations")


def test_adopt_existing_schema_rejects_legacy_tracking_table() -> None:
    engine = create_engine("sqlite:///:memory:")
    _create_adoptable_schema(engine)
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE _schema_migrations (filename TEXT PRIMARY KEY)"))

    with pytest.raises(RuntimeError, match="Legacy _schema_migrations"):
        adopt_existing_schema(engine)


def test_adopt_existing_cli_skips_orm_bootstrap() -> None:
    engine = MagicMock()
    with (
        patch("src.cli.apply_postgres_migrations.create_engine_for_url", return_value=engine),
        patch("src.cli.apply_postgres_migrations.adopt_existing_schema", return_value=[]),
        patch("src.cli.apply_postgres_migrations._bootstrap_orm_schema") as bootstrap,
    ):
        from src.cli.apply_postgres_migrations import main

        assert main(["--url", "postgresql://example/db", "--adopt-existing"]) == 0

    bootstrap.assert_not_called()
    engine.dispose.assert_called_once()
