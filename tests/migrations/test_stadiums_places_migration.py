"""Migration tests for stadiums/places (sqlite 067, postgresql 062)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SQLITE_MIGRATION = ROOT / "migrations/sqlite/067_stadiums_places.sql"
POSTGRES_MIGRATION = ROOT / "migrations/postgresql/062_stadiums_places.sql"

EXPECTED_STADIUM_COLUMNS = {
    "stadium_id",
    "stadium_name",
    "city",
    "team",
    "capacity",
    "seating_capacity",
    "open_year",
    "left_fence_m",
    "center_fence_m",
    "fence_height_m",
    "turf_type",
    "bullpen_type",
    "homerun_park_factor",
    "notes",
    "lat",
    "lng",
    "address",
    "phone",
    "created_at",
    "updated_at",
}

SQLITE_DIM_MIGRATION = ROOT / "migrations/sqlite/068_stadiums_dimensions.sql"
POSTGRES_DIM_MIGRATION = ROOT / "migrations/postgresql/063_stadiums_dimensions.sql"

EXPECTED_PLACE_COLUMNS = {
    "stadium_id",
    "category",
    "name",
    "description",
    "lat",
    "lng",
    "address",
    "phone",
    "rating",
    "open_time",
    "close_time",
    "created_at",
    "updated_at",
}


def test_sqlite_migration_creates_tables_idempotently() -> None:
    sql = SQLITE_MIGRATION.read_text(encoding="utf-8")
    with sqlite3.connect(":memory:") as connection:
        connection.executescript(sql)
        connection.executescript(sql)

        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        stadium_columns = {row[1] for row in connection.execute("PRAGMA table_info(stadiums)")}
        place_columns = {row[1] for row in connection.execute("PRAGMA table_info(places)")}
        place_indexes = {row[1] for row in connection.execute("PRAGMA index_list(places)")}

    assert "stadiums" in tables
    assert "places" in tables
    assert stadium_columns >= EXPECTED_STADIUM_COLUMNS
    assert place_columns >= EXPECTED_PLACE_COLUMNS
    assert "idx_places_stadium" in place_indexes
    assert "idx_places_category" in place_indexes


def test_sqlite_migration_enforces_lat_lng_not_null() -> None:
    sql = SQLITE_MIGRATION.read_text(encoding="utf-8")
    with sqlite3.connect(":memory:") as connection:
        connection.executescript(sql)
        connection.execute("INSERT INTO stadiums (stadium_id, stadium_name) VALUES ('TST', '테스트구장')")
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("INSERT INTO places (stadium_id, category, name) VALUES ('TST', '음식점', '김밥')")
        connection.execute(
            "INSERT INTO places (stadium_id, category, name, lat, lng) VALUES ('TST', '음식점', '김밥', 37.5, 127.0)"
        )
        (count,) = connection.execute("SELECT COUNT(*) FROM places").fetchone()
    assert count == 1


def test_postgres_migration_is_idempotent_syntax() -> None:
    sql = POSTGRES_MIGRATION.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS stadiums" in sql
    assert "CREATE TABLE IF NOT EXISTS places" in sql
    assert "homerun_park_factor" in sql
    assert "lat DOUBLE PRECISION NOT NULL" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_places_stadium" in sql
    assert "DROP" not in sql


def test_dimension_migrations_add_geometry_columns() -> None:
    sqlite_sql = SQLITE_DIM_MIGRATION.read_text(encoding="utf-8")
    pg_sql = POSTGRES_DIM_MIGRATION.read_text(encoding="utf-8")
    assert "right_fence_m" in sqlite_sql
    assert "is_dome" in sqlite_sql
    assert "valid_to_year" in sqlite_sql
    assert "ADD COLUMN IF NOT EXISTS right_fence_m" in pg_sql
    assert "DROP" not in pg_sql

    # ALTER TABLE ADD COLUMN applies once via version tracking (unlike
    # CREATE TABLE IF NOT EXISTS), so a single application is asserted here.
    with sqlite3.connect(":memory:") as connection:
        connection.executescript(SQLITE_MIGRATION.read_text(encoding="utf-8"))
        connection.executescript(sqlite_sql)
        columns = {row[1] for row in connection.execute("PRAGMA table_info(stadiums)")}
    assert {"right_fence_m", "is_dome", "valid_from_year", "valid_to_year"} <= columns


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
