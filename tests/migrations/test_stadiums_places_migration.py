"""Migration tests for stadiums/places (sqlite 067, postgresql 062, oracle 079)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SQLITE_MIGRATION = ROOT / "migrations/sqlite/067_stadiums_places.sql"
POSTGRES_MIGRATION = ROOT / "migrations/postgresql/062_stadiums_places.sql"
ORACLE_MIGRATION = ROOT / "migrations/oracle/079_stadiums_places.sql"

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


def test_oracle_migration_is_idempotent_syntax() -> None:
    sql = ORACLE_MIGRATION.read_text(encoding="utf-8")
    assert "user_tables" in sql
    assert "user_indexes" in sql
    assert "CREATE TABLE STADIUMS" in sql
    assert "CREATE TABLE PLACES" in sql
    assert "HOMERUN_PARK_FACTOR" in sql
    assert "IDX_PLACES_STADIUM" in sql
    assert "DROP" not in sql


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
