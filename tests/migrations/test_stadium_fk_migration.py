"""Migration tests for the stadium FK repoint (sqlite 070, postgresql 065, oracle 082)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SQLITE_MIGRATION = ROOT / "migrations/sqlite/070_stadium_fk_to_stadiums.sql"
POSTGRES_MIGRATION = ROOT / "migrations/postgresql/065_stadium_fk_to_stadiums.sql"
ORACLE_MIGRATION = ROOT / "migrations/oracle/082_stadium_fk_to_stadiums.sql"

CHILD_TABLES = (
    "parking_lots",
    "stadium_food_vendors",
    "stadium_seat_sections",
    "stadium_congestion",
    "stadium_transit_times",
    "stadium_operation_notices",
)


def test_sqlite_rebuild_preserves_data_and_repoints_fk() -> None:
    base = (ROOT / "migrations/sqlite/067_stadiums_places.sql").read_text(encoding="utf-8")
    food = (ROOT / "migrations/sqlite/013_phase1_p1_models.sql").read_text(encoding="utf-8")
    realtime = (ROOT / "migrations/sqlite/017_stadium_realtime_tables.sql").read_text(encoding="utf-8")
    sql = SQLITE_MIGRATION.read_text(encoding="utf-8")
    with sqlite3.connect(":memory:") as connection:
        connection.executescript(base)
        connection.executescript(food)
        connection.executescript(realtime)
        connection.execute("INSERT INTO stadiums (stadium_id, stadium_name) VALUES ('TST', '테스트구장')")
        connection.execute("INSERT INTO parking_lots (stadium_id, name) VALUES ('TST', '제1주차장')")
        connection.execute("INSERT INTO stadium_food_vendors (stadium_id, vendor_name) VALUES ('TST', '김밥')")
        # Sanity: old FK target before rebuild.
        fks_before = {row[2] for row in connection.execute("PRAGMA foreign_key_list(parking_lots)")}
        assert "stadium_info" in fks_before
        connection.executescript(sql)

        assert connection.execute("SELECT COUNT(*) FROM parking_lots").fetchone() == (1,)
        assert connection.execute("SELECT COUNT(*) FROM stadium_food_vendors").fetchone() == (1,)
        for table in CHILD_TABLES:
            targets = {row[2] for row in connection.execute(f"PRAGMA foreign_key_list({table})")}
            assert "stadiums" in targets, (table, targets)
            assert "stadium_info" not in targets, (table, targets)


def test_postgres_migration_drops_and_recreates_constraints() -> None:
    sql = POSTGRES_MIGRATION.read_text(encoding="utf-8")
    assert sql.count("REFERENCES stadiums(stadium_id) ON DELETE CASCADE") == 6
    assert "DROP TABLE" not in sql
    assert "DROP CONSTRAINT IF EXISTS parking_lots_stadium_id_fkey" in sql
    assert "DROP CONSTRAINT IF EXISTS stadium_operation_notices_stadium_code_fkey" in sql


def test_oracle_migration_is_idempotent_syntax() -> None:
    sql = ORACLE_MIGRATION.read_text(encoding="utf-8")
    assert "user_constraints" in sql
    assert "REFERENCES STADIUMS (STADIUM_ID) ON DELETE CASCADE" in sql
    assert sql.count("DROP CONSTRAINT FK_") == 6
    assert "DROP TABLE" not in sql
    assert "DROP CONSTRAINT" in sql


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
