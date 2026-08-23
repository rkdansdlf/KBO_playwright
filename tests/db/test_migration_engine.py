"""Unit tests for src.db.migration_engine."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import create_engine, inspect

from src.db.dto import MigrationDialect
from src.db.migration_engine import MigrationEngine, _split_sql_statements

if TYPE_CHECKING:
    from pathlib import Path


def test_split_sql_statements() -> None:
    sql_text = """
    -- Initial comment
    CREATE TABLE test_a (id INTEGER PRIMARY KEY);
    -- Statement Separator
    CREATE TABLE test_b (id INTEGER PRIMARY KEY);
    """
    stmts = _split_sql_statements(sql_text)
    assert len(stmts) == 2
    assert "CREATE TABLE test_a" in stmts[0]
    assert "CREATE TABLE test_b" in stmts[1]


def test_migration_discovery(tmp_path: Path) -> None:
    oracle_dir = tmp_path / "oracle"
    oracle_dir.mkdir(parents=True)

    (oracle_dir / "001_create_users.sql").write_text("CREATE TABLE users (id INT);")
    (oracle_dir / "002_add_index.sql").write_text("CREATE INDEX idx_users ON users(id);")
    (oracle_dir / "README.md").write_text("Docs")

    engine = MigrationEngine(migrations_root=tmp_path)
    migrations = engine.get_available_migrations(MigrationDialect.ORACLE)

    assert len(migrations) == 2
    assert migrations[0].version == 1
    assert migrations[1].version == 2


def test_migration_apply_lifecycle(tmp_path: Path) -> None:
    sqlite_dir = tmp_path / "sqlite"
    sqlite_dir.mkdir(parents=True)

    (sqlite_dir / "001_create_table_a.sql").write_text("CREATE TABLE table_a (id INTEGER PRIMARY KEY, name TEXT);")
    (sqlite_dir / "002_create_table_b.sql").write_text("CREATE TABLE table_b (id INTEGER PRIMARY KEY, a_id INTEGER);")

    db_engine = create_engine("sqlite:///:memory:")
    engine = MigrationEngine(migrations_root=tmp_path)

    # Initial status check
    status_report = engine.get_status(db_engine, MigrationDialect.SQLITE)
    assert status_report.total_available == 2
    assert status_report.applied_count == 0
    assert status_report.pending_count == 2

    # Dry run
    dry_report = engine.apply_migrations(db_engine, MigrationDialect.SQLITE, dry_run=True)
    assert dry_report.applied_count == 0
    assert len(dry_report.results) == 2
    assert dry_report.results[0].status == "PENDING_DRY_RUN"

    # Actual Apply
    apply_report = engine.apply_migrations(db_engine, MigrationDialect.SQLITE, dry_run=False)
    assert apply_report.applied_count == 2
    assert apply_report.pending_count == 0
    assert apply_report.results[0].status == "APPLIED"
    assert apply_report.results[1].status == "APPLIED"

    # Verify tables created in DB
    with db_engine.connect() as conn:
        inspector = inspect(conn)
        tables = inspector.get_table_names()
        assert "table_a" in tables
        assert "table_b" in tables
        assert "schema_migrations" in tables

    # Second run should skip
    second_report = engine.apply_migrations(db_engine, MigrationDialect.SQLITE, dry_run=False)
    assert second_report.applied_count == 2
    assert second_report.pending_count == 0
    assert second_report.results[0].status == "SKIPPED"
