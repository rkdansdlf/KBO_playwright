"""Unit and concurrency tests for SQLite to OCI synchronization CLI and CheckpointManager."""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime

import pytest

from src.cli.sync_sqlite_to_oci import (
    SyncOptions,
    SqliteToOciSynchronizer,
    _resolve_source_url,
    _resolve_target_url,
    _sqlite_path_from_url,
)
from src.sync.checkpoint import CheckpointManager
from src.sync.oracle_writer import OracleWriter
from src.sync.table_dag import TABLE_META_BY_NAME, SyncStrategy, TableMeta


def test_checkpoint_manager_thread_safety() -> None:
    """Test CheckpointManager operations concurrently across multiple threads."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    mgr = CheckpointManager(conn)

    def worker(table_name: str, rows: int) -> None:
        now = datetime.now()
        mgr.record_success(table_name, synced_at=now, rows_synced=rows)
        ckpt = mgr.get_checkpoint(table_name)
        assert ckpt is not None
        assert ckpt.rows_synced >= rows

    threads = []
    for i in range(10):
        t = threading.Thread(target=worker, args=(f"table_{i % 3}", 100))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    all_ckpts = mgr.get_all_checkpoints()
    assert len(all_ckpts) == 3
    conn.close()


def test_sqlite_to_oci_synchronizer_dry_run() -> None:
    """Test SqliteToOciSynchronizer dry-run on in-memory SQLite tables."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE test_game (game_id TEXT PRIMARY KEY, updated_at TEXT)")
    conn.execute("INSERT INTO test_game VALUES ('20260101LGKIA0', '2026-08-01 12:00:00')")
    conn.commit()

    options = SyncOptions(concurrency=2, apply_changes=False)
    sync = SqliteToOciSynchronizer(
        sqlite_path=":memory:",
        oci_url=None,
        tns_admin=None,
        options=options,
    )
    sync.sq_conn = conn
    sync.checkpoint_mgr = CheckpointManager(conn)

    meta = TableMeta("test_game", level=1, strategy=SyncStrategy.INCREMENTAL, timestamp_col="updated_at")
    res = sync.sync_single_table(meta, mode="incremental", since_dt=None, season=None, writer=None)

    assert res.status == "DRY_RUN"
    assert res.candidates_count == 1
    assert res.table_name == "test_game"
    conn.close()


def test_sqlite_to_oci_dry_run_does_not_create_checkpoint_table(tmp_path) -> None:
    """Test that a dry-run leaves the SQLite source schema unchanged."""
    db_path = tmp_path / "source.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE game (game_id TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()

    sync = SqliteToOciSynchronizer(
        sqlite_path=str(db_path),
        oci_url=None,
        tns_admin=None,
        options=SyncOptions(apply_changes=False),
    )
    sync.close()

    with sqlite3.connect(db_path) as verify_conn:
        checkpoint_table = verify_conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_sync_checkpoints'",
        ).fetchone()
    assert checkpoint_table is None


def test_sync_url_resolution_separates_sqlite_source_and_oracle_target(monkeypatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("SQLITE_SOURCE_URL", "sqlite:////tmp/source.db")
    monkeypatch.setenv("OCI_DB_URL", "oracle+oracledb://user:pass@db/service")

    assert _resolve_source_url(None) == "sqlite:////tmp/source.db"
    assert _resolve_target_url(None) == "oracle+oracledb://user:pass@db/service"
    assert _sqlite_path_from_url("sqlite:////tmp/source.db") == "/tmp/source.db"


def test_sync_url_resolution_uses_oracle_database_url_as_target(monkeypatch) -> None:
    monkeypatch.delenv("SQLITE_SOURCE_URL", raising=False)
    monkeypatch.delenv("OCI_DB_URL", raising=False)
    monkeypatch.setenv("DATABASE_URL", "oracle+oracledb://user:pass@db/service")

    assert _resolve_source_url(None).startswith("sqlite:")
    assert _resolve_target_url(None) == "oracle+oracledb://user:pass@db/service"


def test_table_registry_covers_every_orm_table() -> None:
    import src.models
    from src.models.base import Base

    assert set(Base.metadata.tables) == set(TABLE_META_BY_NAME)


def test_oracle_writer_rejects_silent_string_truncation() -> None:
    writer = OracleWriter.__new__(OracleWriter)

    with pytest.raises(ValueError, match="exceeds limit 5"):
        writer.convert_value("very long text string", "VARCHAR2", char_limit=5)
