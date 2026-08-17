"""Unit and concurrency tests for SQLite to OCI synchronization CLI and CheckpointManager."""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime

import pytest

from src.cli.sync_sqlite_to_oci import (
    SyncOptions,
    SqliteToOciSynchronizer,
    WriteBatchContext,
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


def test_sync_url_resolution_prefers_oracle_database_url_over_admin_fallback(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "oracle+oracledb://app:pass@db/service")
    monkeypatch.setenv("OCI_DB_URL", "oracle+oracledb://ADMIN:pass@db/service")
    monkeypatch.delenv("ORACLE_TARGET_URL", raising=False)

    assert _resolve_target_url(None) == "oracle+oracledb://app:pass@db/service"


def test_table_registry_covers_every_orm_table() -> None:
    import src.models
    from src.models.base import Base

    assert set(Base.metadata.tables) == set(TABLE_META_BY_NAME)


def test_game_stat_sync_keys_preserve_multiple_appearances() -> None:
    """Game batting and pitching sync keys must include appearance sequence."""
    assert TABLE_META_BY_NAME["game_batting_stats"].natural_keys == [
        "game_id",
        "player_id",
        "appearance_seq",
    ]
    assert TABLE_META_BY_NAME["game_pitching_stats"].natural_keys == [
        "game_id",
        "player_id",
        "appearance_seq",
    ]


def test_oracle_writer_rejects_silent_string_truncation() -> None:
    writer = OracleWriter.__new__(OracleWriter)

    with pytest.raises(ValueError, match="exceeds limit 5"):
        writer.convert_value("very long text string", "VARCHAR2", char_limit=5)


def test_sync_normalizes_legacy_roster_and_movement_values() -> None:
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE player_basic (player_id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO player_basic VALUES (88770, '한용덕')")
    conn.execute("CREATE TABLE team_daily_roster (player_id INTEGER, player_name TEXT, canonical_team_id TEXT)")
    conn.execute("INSERT INTO team_daily_roster VALUES (88770, '', NULL)")
    conn.execute("CREATE TABLE player_movements (team_code TEXT, canonical_team_id TEXT)")
    conn.execute("INSERT INTO player_movements VALUES ('', 'LT')")
    conn.commit()

    sync = SqliteToOciSynchronizer(
        sqlite_path=":memory:",
        oci_url=None,
        tns_admin=None,
        options=SyncOptions(apply_changes=False),
    )
    sync.sq_conn = conn

    roster_row = conn.execute("SELECT * FROM team_daily_roster").fetchone()
    movement_row = conn.execute("SELECT * FROM player_movements").fetchone()
    assert roster_row is not None
    assert movement_row is not None
    assert sync._normalize_row_value("team_daily_roster", roster_row, "player_name") == "한용덕"
    assert sync._normalize_row_value("player_movements", movement_row, "team_code") == "LT"

    sync.close()
    conn.close()


def test_sync_write_batches_commits_at_configured_interval() -> None:
    class FakeWriter:
        def __init__(self) -> None:
            self.batch_sizes: list[int] = []
            self.commit_count = 0

        def convert_value(self, value, _oci_type, _char_limit=None):
            return value

        def execute_batch(self, _sql, payloads, _table, _columns, _pks, _types, _names, *, insert_only=False):
            assert insert_only is True
            self.batch_sizes.append(len(payloads))
            return len(payloads), 0

        def commit(self) -> None:
            self.commit_count += 1

    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE rows (id INTEGER PRIMARY KEY)")
    conn.executemany("INSERT INTO rows VALUES (?)", [(1,), (2,), (3,), (4,), (5,)])
    conn.commit()

    sync = SqliteToOciSynchronizer(
        sqlite_path=":memory:",
        oci_url=None,
        tns_admin=None,
        options=SyncOptions(batch_size=2, commit_every=3, apply_changes=True),
    )
    sync.sq_conn = conn
    writer = FakeWriter()
    context = WriteBatchContext(
        table="rows",
        query="SELECT id FROM rows ORDER BY id",
        params=[],
        sync_columns=["id"],
        pk_columns=["ID"],
        insert_only=True,
        oci_cols={"ID": "NUMBER"},
        column_names={"ID": "ID"},
        char_sizes={},
        sync_sql="INSERT",
        writer=writer,
    )

    assert sync._execute_write_batches(context) == (5, 0)
    assert writer.batch_sizes == [2, 2, 1]
    assert writer.commit_count == 2

    sync.close()
    conn.close()
