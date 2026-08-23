"""Unit tests for src.sync.sync_engine."""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock

from src.sync.checkpoint import CheckpointManager
from src.sync.dto import SyncEngineConfig
from src.sync.sync_engine import OciSyncEngine
from src.sync.table_dag import SyncStrategy, TableMeta


def test_plan_table_incremental() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE game (game_id TEXT PRIMARY KEY, updated_at TEXT)")
    conn.execute("INSERT INTO game VALUES ('G1', '2026-08-23 10:00:00')")

    cp_manager = CheckpointManager(conn, initialize=True)
    config = SyncEngineConfig(mode="incremental", apply=False)
    engine = OciSyncEngine(sqlite_conn=conn, config=config, checkpoint_manager=cp_manager)

    meta = TableMeta("game", level=1, strategy=SyncStrategy.INCREMENTAL, timestamp_col="updated_at")
    plan = engine.plan_table(meta)

    assert plan.table_name == "game"
    assert plan.candidate_count == 1
    assert plan.is_dirty is True


def test_sync_table_dry_run() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE game (game_id TEXT PRIMARY KEY, updated_at TEXT)")
    conn.execute("INSERT INTO game VALUES ('G1', '2026-08-23 10:00:00')")

    cp_manager = CheckpointManager(conn, initialize=True)
    config = SyncEngineConfig(mode="incremental", apply=False)
    engine = OciSyncEngine(sqlite_conn=conn, config=config, checkpoint_manager=cp_manager)

    meta = TableMeta("game", level=1, strategy=SyncStrategy.INCREMENTAL, timestamp_col="updated_at")
    res = engine.sync_table(meta, dry_run=True)

    assert res.table_name == "game"
    assert res.candidates_count == 1
    assert res.synced_count == 1
    assert res.status == "DRY_RUN"


def test_sync_table_apply() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE game (game_id TEXT PRIMARY KEY, updated_at TEXT)")
    conn.execute("INSERT INTO game VALUES ('G1', '2026-08-23 10:00:00')")

    mock_writer = MagicMock()
    mock_writer.upsert_rows.return_value = 1

    cp_manager = CheckpointManager(conn, initialize=True)
    config = SyncEngineConfig(mode="incremental", apply=True)
    engine = OciSyncEngine(
        sqlite_conn=conn,
        config=config,
        checkpoint_manager=cp_manager,
        oracle_writer=mock_writer,
    )

    meta = TableMeta("game", level=1, strategy=SyncStrategy.INCREMENTAL, timestamp_col="updated_at")
    res = engine.sync_table(meta, dry_run=False)

    assert res.table_name == "game"
    assert res.synced_count == 1
    assert res.status == "SUCCESS"
    mock_writer.upsert_rows.assert_called_once()


def test_run_full_sync_dry_run() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE kbo_seasons (season_id INTEGER PRIMARY KEY)")
    conn.execute("INSERT INTO kbo_seasons VALUES (2025)")
    conn.commit()

    cp_manager = CheckpointManager(conn, initialize=True)
    config = SyncEngineConfig(mode="incremental", apply=False, concurrency=1)
    engine = OciSyncEngine(sqlite_conn=conn, config=config, checkpoint_manager=cp_manager)

    summary = engine.run_full_sync(dry_run=True)

    assert summary.mode == "incremental"
    assert summary.apply is False
    assert summary.tables_total > 0
    assert summary.tables_synced >= 1
