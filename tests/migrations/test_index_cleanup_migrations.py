from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATHS = (
    ROOT / "migrations/sqlite/046_remove_redundant_phase1_indexes.sql",
    ROOT / "migrations/oci/047_remove_redundant_phase1_indexes.sql",
    ROOT / "migrations/supabase/027_remove_redundant_phase1_indexes.sql",
)
INDEX_CONTRACT = {
    "game_broadcasts": ("ix_game_broadcasts_game_id", "idx_broadcast_game"),
    "cheer_songs": ("ix_cheer_songs_team_id", "idx_cheer_song_team"),
    "cheer_chants": ("ix_cheer_chants_team_id", "idx_cheer_chant_team"),
    "foreign_player_changes": ("ix_foreign_player_changes_team_id", "idx_fp_team_season"),
    "game_mvps": ("ix_game_mvps_game_id", "idx_mvp_game"),
    "injury_entries": ("ix_injury_entries_team_id", "idx_injury_team"),
    "manager_changes": ("ix_manager_changes_team_id", "idx_mgr_team_season"),
}


def _create_index_fixture(connection: sqlite3.Connection) -> None:
    for table_name, (redundant_index, canonical_index) in INDEX_CONTRACT.items():
        connection.execute(f'CREATE TABLE "{table_name}" (value TEXT)')
        connection.execute(f'CREATE INDEX "{redundant_index}" ON "{table_name}" (value)')
        connection.execute(f'CREATE INDEX "{canonical_index}" ON "{table_name}" (value)')


def _index_names(connection: sqlite3.Connection, table_name: str) -> set[str]:
    return {row[1] for row in connection.execute(f'PRAGMA index_list("{table_name}")')}


@pytest.mark.parametrize("migration_path", MIGRATION_PATHS, ids=lambda path: path.parent.name)
def test_phase1_index_cleanup_migration_is_idempotent(migration_path: Path) -> None:
    sql = migration_path.read_text(encoding="utf-8")

    with sqlite3.connect(":memory:") as connection:
        _create_index_fixture(connection)
        connection.executescript(sql)
        connection.executescript(sql)

        for table_name, (redundant_index, canonical_index) in INDEX_CONTRACT.items():
            indexes = _index_names(connection, table_name)
            assert redundant_index not in indexes
            assert canonical_index in indexes
