from __future__ import annotations

import sqlite3

from src.services.game_deduplication_service import DeduplicationWindow, mark_primary_games
from scripts.maintenance import smart_deduplicate


def _init_db(path):
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE game (
            game_id TEXT PRIMARY KEY,
            game_date TEXT NOT NULL,
            home_franchise_id INTEGER,
            away_franchise_id INTEGER,
            is_primary INTEGER DEFAULT 1
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE game_batting_stats (
            game_id TEXT NOT NULL,
            player_id INTEGER
        )
        """
    )
    conn.commit()
    return conn


def test_smart_deduplicate_marks_one_primary_per_slot(monkeypatch, tmp_path, capsys):
    db_path = tmp_path / "kbo_dev.db"
    conn = _init_db(db_path)
    cursor = conn.cursor()
    cursor.executemany(
        """
        INSERT INTO game (game_id, game_date, home_franchise_id, away_franchise_id, is_primary)
        VALUES (?, ?, ?, ?, 1)
        """,
        [
            ("20250401SKNC0", "2025-04-01", 1, 8),
            ("20250401SSGNC0", "2025-04-01", 1, 8),
            ("20250402LGSS0", "2025-04-02", 2, 3),
        ],
    )
    cursor.executemany(
        "INSERT INTO game_batting_stats (game_id, player_id) VALUES (?, ?)",
        [
            ("20250401SKNC0", 1),
            ("20250401SSGNC0", 1),
            ("20250401SSGNC0", 2),
            ("20250402LGSS0", 1),
        ],
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(smart_deduplicate, "DB_PATH", db_path)

    smart_deduplicate.smart_deduplicate()

    out = capsys.readouterr().out
    assert "2 primary games marked" in out

    conn = sqlite3.connect(db_path)
    rows = dict(conn.execute("SELECT game_id, is_primary FROM game").fetchall())
    conn.close()

    assert rows == {
        "20250401SKNC0": 0,
        "20250401SSGNC0": 1,
        "20250402LGSS0": 1,
    }


def test_deduplication_service_can_limit_to_windows(tmp_path):
    db_path = tmp_path / "kbo_dev.db"
    conn = _init_db(db_path)
    cursor = conn.cursor()
    cursor.executemany(
        """
        INSERT INTO game (game_id, game_date, home_franchise_id, away_franchise_id, is_primary)
        VALUES (?, ?, ?, ?, 1)
        """,
        [
            ("20250401SKNC0", "2025-04-01", 1, 8),
            ("20250401SSGNC0", "2025-04-01", 1, 8),
            ("20260115SKNC0", "2026-01-15", 1, 8),
            ("20260115SSGNC0", "2026-01-15", 1, 8),
        ],
    )
    cursor.executemany(
        "INSERT INTO game_batting_stats (game_id, player_id) VALUES (?, ?)",
        [
            ("20250401SSGNC0", 1),
            ("20250401SSGNC0", 2),
            ("20260115SSGNC0", 1),
            ("20260115SSGNC0", 2),
        ],
    )
    conn.commit()
    conn.close()

    result = mark_primary_games(
        db_path,
        windows=[DeduplicationWindow("2025 regular", "2025-03-22", "2025-10-31")],
        reset_all=True,
    )

    conn = sqlite3.connect(db_path)
    rows = dict(conn.execute("SELECT game_id, is_primary FROM game").fetchall())
    conn.close()

    assert result.scanned_slots == 1
    assert result.marked_primary == 1
    assert rows == {
        "20250401SKNC0": 0,
        "20250401SSGNC0": 1,
        "20260115SKNC0": 0,
        "20260115SSGNC0": 0,
    }
