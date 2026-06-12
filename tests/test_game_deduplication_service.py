"""Tests for game_deduplication_service — mark_primary_games logic."""

import sqlite3
import tempfile
from pathlib import Path

import pytest

from src.services.game_deduplication_service import (
    DEFAULT_PRIMARY_CODE_PREFERENCES,
    DeduplicationWindow,
    _load_candidates,
    _load_slots,
    _mark_window,
    _select_primary,
    mark_primary_games,
)

# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def db_path():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = Path(f.name)
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE game (
            game_id TEXT PRIMARY KEY,
            game_date TEXT,
            home_franchise_id INT,
            away_franchise_id INT,
            is_primary INT DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE game_batting_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT
        )
        """
    )
    conn.commit()
    conn.close()
    yield path
    path.unlink(missing_ok=True)


def _add_game(conn, game_id, game_date, home_fid, away_fid, is_primary=0):
    conn.execute(
        "INSERT INTO game (game_id, game_date, home_franchise_id, away_franchise_id, is_primary) "
        "VALUES (?, ?, ?, ?, ?)",
        (game_id, game_date, home_fid, away_fid, is_primary),
    )


def _add_stats(conn, game_id, count=1):
    for _ in range(count):
        conn.execute("INSERT INTO game_batting_stats (game_id) VALUES (?)", (game_id,))


# ── _select_primary ──────────────────────────────────────────────────────────


class TestSelectPrimary:
    def test_selects_highest_stat_count(self):
        candidates = [("G0", 5), ("G2", 10), ("G3", 3)]
        assert _select_primary(candidates, DEFAULT_PRIMARY_CODE_PREFERENCES) == "G2"

    def test_tiebreak_preferred_code(self):
        candidates = [("SSG0", 5), ("LG0", 5)]
        assert _select_primary(candidates, ("SSG",)) == "SSG0"

    def test_single_candidate(self):
        assert _select_primary([("G1", 0)], ()) == "G1"


# ── _load_slots ──────────────────────────────────────────────────────────────


class TestLoadSlots:
    def test_groups_by_date_home_away_suffix(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "20241015LGSS0", "2024-10-15", 1, 2)
        _add_game(conn, "20241015LGSS1", "2024-10-15", 1, 2)
        _add_game(conn, "20241015LGSS0_v2", "2024-10-15", 1, 2)
        conn.commit()
        cursor = conn.cursor()

        slots = _load_slots(cursor, start_date=None, end_date=None, suffixes=("0", "1"))
        assert len(slots) == 2
        suffixes = {s[3] for s in slots}
        assert suffixes == {"0", "1"}
        conn.close()

    def test_filters_by_date_range(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "202410150", "2024-10-15", 1, 2)
        _add_game(conn, "202410160", "2024-10-16", 1, 2)
        conn.commit()
        cursor = conn.cursor()

        slots = _load_slots(cursor, start_date="2024-10-15", end_date="2024-10-15", suffixes=("0",))
        assert len(slots) == 1
        assert slots[0][0] == "2024-10-15"
        conn.close()

    def test_excludes_null_franchise_ids(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "G1_0", "2024-10-15", None, 2)
        _add_game(conn, "G2_0", "2024-10-15", 1, None)
        _add_game(conn, "G3_0", "2024-10-15", 1, 2)
        conn.commit()
        cursor = conn.cursor()

        slots = _load_slots(cursor, start_date=None, end_date=None, suffixes=("0",))
        assert len(slots) == 1
        conn.close()

    def test_filters_by_suffix(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "G1_0", "2024-10-15", 1, 2)
        _add_game(conn, "G2_1", "2024-10-15", 1, 2)
        conn.commit()
        cursor = conn.cursor()

        slots = _load_slots(cursor, start_date=None, end_date=None, suffixes=("0",))
        assert len(slots) == 1
        assert slots[0][3] == "0"
        conn.close()


# ── _load_candidates ─────────────────────────────────────────────────────────


class TestLoadCandidates:
    def test_returns_candidates_matching_slot_and_suffix(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "G_A_0", "2024-10-15", 1, 2)
        _add_game(conn, "G_B_0", "2024-10-15", 1, 2)
        _add_game(conn, "OTHER_1", "2024-10-15", 1, 2)
        conn.commit()
        cursor = conn.cursor()

        # suffix="0" + suffixes=("0",) — only game_ids ending with 0 qualify
        candidates = _load_candidates(
            cursor,
            game_date="2024-10-15",
            home_fid=1,
            away_fid=2,
            suffix="0",
            start_date=None,
            end_date=None,
            suffixes=("0",),
        )
        assert len(candidates) == 2
        # G_A_0 and G_B_0 both end with 0, OTHER_1 ends with 1
        assert all(c[0].endswith("_0") for c in candidates)
        conn.close()

    def test_includes_stat_count(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "G1_0", "2024-10-15", 1, 2)
        _add_game(conn, "G2_0", "2024-10-15", 1, 2)
        _add_stats(conn, "G2_0", count=3)
        conn.commit()
        cursor = conn.cursor()

        candidates = _load_candidates(
            cursor,
            game_date="2024-10-15",
            home_fid=1,
            away_fid=2,
            suffix="0",
            start_date=None,
            end_date=None,
            suffixes=("0",),
        )
        lookup = dict(candidates)
        assert "G1_0" in lookup
        assert lookup["G1_0"] == 0
        assert lookup["G2_0"] == 3
        conn.close()


# ── _mark_window ─────────────────────────────────────────────────────────────


class TestMarkWindow:
    def test_marks_highest_stat_count_per_slot(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "G1_0", "2024-10-15", 1, 2)
        _add_game(conn, "G2_0", "2024-10-15", 1, 2)
        _add_stats(conn, "G2_0", count=5)
        _add_stats(conn, "G1_0", count=1)
        conn.commit()

        cursor = conn.cursor()
        result = _mark_window(cursor, start_date=None, end_date=None, suffixes=("0",), preferred_codes=())
        conn.commit()

        assert result.scanned_slots == 1
        assert result.marked_primary == 1
        primary = conn.execute("SELECT game_id FROM game WHERE is_primary = 1").fetchall()
        assert primary == [("G2_0",)]
        conn.close()

    def test_no_games_returns_zero(self, db_path):
        conn = sqlite3.connect(db_path)
        conn.commit()
        cursor = conn.cursor()
        result = _mark_window(cursor, start_date=None, end_date=None, suffixes=("0",), preferred_codes=())
        assert result.marked_primary == 0
        conn.close()


# ── mark_primary_games (integration) ─────────────────────────────────────────


class TestMarkPrimaryGames:
    def test_resets_all_before_marking(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "G1_0", "2024-10-15", 1, 2, is_primary=1)
        _add_game(conn, "G2_0", "2024-10-16", 1, 2)
        conn.commit()
        conn.close()

        result = mark_primary_games(
            db_path,
            windows=[DeduplicationWindow(label="test", start_date="2024-10-15", end_date="2024-10-16")],
        )
        assert result.scanned_slots == 2
        assert result.marked_primary > 0

    def test_clear_years_resets_and_window_remains_usable(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "G2024_0", "2024-10-15", 1, 2, is_primary=1)
        conn.commit()
        conn.close()

        result = mark_primary_games(
            db_path,
            windows=[DeduplicationWindow(label="t", start_date="2024-10-15", end_date="2024-10-15")],
            clear_years=[2024],
        )
        # G2024 reset to 0, then re-marked to 1 (only game in slot)
        assert result.scanned_slots == 1
        assert result.marked_primary == 1
        conn2 = sqlite3.connect(db_path)
        assert conn2.execute("SELECT is_primary FROM game WHERE game_id = 'G2024_0'").fetchone()[0] == 1
        conn2.close()

    def test_window_clear_year_works(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "G2024_0", "2024-10-15", 1, 2, is_primary=1)
        conn.commit()
        conn.close()

        result = mark_primary_games(
            db_path,
            windows=[DeduplicationWindow(label="t", start_date="2024-10-15", end_date="2024-10-15", clear_year=2024)],
        )
        assert result.marked_primary == 1

    def test_integration_mark_primary_with_duplicates(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "G_A_0", "2024-10-15", 1, 2)
        _add_game(conn, "G_B_0", "2024-10-15", 1, 2)
        _add_stats(conn, "G_A_0", count=10)
        conn.commit()
        conn.close()

        result = mark_primary_games(
            db_path,
            windows=[DeduplicationWindow(label="test", start_date="2024-10-15", end_date="2024-10-15")],
        )
        assert result.scanned_slots == 1
        assert result.marked_primary == 1

        conn2 = sqlite3.connect(db_path)
        primary = conn2.execute("SELECT game_id FROM game WHERE is_primary = 1").fetchall()
        assert primary == [("G_A_0",)]
        conn2.close()

    def test_no_windows_processes_all(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "G1_0", "2024-10-15", 1, 2)
        _add_stats(conn, "G1_0", count=1)
        conn.commit()
        conn.close()

        result = mark_primary_games(db_path, windows=None)
        assert result.scanned_slots > 0
        assert result.marked_primary > 0

    def test_remove_extreme_dates(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "G_EXTREME_0", "2022-02-15", 1, 2, is_primary=1)
        _add_game(conn, "G_NORMAL_0", "2024-06-15", 1, 2, is_primary=1)
        conn.commit()
        conn.close()

        mark_primary_games(db_path, windows=None, remove_extreme_dates=True)
        conn2 = sqlite3.connect(db_path)
        rows = conn2.execute("SELECT game_id, is_primary FROM game ORDER BY game_id").fetchall()
        assert dict(rows)["G_EXTREME_0"] == 0
        assert dict(rows)["G_NORMAL_0"] == 1
        conn2.close()

    def test_mark_primary_prefers_stat_count(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "SLOT_A0", "2024-10-15", 1, 2)
        _add_game(conn, "SLOT_B0", "2024-10-15", 1, 2)
        _add_stats(conn, "SLOT_A0", count=1)
        _add_stats(conn, "SLOT_B0", count=5)
        conn.commit()
        conn.close()

        mark_primary_games(
            db_path,
            windows=[DeduplicationWindow(label="t", start_date="2024-10-15", end_date="2024-10-15")],
        )
        conn2 = sqlite3.connect(db_path)
        primary = conn2.execute("SELECT game_id FROM game WHERE is_primary = 1").fetchall()
        assert primary == [("SLOT_B0",)]
        conn2.close()
