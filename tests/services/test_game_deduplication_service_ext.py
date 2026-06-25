import sqlite3
import tempfile
from pathlib import Path

import pytest

from src.services.game_deduplication_service import (
    DEFAULT_PRIMARY_CODE_PREFERENCES,
    DeduplicationWindow,
    _CandidateQuery,
    _load_candidates,
    _load_slots,
    _mark_window,
    _select_primary,
    mark_primary_games,
)


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


class TestSelectPrimary:
    def test_selects_highest_stat_count(self):
        candidates = [("G0", 5), ("G2", 10), ("G3", 3)]
        assert _select_primary(candidates, DEFAULT_PRIMARY_CODE_PREFERENCES) == "G2"

    def test_tiebreak_preferred_code(self):
        candidates = [("SSG0", 5), ("LG0", 5)]
        assert _select_primary(candidates, ("SSG",)) == "SSG0"

    def test_single_candidate(self):
        assert _select_primary([("G1", 0)], ()) == "G1"

    def test_tiebreak_preferred_code_with_longer_id(self):
        candidates = [("SSG202403150", 5), ("HT202403150", 5)]
        assert _select_primary(candidates, ("SSG", "KIA")) == "SSG202403150"


class TestLoadSlots:
    def test_returns_slots(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "20240315LGSS0", "2024-03-15", 1, 2)
        conn.commit()
        conn.close()
        cursor = sqlite3.connect(db_path).cursor()
        slots = _load_slots(cursor, start_date=None, end_date=None, suffixes=("0", "1", "2"))
        assert len(slots) == 1
        assert slots[0][0] == "2024-03-15"

    def test_empty_when_no_games(self, db_path):
        cursor = sqlite3.connect(db_path).cursor()
        slots = _load_slots(cursor, start_date=None, end_date=None, suffixes=("0",))
        assert slots == []

    def test_filters_by_date_window(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "20240315LGSS0", "2024-03-15", 1, 2)
        _add_game(conn, "20240316LGSS0", "2024-03-16", 1, 2)
        conn.commit()
        conn.close()
        cursor = sqlite3.connect(db_path).cursor()
        slots = _load_slots(cursor, start_date="2024-03-16", end_date="2024-03-16", suffixes=("0",))
        assert len(slots) == 1


class TestLoadCandidates:
    def test_returns_candidates_with_counts(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "20240315LGSS0", "2024-03-15", 1, 2)
        _add_stats(conn, "20240315LGSS0", 9)
        conn.commit()
        conn.close()
        cursor = sqlite3.connect(db_path).cursor()
        candidates = _load_candidates(
            cursor,
            query=_CandidateQuery(
                game_date="2024-03-15",
                home_fid=1,
                away_fid=2,
                suffix="0",
                suffixes=("0",),
            ),
        )
        assert len(candidates) == 1
        assert candidates[0][1] == 9

    def test_empty_when_no_match(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "20240315LGSS0", "2024-03-15", 1, 2)
        conn.commit()
        conn.close()
        cursor = sqlite3.connect(db_path).cursor()
        candidates = _load_candidates(
            cursor,
            query=_CandidateQuery(
                game_date="2024-03-16",
                home_fid=1,
                away_fid=2,
                suffix="0",
                suffixes=("0",),
            ),
        )
        assert candidates == []


class TestMarkWindow:
    def test_marks_primary(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "20240315LGSS0", "2024-03-15", 1, 2)
        _add_stats(conn, "20240315LGSS0", 10)
        _add_game(conn, "20240315LGSS1", "2024-03-15", 1, 2)
        _add_stats(conn, "20240315LGSS1", 5)
        conn.commit()
        conn.close()
        cursor = sqlite3.connect(db_path).cursor()
        window = DeduplicationWindow(label="test", start_date="2024-03-15", end_date="2024-03-15")
        result = _mark_window(
            cursor,
            start_date=window.start_date,
            end_date=window.end_date,
            suffixes=("0", "1"),
            preferred_codes=DEFAULT_PRIMARY_CODE_PREFERENCES,
        )
        assert result.scanned_slots == 2
        assert result.marked_primary == 2


class TestMarkPrimaryGames:
    def test_full_flow(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "20240315LGSS0", "2024-03-15", 1, 2)
        _add_stats(conn, "20240315LGSS0", 8)
        conn.commit()
        conn.close()
        window = DeduplicationWindow(label="w1", start_date="2024-03-15", end_date="2024-03-15")
        result = mark_primary_games(db_path, windows=[window], reset_all=True)
        assert result.scanned_slots == 1
        assert result.marked_primary == 1

    def test_no_windows_scans_all(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "20240315LGSS1", "2024-03-15", 1, 2)
        _add_stats(conn, "20240315LGSS1", 3)
        conn.commit()
        conn.close()
        result = mark_primary_games(db_path, windows=None, reset_all=True)
        assert result.scanned_slots >= 0

    def test_clear_year_resets_primary(self, db_path):
        conn = sqlite3.connect(db_path)
        _add_game(conn, "20240315LGSS0", "2024-03-15", 1, 2, is_primary=1)
        _add_stats(conn, "20240315LGSS0", 5)
        conn.commit()
        conn.close()
        window = DeduplicationWindow(label="w1", start_date="2024-03-15", end_date="2024-03-15", clear_year=2024)
        result = mark_primary_games(db_path, windows=[window], reset_all=False)
        conn2 = sqlite3.connect(db_path)
        row = conn2.execute("SELECT is_primary FROM game WHERE game_id='20240315LGSS0'").fetchone()
        conn2.close()
        assert row[0] == 1
        assert result.marked_primary >= 0
