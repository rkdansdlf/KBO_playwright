"""Shared SQLite helpers for selecting primary game rows."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


DEFAULT_DB_PATH = Path("data/kbo_dev.db")
DEFAULT_PRIMARY_CODE_PREFERENCES = ("SSG", "KH", "DB", "KIA")
DEFAULT_REGULAR_SUFFIXES = ("0", "1", "2")


@dataclass(frozen=True)
class DeduplicationWindow:
    label: str
    start_date: str
    end_date: str
    clear_year: int | None = None


@dataclass(frozen=True)
class DeduplicationResult:
    scanned_slots: int
    marked_primary: int


def mark_primary_games(
    db_path: str | Path = DEFAULT_DB_PATH,
    *,
    windows: Iterable[DeduplicationWindow] | None = None,
    reset_all: bool = True,
    clear_years: Iterable[int] | None = None,
    suffixes: Sequence[str] = DEFAULT_REGULAR_SUFFIXES,
    preferred_codes: Sequence[str] = DEFAULT_PRIMARY_CODE_PREFERENCES,
    remove_extreme_dates: bool = False,
) -> DeduplicationResult:
    """Mark one primary game per date/franchise/doubleheader slot.

    The winner is the candidate with the most `game_batting_stats` rows. Ties
    prefer modern canonical team-code strings such as SSG/KH/DB/KIA.
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        if reset_all:
            cursor.execute("UPDATE game SET is_primary = 0")
        for year in clear_years or ():
            cursor.execute(
                "UPDATE game SET is_primary = 0 WHERE strftime('%Y', game_date) = ?",
                (str(year),),
            )

        scanned = 0
        marked = 0
        if windows:
            for window in windows:
                if window.clear_year is not None:
                    cursor.execute(
                        "UPDATE game SET is_primary = 0 WHERE strftime('%Y', game_date) = ?",
                        (str(window.clear_year),),
                    )
                window_result = _mark_window(
                    cursor,
                    start_date=window.start_date,
                    end_date=window.end_date,
                    suffixes=suffixes,
                    preferred_codes=preferred_codes,
                )
                scanned += window_result.scanned_slots
                marked += window_result.marked_primary
        else:
            result = _mark_window(
                cursor,
                start_date=None,
                end_date=None,
                suffixes=suffixes,
                preferred_codes=preferred_codes,
            )
            scanned += result.scanned_slots
            marked += result.marked_primary

        if remove_extreme_dates:
            cursor.execute(
                """
                UPDATE game
                SET is_primary = 0
                WHERE (strftime('%m%d', game_date) < '0301' OR strftime('%m%d', game_date) > '1231')
                  AND strftime('%Y', game_date) != '2026'
                """
            )

        conn.commit()
        return DeduplicationResult(scanned_slots=scanned, marked_primary=marked)
    finally:
        conn.close()


def _mark_window(
    cursor: sqlite3.Cursor,
    *,
    start_date: str | None,
    end_date: str | None,
    suffixes: Sequence[str],
    preferred_codes: Sequence[str],
) -> DeduplicationResult:
    groups = _load_slots(cursor, start_date=start_date, end_date=end_date, suffixes=suffixes)
    marked = 0
    for game_date, home_fid, away_fid, suffix in groups:
        candidates = _load_candidates(
            cursor,
            game_date=game_date,
            home_fid=home_fid,
            away_fid=away_fid,
            suffix=suffix,
            start_date=start_date,
            end_date=end_date,
            suffixes=suffixes,
        )
        if not candidates:
            continue
        best_id = _select_primary(candidates, preferred_codes)
        cursor.execute("UPDATE game SET is_primary = 1 WHERE game_id = ?", (best_id,))
        marked += 1
    return DeduplicationResult(scanned_slots=len(groups), marked_primary=marked)


def _load_slots(
    cursor: sqlite3.Cursor,
    *,
    start_date: str | None,
    end_date: str | None,
    suffixes: Sequence[str],
) -> list[tuple[str, int, int, str]]:
    suffix_placeholders = ",".join("?" for _ in suffixes)
    where_clauses = [
        "SUBSTR(game_id, -1, 1) IN (" + suffix_placeholders + ")",
        "home_franchise_id IS NOT NULL",
        "away_franchise_id IS NOT NULL",
    ]
    params: list[object] = list(suffixes)
    if start_date and end_date:
        where_clauses.append("game_date BETWEEN ? AND ?")
        params.extend([start_date, end_date])

    rows = cursor.execute(
        f"""
        SELECT game_date, home_franchise_id, away_franchise_id, SUBSTR(game_id, -1, 1) AS suffix
        FROM game
        WHERE {" AND ".join(where_clauses)}
        GROUP BY game_date, home_franchise_id, away_franchise_id, suffix
        """,
        params,
    ).fetchall()
    return [(row[0], row[1], row[2], row[3]) for row in rows]


def _load_candidates(
    cursor: sqlite3.Cursor,
    *,
    game_date: str,
    home_fid: int,
    away_fid: int,
    suffix: str,
    start_date: str | None,
    end_date: str | None,
    suffixes: Sequence[str],
) -> list[tuple[str, int]]:
    where_clauses = [
        "g.game_date = ?",
        "g.home_franchise_id = ?",
        "g.away_franchise_id = ?",
        "g.game_id LIKE ?",
    ]
    params: list[object] = [game_date, home_fid, away_fid, f"%{suffix}"]
    if start_date and end_date:
        where_clauses.append("g.game_date BETWEEN ? AND ?")
        params.extend([start_date, end_date])
    if suffixes:
        suffix_placeholders = ",".join("?" for _ in suffixes)
        where_clauses.append("SUBSTR(g.game_id, -1, 1) IN (" + suffix_placeholders + ")")
        params.extend(suffixes)

    rows = cursor.execute(
        f"""
        SELECT g.game_id,
               (SELECT COUNT(*) FROM game_batting_stats b WHERE b.game_id = g.game_id) AS stat_count
        FROM game g
        WHERE {" AND ".join(where_clauses)}
        """,
        params,
    ).fetchall()
    return [(row[0], int(row[1] or 0)) for row in rows]


def _select_primary(candidates: Sequence[tuple[str, int]], preferred_codes: Sequence[str]) -> str:
    return sorted(
        candidates,
        key=lambda row: (row[1], any(code in row[0] for code in preferred_codes), len(row[0]), row[0]),
        reverse=True,
    )[0][0]
