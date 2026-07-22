# noqa: INP001
"""Migration 047: preserve team-split season rows in SQLite."""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

DEFAULT_DB_PATH = Path(__file__).resolve().parents[2] / "data" / "kbo_dev.db"
TABLES = ("player_season_batting", "player_season_pitching")
OLD_UNIQUE = re.compile(
    r"UNIQUE\s*\(\s*player_id\s*,\s*season\s*,\s*league\s*,\s*level\s*\)",
    flags=re.IGNORECASE,
)
NEW_UNIQUE = re.compile(
    r"UNIQUE\s*\(\s*player_id\s*,\s*season\s*,\s*league\s*,\s*level\s*,\s*team_code\s*\)",
    flags=re.IGNORECASE,
)


def _table_sql(conn: sqlite3.Connection, table_name: str) -> str:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row[0] if row else ""


def _index_sql(conn: sqlite3.Connection, table_name: str) -> list[str]:
    rows = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL",
        (table_name,),
    ).fetchall()
    return [row[0] for row in rows]


def _columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    return [row[1] for row in conn.execute(f'PRAGMA table_info("{table_name}")')]


def _rebuild_table(conn: sqlite3.Connection, table_name: str) -> bool:
    create_sql = _table_sql(conn, table_name)
    if not create_sql or NEW_UNIQUE.search(create_sql):
        return False
    if not OLD_UNIQUE.search(create_sql):
        message = f"{table_name} does not contain the expected season unique constraint"
        raise RuntimeError(message)

    indexes = _index_sql(conn, table_name)
    backup_name = f"{table_name}__team_code_legacy"
    columns = _columns(conn, table_name)
    quoted_columns = ", ".join(f'"{column}"' for column in columns)

    conn.execute(f'ALTER TABLE "{table_name}" RENAME TO "{backup_name}"')
    conn.execute(OLD_UNIQUE.sub("UNIQUE (player_id, season, league, level, team_code)", create_sql, count=1))
    conn.execute(
        f'INSERT INTO "{table_name}" ({quoted_columns}) SELECT {quoted_columns} FROM "{backup_name}"',
    )
    conn.execute(f'DROP TABLE "{backup_name}"')
    for index_sql in indexes:
        conn.execute(index_sql)
    return True


def upgrade(conn: sqlite3.Connection | None = None) -> None:
    """Rebuild season tables with team_code in their logical unique key."""
    should_close = conn is None
    if conn is None:
        conn = sqlite3.connect(DEFAULT_DB_PATH)

    foreign_keys = bool(conn.execute("PRAGMA foreign_keys").fetchone()[0])
    conn.commit()
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        for table_name in TABLES:
            _rebuild_table(conn, table_name)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.execute(f"PRAGMA foreign_keys = {'ON' if foreign_keys else 'OFF'}")
        conn.commit()
        if should_close:
            conn.close()


if __name__ == "__main__":
    upgrade()
