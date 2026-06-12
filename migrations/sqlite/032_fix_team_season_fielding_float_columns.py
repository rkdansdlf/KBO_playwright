"""
Migration 032: Fix INTEGER columns that should be REAL/FLOAT in
team_season_fielding and team_season_baserunning.

SQLite doesn't support ALTER COLUMN TYPE, so we read the CREATE TABLE
statement, replace the type of target columns, and recreate the table
via rename+copy to preserve all data.
"""

import re
import sqlite3
from pathlib import Path

DEFAULT_DB_PATH = Path(__file__).resolve().parents[2] / "data" / "kbo_dev.db"


def table_sql(conn, table_name):
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row[0] if row else ""


def column_has_type(create_sql, col_name, target_type):
    return bool(
        re.search(
            rf"\b{re.escape(col_name)}\s+{target_type}\b",
            create_sql,
            re.IGNORECASE,
        )
    )


def fix_column(conn, table_name, col_name):
    create_sql = table_sql(conn, table_name)
    if not create_sql:
        print(f"     Table {table_name} not found — skipping")
        return False

    if column_has_type(create_sql, col_name, "REAL"):
        print(f"     {table_name}.{col_name} already REAL — skipping")
        return False

    if not column_has_type(create_sql, col_name, "INTEGER"):
        print(f"     {table_name}.{col_name} is neither INTEGER nor REAL — skipping")
        return False

    print(f"     Fixing {table_name}.{col_name} (INTEGER → REAL)...", end="")

    # Capture current data
    conn.execute(f"CREATE TABLE {table_name}_backup AS SELECT * FROM {table_name}")
    all_data = conn.execute(f"SELECT * FROM {table_name}").description
    col_names = [d[0] for d in all_data]

    # Modify CREATE TABLE: replace "col_name INTEGER" with "col_name REAL"
    new_sql = re.sub(
        rf"\b{re.escape(col_name)}\s+INT(?:EGER)?\b",
        f"{col_name} REAL",
        create_sql,
        flags=re.IGNORECASE,
    )

    # Drop original, recreate with fixed DDL, reinsert data
    conn.execute(f"DROP TABLE {table_name}")
    conn.execute(new_sql)

    placeholders = ", ".join(["?"] * len(col_names))
    cols = ", ".join(col_names)
    rows = conn.execute(f"SELECT * FROM {table_name}_backup").fetchall()
    for row in rows:
        conn.execute(f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})", row)

    conn.execute(f"DROP TABLE {table_name}_backup")
    conn.commit()
    print(" OK")
    return True


def upgrade(conn=None):
    should_close = conn is None
    if conn is None:
        conn = sqlite3.connect(DEFAULT_DB_PATH)

    print("  032: Fix INTEGER columns → REAL")

    try:
        for table, cols in [
            ("team_season_fielding", ["def_innings", "fielding_pct", "range_factor_per_game"]),
            ("team_season_baserunning", ["sb_success_rate"]),
        ]:
            for col in cols:
                fix_column(conn, table, col)
    finally:
        if should_close:
            conn.close()

    print("  032 done")


if __name__ == "__main__":
    upgrade()
