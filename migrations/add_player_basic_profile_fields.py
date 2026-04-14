"""
Migration: Add extended profile fields to player_basic table.
Adds: photo_url, bats, throws, debut_year, salary_original,
      signing_bonus_original, draft_info
"""
import sqlite3
import os
import sys

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "kbo_dev.db")

NEW_COLUMNS = [
    ("photo_url",               "VARCHAR(500)"),
    ("bats",                    "VARCHAR(4)"),
    ("throws",                  "VARCHAR(4)"),
    ("debut_year",              "INTEGER"),
    ("salary_original",         "VARCHAR(50)"),
    ("signing_bonus_original",  "VARCHAR(50)"),
    ("draft_info",              "VARCHAR(100)"),
]

def run():
    if not os.path.exists(DB_PATH):
        print(f"[ERROR] DB not found at {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get existing columns
    cursor.execute("PRAGMA table_info(player_basic)")
    existing = {row[1] for row in cursor.fetchall()}

    added = 0
    for col_name, col_type in NEW_COLUMNS:
        if col_name in existing:
            print(f"[SKIP]  Column already exists: {col_name}")
        else:
            cursor.execute(f"ALTER TABLE player_basic ADD COLUMN {col_name} {col_type}")
            print(f"[ADD]   Added column: {col_name} ({col_type})")
            added += 1

    conn.commit()
    conn.close()
    print(f"\n✅ Migration complete. {added} column(s) added.")

if __name__ == "__main__":
    run()
