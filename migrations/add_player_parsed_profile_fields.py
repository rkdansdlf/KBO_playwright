"""
Migration: Add structured player profile columns to player_basic and players tables.
Applies to both Local SQLite and Remote OCI Postgres.
"""

import os
import sqlite3

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# Paths to files
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "kbo_dev.db")
SQLITE_MIGRATION_SQL = os.path.join(os.path.dirname(__file__), "sqlite", "018_add_player_parsed_profile_fields.sql")
OCI_MIGRATION_SQL = os.path.join(os.path.dirname(__file__), "oci", "040_add_player_parsed_profile_fields.sql")

# New columns configuration
NEW_COLUMNS = [
    ("salary_amount", "BIGINT"),
    ("salary_currency", "VARCHAR(8)"),
    ("signing_bonus_amount", "BIGINT"),
    ("signing_bonus_currency", "VARCHAR(8)"),
    ("draft_year", "INTEGER"),
    ("draft_round", "INTEGER"),
    ("draft_pick_overall", "INTEGER"),
    ("draft_type", "VARCHAR(32)"),
    ("education_path", "JSON"),
]


def migrate_sqlite():
    print(f"--- Migrating SQLite: {DB_PATH} ---")
    if not os.path.exists(DB_PATH):
        print(" [SKIP] SQLite DB not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for table in ["player_basic", "players"]:
        cursor.execute(f"PRAGMA table_info({table})")
        existing_cols = {row[1] for row in cursor.fetchall()}

        for col_name, col_type in NEW_COLUMNS:
            if col_name not in existing_cols:
                print(f" [ADD] Adding {col_name} ({col_type}) to {table} in SQLite...")
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
            else:
                print(f" [SKIP] Column {col_name} already exists in {table} (SQLite).")

    # Apply SQLite indexes SQL
    if os.path.exists(SQLITE_MIGRATION_SQL):
        print(" [RUN] Applying indexes from SQL file...")
        with open(SQLITE_MIGRATION_SQL, encoding="utf-8") as f:
            sql_script = f.read()
        cursor.executescript(sql_script)
        print("  SQLite indexes verified/created.")
    else:
        print(f" [WARN] SQLite index SQL file not found at {SQLITE_MIGRATION_SQL}")

    conn.commit()
    conn.close()
    print(" ✅ SQLite migration complete.")


def migrate_oci():
    url = os.getenv("OCI_DB_URL")
    if not url:
        print(" [SKIP] OCI_DB_URL not set.")
        return

    print("--- Migrating OCI Postgres ---")
    if not os.path.exists(OCI_MIGRATION_SQL):
        print(f" [ERROR] OCI migration SQL file not found at {OCI_MIGRATION_SQL}")
        return

    with open(OCI_MIGRATION_SQL, encoding="utf-8") as f:
        sql_script = f.read()

    engine = create_engine(url)
    with engine.connect() as conn:
        print(" [RUN] Executing PostgreSQL migration transaction...")
        conn.execute(text(sql_script))
        conn.commit()

    print(" ✅ OCI migration complete.")


if __name__ == "__main__":
    migrate_sqlite()
    migrate_oci()
