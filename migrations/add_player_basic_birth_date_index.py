"""
Migration: Add birth_date_date column and indexes to player_basic table.
Applies to both Local SQLite and Remote OCI Postgres.
"""

import os
import sqlite3

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# 1. Local SQLite Migration
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "kbo_dev.db")
SQLITE_MIGRATION_SQL = os.path.join(os.path.dirname(__file__), "sqlite", "016_player_basic_birth_date_index.sql")
OCI_MIGRATION_SQL = os.path.join(os.path.dirname(__file__), "oci", "038_player_basic_birth_date_index.sql")


def migrate_sqlite():
    print(f"--- Migrating SQLite: {DB_PATH} ---")
    if not os.path.exists(DB_PATH):
        print(" [SKIP] SQLite DB not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Check player_basic columns
    cursor.execute("PRAGMA table_info(player_basic)")
    cols = {row[1] for row in cursor.fetchall()}

    if "birth_date_date" not in cols:
        print(" [ADD] Adding birth_date_date to SQLite...")
        # SQLite store dates as TEXT, but mapped as DATE in SQLAlchemy
        cursor.execute("ALTER TABLE player_basic ADD COLUMN birth_date_date DATE")
        print("  Column birth_date_date added.")
    else:
        print(" [SKIP] Column birth_date_date already exists on SQLite.")

    # Apply indexes SQL
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
