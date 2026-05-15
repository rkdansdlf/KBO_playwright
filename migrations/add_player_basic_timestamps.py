"""
Migration: Add created_at and updated_at timestamps to player_basic table.
Applies to both Local SQLite and Remote OCI Postgres.
"""
import os
import sqlite3
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# 1. Local SQLite Migration
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "kbo_dev.db")

def migrate_sqlite():
    print(f"--- Migrating SQLite: {DB_PATH} ---")
    if not os.path.exists(DB_PATH):
        print(" [SKIP] SQLite DB not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA table_info(player_basic)")
    cols = {row[1] for row in cursor.fetchall()}
    
    if "created_at" not in cols:
        print(" [ADD] Adding created_at to SQLite...")
        # SQLite limitation: Cannot add a column with non-constant default (CURRENT_TIMESTAMP)
        cursor.execute("ALTER TABLE player_basic ADD COLUMN created_at DATETIME DEFAULT '2026-01-01 00:00:00'")
    
    if "updated_at" not in cols:
        print(" [ADD] Adding updated_at to SQLite...")
        cursor.execute("ALTER TABLE player_basic ADD COLUMN updated_at DATETIME DEFAULT '2026-01-01 00:00:00'")
        
    conn.commit()
    conn.close()
    print(" ✅ SQLite migration complete.")

def migrate_oci():
    url = os.getenv("OCI_DB_URL")
    if not url:
        print(" [SKIP] OCI_DB_URL not set.")
        return
    
    print(f"--- Migrating OCI Postgres ---")
    engine = create_engine(url)
    with engine.connect() as conn:
        # Check columns
        res = conn.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'player_basic' AND column_name IN ('created_at', 'updated_at')
        """))
        existing = {row[0] for row in res}
        
        if "created_at" not in existing:
            print(" [ADD] Adding created_at to OCI...")
            conn.execute(text("ALTER TABLE player_basic ADD COLUMN created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"))
            conn.execute(text("UPDATE player_basic SET created_at = '2026-01-01 00:00:00+00'"))
            conn.execute(text("ALTER TABLE player_basic ALTER COLUMN created_at SET NOT NULL"))
            
        if "updated_at" not in existing:
            print(" [ADD] Adding updated_at to OCI...")
            conn.execute(text("ALTER TABLE player_basic ADD COLUMN updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"))
            conn.execute(text("UPDATE player_basic SET updated_at = '2026-01-01 00:00:00+00'"))
            conn.execute(text("ALTER TABLE player_basic ALTER COLUMN updated_at SET NOT NULL"))
            
        conn.commit()
    print(" ✅ OCI migration complete.")

if __name__ == "__main__":
    migrate_sqlite()
    migrate_oci()
