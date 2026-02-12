#!/usr/bin/env python3
import os
import psycopg2
from dotenv import load_dotenv

def migrate_db(url):
    if not url: return
    print(f"🚀 Migrating {url}...")
    try:
        conn = psycopg2.connect(url)
        cur = conn.cursor()
        
        # 1. game_lineups
        cur.execute("ALTER TABLE game_lineups ADD COLUMN IF NOT EXISTS uniform_no VARCHAR(10)")
        cur.execute("ALTER TABLE game_lineups ADD COLUMN IF NOT EXISTS standard_position VARCHAR(10)")
        
        # 2. game_batting_stats
        cur.execute("ALTER TABLE game_batting_stats ADD COLUMN IF NOT EXISTS uniform_no VARCHAR(10)")
        cur.execute("ALTER TABLE game_batting_stats ADD COLUMN IF NOT EXISTS standard_position VARCHAR(10)")
        
        # 3. game_pitching_stats
        cur.execute("ALTER TABLE game_pitching_stats ADD COLUMN IF NOT EXISTS uniform_no VARCHAR(10)")
        
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Migration successful.")
    except Exception as e:
        print(f"❌ Migration failed: {e}")

if __name__ == "__main__":
    load_dotenv()
    oci_url = os.getenv("OCI_DB_URL")
    migrate_db(oci_url)
    
    if oci_url and oci_url.endswith("/postgres"):
        bega_url = oci_url[:-9] + "/bega_backend"
        migrate_db(bega_url)
