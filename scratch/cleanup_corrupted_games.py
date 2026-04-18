import os
import sys
from sqlalchemy import text
from dotenv import load_dotenv

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.db.engine import SessionLocal, Engine
from src.sync.oci_sync import OCISync

def cleanup_corrupted_games():
    load_dotenv()
    
    # 1. Identify corrupted game IDs in local SQLite
    # Criteria: length != 12 OR contains Korean characters
    # Specifically targeting the patterns provided by user as well
    
    corrupted_patterns = [
        "20260414KT00NC0",
        "20260414롯데00LG0",
        "20260414삼성00한화0"
    ]
    
    with SessionLocal() as session:
        # Find by length
        res = session.execute(text("SELECT game_id FROM game WHERE length(game_id) != 12")).fetchall()
        too_long_or_short = [r[0] for r in res]
        
        # Find by Korean characters (SQLite doesn't have regex by default, but we can check specific ones)
        res = session.execute(text("SELECT game_id FROM game WHERE game_id LIKE '%롯데%' OR game_id LIKE '%삼성%' OR game_id LIKE '%한화%'")).fetchall()
        korean_names = [r[0] for r in res]
        
        all_to_delete = sorted(list(set(too_long_or_short + korean_names + corrupted_patterns)))
        
        if not all_to_delete:
            print("✅ No corrupted game records found.")
            return

        print(f"🚀 Found {len(all_to_delete)} corrupted game records to delete:")
        for gid in all_to_delete:
            print(f"  - {gid}")
            
        # Delete from all related tables due to foreign keys (or cascading)
        # In SQLite if PRAGMA foreign_keys = ON and ON DELETE CASCADE is set, child rows follow.
        # But to be safe and thorough, let's delete explicitly if needed.
        tables = [
            "game_summary", "game_play_by_play", "game_metadata", "game_inning_scores",
            "game_lineups", "game_batting_stats", "game_pitching_stats", "game_events", "game"
        ]
        
        try:
            for gid in all_to_delete:
                for table in tables:
                    session.execute(text(f"DELETE FROM {table} WHERE game_id = :gid"), {"gid": gid})
            
            session.commit()
            print(f"✅ Successfully deleted corrupted records from local DB.")
        except Exception as e:
            session.rollback()
            print(f"❌ Error during local deletion: {e}")
            return

    # 2. Sync deletion to OCI
    oci_url = os.getenv("OCI_DB_URL")
    if oci_url:
        print(f"☁️ Synchronizing deletions to OCI...")
        try:
            # We don't have a direct 'delete' method in OCISync usually, 
            # but we can run raw SQL on the OCI engine if we have access.
            from sqlalchemy import create_engine
            oci_engine = create_engine(oci_url)
            with oci_engine.begin() as oci_conn:
                for gid in all_to_delete:
                    for table in tables:
                        oci_conn.execute(text(f"DELETE FROM {table} WHERE game_id = :gid"), {"gid": gid})
            print(f"✅ Successfully synchronized deletions to OCI.")
        except Exception as e:
            print(f"⚠️ OCI Sync failed (continuing): {e}")
    else:
        print("⚠️ OCI_DB_URL not set. Skipping OCI sync.")

if __name__ == "__main__":
    cleanup_corrupted_games()
