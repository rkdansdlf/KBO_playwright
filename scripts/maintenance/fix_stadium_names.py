"""
Script to normalize stadium names in the game_metadata table.
"""
import os
import sys
from sqlalchemy import text
from dotenv import load_dotenv

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db.engine import SessionLocal
from src.utils.stadium_normalizer import normalize_stadium_name, get_stadium_code

def fix_stadium_names():
    load_dotenv()
    
    print("🚀 Starting stadium name normalization...")
    
    with SessionLocal() as session:
        # 1. Fetch all distinct stadium names in metadata
        res = session.execute(text("SELECT DISTINCT stadium_name FROM game_metadata WHERE stadium_name IS NOT NULL")).fetchall()
        unique_names = [r[0] for r in res]
        
        counts = {"updated": 0, "skipped": 0, "errors": 0}
        
        for raw_name in unique_names:
            normalized = normalize_stadium_name(raw_name)
            if normalized != raw_name:
                print(f"🔄 Normalizing: '{raw_name}' -> '{normalized}'")
                try:
                    # Update all rows with this raw name
                    session.execute(
                        text("UPDATE game_metadata SET stadium_name = :normalized WHERE stadium_name = :raw"),
                        {"normalized": normalized, "raw": raw_name}
                    )
                    counts["updated"] += 1
                except Exception as e:
                    print(f"  ❌ Error updating '{raw_name}': {e}")
                    counts["errors"] += 1
            else:
                counts["skipped"] += 1
        
        session.commit()
        print(f"\n✅ Local normalization complete: {counts['updated']} groups updated, {counts['skipped']} already normalized.")

    # 2. Sync to OCI if possible
    oci_url = os.getenv("OCI_DB_URL")
    if oci_url:
        print(f"\n☁️ Synchronizing changes to OCI...")
        from sqlalchemy import create_engine
        oci_engine = create_engine(oci_url)
        try:
            with oci_engine.begin() as oci_conn:
                for raw_name in unique_names:
                    normalized = normalize_stadium_name(raw_name)
                    if normalized != raw_name:
                        oci_conn.execute(
                            text("UPDATE game_metadata SET stadium_name = :normalized WHERE stadium_name = :raw"),
                            {"normalized": normalized, "raw": raw_name}
                        )
            print("✅ OCI synchronization successful.")
        except Exception as e:
            print(f"⚠️ OCI Sync failed: {e}")
    else:
        print("\n⚠️ OCI_DB_URL not found. Skipping sync.")

if __name__ == "__main__":
    fix_stadium_names()
