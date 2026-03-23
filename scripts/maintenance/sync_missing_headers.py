import os
import sys
from sqlalchemy.orm import Session
from dotenv import load_dotenv

# Ensure root project is in path
sys.path.insert(0, os.getcwd())

from src.db.engine import SessionLocal
from src.sync.oci_sync import OCISync
from src.models.game import Game, GameMetadata

def sync_headers():
    load_dotenv()
    url = os.getenv("OCI_DB_URL")
    if not url:
        print("❌ OCI_DB_URL not found in .env")
        return

    years = list(range(2001, 2026))
    print(f"🚀 Starting sync of game headers for years: {years}")

    with SessionLocal() as session:
        syncer = OCISync(url, session)
        
        for year in years:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    print(f"\n📅 Processing Year {year} (Attempt {attempt + 1})...")
                    
                    # Sync Game table
                    game_filters = [Game.game_id.like(f"{year}%")]
                    print(f"  🚚 Syncing Game table for {year}...")
                    synced_games = syncer.sync_games(filters=game_filters)
                    print(f"  ✅ Synced {synced_games} game records.")
                    
                    # Sync GameMetadata table
                    meta_filters = [GameMetadata.game_id.like(f"{year}%")]
                    print(f"  🚚 Syncing GameMetadata table for {year}...")
                    synced_meta = syncer._sync_simple_table(
                        GameMetadata, 
                        ['game_id'], 
                        exclude_cols=['created_at', 'updated_at'],
                        filters=meta_filters
                    )
                    print(f"  ✅ Synced {synced_meta} metadata records.")
                    break # Success, move to next year
                except Exception as e:
                    print(f"  ❌ Error syncing {year}: {e}")
                    if attempt < max_retries - 1:
                        print(f"  🔄 Retrying in 5 seconds...")
                        import time
                        time.sleep(5)
                        # Re-instantiate syncer on error to refresh connection
                        syncer.close()
                        syncer = OCISync(url, session)
                    else:
                        print(f"  🛑 Maximum retries reached for {year}. Skipping...")

        syncer.close()

    print("\n🎉 Sync Process Completed!")

if __name__ == "__main__":
    sync_headers()
