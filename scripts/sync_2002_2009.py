
import os
import sys
from dotenv import load_dotenv

sys.path.insert(0, os.getcwd())
from src.db.engine import SessionLocal
from src.sync.oci_sync import OCISync
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching

def sync_2002_2009():
    load_dotenv()
    url = os.getenv("OCI_DB_URL")
    if not url:
        print("OCI_DB_URL not found")
        return

    years = list(range(2002, 2010))
    filters_batting = [PlayerSeasonBatting.season.in_(years)]
    filters_pitching = [PlayerSeasonPitching.season.in_(years)]

    with SessionLocal() as session:
        syncer = OCISync(url, session)
        
        print(f"🚀 Syncing 2002-2009 PlayerSeasonBatting...")
        syncer._sync_simple_table(
            PlayerSeasonBatting,
            ['player_id', 'season', 'league', 'level'],
            exclude_cols=['created_at', 'updated_at'],
            filters=filters_batting
        )
        
        print(f"🚀 Syncing 2002-2009 PlayerSeasonPitching...")
        syncer._sync_simple_table(
            PlayerSeasonPitching,
            ['player_id', 'season', 'league', 'level'],
            exclude_cols=['created_at', 'updated_at'],
            filters=filters_pitching
        )
        print("✅ Finished syncing 2002-2009 stats")

if __name__ == "__main__":
    sync_2002_2009()
