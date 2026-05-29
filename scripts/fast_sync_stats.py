import os
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.getcwd())
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from src.sync.oci_sync import OCISync


def fast_sync_stats():
    load_dotenv()
    url = os.getenv("OCI_DB_URL")
    if not url:
        print("OCI_DB_URL not found")
        return

    with SessionLocal() as session:
        syncer = OCISync(url, session)

        print("🚀 Fast Syncing PlayerSeasonBatting...")
        syncer._sync_simple_table(
            PlayerSeasonBatting, ["player_id", "season", "league", "level"], exclude_cols=["created_at", "updated_at"]
        )

        print("🚀 Fast Syncing PlayerSeasonPitching...")
        syncer._sync_simple_table(
            PlayerSeasonPitching, ["player_id", "season", "league", "level"], exclude_cols=["created_at", "updated_at"]
        )
        print("✅ Finished fast sync of stats")


if __name__ == "__main__":
    fast_sync_stats()
