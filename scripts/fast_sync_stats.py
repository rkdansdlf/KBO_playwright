import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path.cwd()))
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from src.sync.oci_sync import OCISync


def fast_sync_stats():
    load_dotenv()
    url = os.getenv("OCI_DB_URL")
    if not url:
        logger.error("OCI_DB_URL not found")
        return

    with SessionLocal() as session:
        syncer = OCISync(url, session)

        logger.info("🚀 Fast Syncing PlayerSeasonBatting...")
        syncer.sync_simple_table(
            PlayerSeasonBatting, ["player_id", "season", "league", "level"], exclude_cols=["created_at", "updated_at"]
        )

        logger.info("🚀 Fast Syncing PlayerSeasonPitching...")
        syncer.sync_simple_table(
            PlayerSeasonPitching, ["player_id", "season", "league", "level"], exclude_cols=["created_at", "updated_at"]
        )
        logger.info("✅ Finished fast sync of stats")


if __name__ == "__main__":
    fast_sync_stats()
