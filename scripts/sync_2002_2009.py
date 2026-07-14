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
from src.sync.sync_base import SimpleTableSyncOptions


def sync_2002_2009():
    load_dotenv()
    url = os.getenv("OCI_DB_URL")
    if not url:
        logger.error("OCI_DB_URL not found")
        return

    years = list(range(2002, 2010))
    filters_batting = [PlayerSeasonBatting.season.in_(years)]
    filters_pitching = [PlayerSeasonPitching.season.in_(years)]

    with SessionLocal() as session:
        syncer = OCISync(url, session)

        logger.info("🚀 Syncing 2002-2009 PlayerSeasonBatting...")
        syncer.sync_simple_table(
            PlayerSeasonBatting,
            SimpleTableSyncOptions(
                conflict_keys=["player_id", "season", "league", "level"],
                exclude_cols=["created_at", "updated_at"],
                filters=filters_batting,
            ),
        )

        logger.info("🚀 Syncing 2002-2009 PlayerSeasonPitching...")
        syncer.sync_simple_table(
            PlayerSeasonPitching,
            SimpleTableSyncOptions(
                conflict_keys=["player_id", "season", "league", "level"],
                exclude_cols=["created_at", "updated_at"],
                filters=filters_pitching,
            ),
        )
        logger.info("✅ Finished syncing 2002-2009 stats")


if __name__ == "__main__":
    sync_2002_2009()
