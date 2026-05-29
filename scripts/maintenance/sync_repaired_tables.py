import logging
import os
import sys

# Add project root to sys.path
sys.path.append(os.getcwd())

from src.db.engine import SessionLocal
from src.sync.oci_sync import OCISync

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def sync_repaired_tables():
    with SessionLocal() as session:
        syncer = OCISync(os.environ["OCI_DB_URL"], session)

        # logger.info("🚀 Syncing repaired game_batting_stats to OCI...")
        # from src.models.game import GameBattingStat
        # syncer._sync_simple_table(GameBattingStat, ['game_id', 'player_id', 'appearance_seq'], batch_size=10000)

        # logger.info("🚀 Syncing repaired game_pitching_stats to OCI...")
        # from src.models.game import GamePitchingStat
        # syncer._sync_simple_table(GamePitchingStat, ['game_id', 'player_id', 'appearance_seq'], batch_size=10000)

        logger.info("🚀 Syncing repaired game_lineups to OCI...")
        from src.models.game import GameLineup

        synced = syncer._sync_simple_table(GameLineup, ["game_id", "player_id", "appearance_seq"], batch_size=10000)

        logger.info(f"✅ Finished syncing {synced} game_lineups rows.")


if __name__ == "__main__":
    sync_repaired_tables()
