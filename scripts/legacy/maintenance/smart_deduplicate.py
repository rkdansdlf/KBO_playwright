import logging

logger = logging.getLogger(__name__)

from pathlib import Path

from src.services.game_deduplication_service import mark_primary_games

DB_PATH = Path("data/kbo_dev.db")


def smart_deduplicate():
    logger.info("Resetting is_primary to 0...")
    result = mark_primary_games(DB_PATH, reset_all=True, remove_extreme_dates=True)
    logger.info(f"Analyzing {result.scanned_slots} unique game slots...")
    logger.info(f"Deduplication complete. {result.marked_primary} primary games marked.")


if __name__ == "__main__":
    smart_deduplicate()
