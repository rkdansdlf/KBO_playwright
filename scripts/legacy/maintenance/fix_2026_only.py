import logging

logger = logging.getLogger(__name__)

from src.services.game_deduplication_service import DeduplicationWindow, mark_primary_games


def fix_2026_integrity():
    logger.info("Fixing 2026 Regular Season Integrity...")
    result = mark_primary_games(
        "data/kbo_dev.db",
        windows=[DeduplicationWindow("2026 regular", "2026-03-28", "2026-12-31", clear_year=2026)],
        reset_all=False,
    )
    logger.info(f"2026 primary flags calibrated. {result.marked_primary} primary games marked.")
    logger.info("Re-resolving player IDs for 2026 should run as a separate explicit repair step.")


if __name__ == "__main__":
    fix_2026_integrity()
