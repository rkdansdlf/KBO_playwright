"""Cleanup script to remove corrupted historical player records.
Targets records with unrealistic values (e.g., wins > 35, games > 165).
"""

import logging

from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching

logger = logging.getLogger(__name__)


def cleanup_corrupted_stats():
    with SessionLocal() as session:
        logger.info("🧹 Starting precision cleaning of historical data...")

        # 1. Pitching Cleanup
        p_corrupted = (
            session.query(PlayerSeasonPitching)
            .filter((PlayerSeasonPitching.wins > 35) | (PlayerSeasonPitching.games > 165))
            .all()
        )

        logger.info("   Found %s corrupted pitching records.", len(p_corrupted))
        for rec in p_corrupted:
            session.delete(rec)

        # 2. Batting Cleanup
        b_corrupted = (
            session.query(PlayerSeasonBatting)
            .filter((PlayerSeasonBatting.home_runs > 65) | (PlayerSeasonBatting.games > 165))
            .all()
        )

        logger.info("   Found %s corrupted batting records.", len(b_corrupted))
        for rec in b_corrupted:
            session.delete(rec)

        session.commit()
        logger.info("✅ Cleanup complete. Database is now purged of unrealistic outliers.")


if __name__ == "__main__":
    cleanup_corrupted_stats()
