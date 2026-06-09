import logging
import os
import sys

# Add project root to sys.path
sys.path.append(os.getcwd())

from sqlalchemy import text

from src.db.engine import SessionLocal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def repair_logical_errors():
    session = SessionLocal()
    try:
        # 1. Batting: AB > PA
        logger.info("Checking for Batting logical errors (AB > PA)...")
        # game_batting_stats
        res = session.execute(
            text("""
            UPDATE game_batting_stats
            SET plate_appearances = at_bats + COALESCE(walks, 0) + COALESCE(hbp, 0) +
                                   COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
            WHERE at_bats > plate_appearances
        """)
        )
        logger.info(f"Fixed {res.rowcount} game_batting_stats rows.")

        # player_season_batting
        res = session.execute(
            text("""
            UPDATE player_season_batting
            SET plate_appearances = at_bats + COALESCE(walks, 0) + COALESCE(hbp, 0) +
                                   COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
            WHERE at_bats > plate_appearances
        """)
        )
        logger.info(f"Fixed {res.rowcount} player_season_batting rows.")

        # 2. Pitching: ER > R
        logger.info("Checking for Pitching logical errors (ER > R)...")
        # game_pitching_stats
        res = session.execute(
            text("""
            UPDATE game_pitching_stats
            SET earned_runs = runs_allowed
            WHERE earned_runs > runs_allowed
        """)
        )
        logger.info(f"Fixed {res.rowcount} game_pitching_stats rows.")

        # player_season_pitching
        res = session.execute(
            text("""
            UPDATE player_season_pitching
            SET earned_runs = runs_allowed
            WHERE earned_runs > runs_allowed
        """)
        )
        logger.info(f"Fixed {res.rowcount} player_season_pitching rows.")

        session.commit()
        logger.info("Logical error repair complete.")

    except Exception as e:  # noqa: BLE001
        session.rollback()
        logger.error(f"Error repairing logical errors: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    repair_logical_errors()
