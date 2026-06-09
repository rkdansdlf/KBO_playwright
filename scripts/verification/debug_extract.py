import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import logging  # noqa: E402

from sqlalchemy import text  # noqa: E402

from src.db.engine import SessionLocal  # noqa: E402

logger = logging.getLogger(__name__)
def main():
    session = SessionLocal()
    logger.info("\n--- 20250308HTLT0 Game Batting Stats in DB ---")
    rows = session.execute(
        text("""
        SELECT player_name, at_bats, hits, strikeouts, walks, hbp, plate_appearances
        FROM game_batting_stats
        WHERE game_id = '20250308HTLT0'
    """)
    ).fetchall()
    for row in rows:
        logger.info(f"Name: {row[0]}, AB: {row[1]}, H: {row[2]}, SO: {row[3]}, BB: {row[4]}, HBP: {row[5]}, PA: {row[6]}")

    session.close()


if __name__ == "__main__":
    main()
