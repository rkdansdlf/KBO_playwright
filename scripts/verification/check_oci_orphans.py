import logging

import sqlalchemy
from sqlalchemy import text

logger = logging.getLogger(__name__)
def check_oci_orphans():
    engine = sqlalchemy.create_engine("postgresql://postgres:rkdansdlf@134.185.107.178:5432/bega_backend")
    with engine.connect() as conn:
        logger.info("=== OCI Orphan Data Check ===\n")

        # Pitching orphans
        pitching_query = text("""
            SELECT COUNT(DISTINCT player_id) FROM player_season_pitching
            WHERE player_id NOT IN (SELECT player_id FROM player_basic)
            AND player_id IS NOT NULL
        """)
        pitching_orphans = conn.execute(pitching_query).scalar()
        logger.info(f"Pitching orphans: {pitching_orphans}")

        # Batting orphans
        batting_query = text("""
            SELECT COUNT(DISTINCT player_id) FROM player_season_batting
            WHERE player_id NOT IN (SELECT player_id FROM player_basic)
            AND player_id IS NOT NULL
        """)
        batting_orphans = conn.execute(batting_query).scalar()
        logger.info(f"Batting orphans: {batting_orphans}")

        # Team orphans (Game)
        home_team_query = text("""
            SELECT COUNT(*) FROM game g LEFT JOIN teams t ON g.home_team = t.team_id WHERE t.team_id IS NULL AND g.home_team IS NOT NULL
        """)
        home_team_orphans = conn.execute(home_team_query).scalar()
        logger.info(f"Game home_team orphans: {home_team_orphans}")

        away_team_query = text("""
            SELECT COUNT(*) FROM game g LEFT JOIN teams t ON g.away_team = t.team_id WHERE t.team_id IS NULL AND g.away_team IS NOT NULL
        """)
        away_team_orphans = conn.execute(away_team_query).scalar()
        logger.info(f"Game away_team orphans: {away_team_orphans}")

        # Game Metadata orphans
        metadata_query = text("""
            SELECT COUNT(*) FROM game_metadata m LEFT JOIN game g ON m.game_id = g.game_id WHERE g.game_id IS NULL
        """)
        metadata_orphans = conn.execute(metadata_query).scalar()
        logger.info(f"Game metadata orphans: {metadata_orphans}")

        # Franchise count
        franchise_count_query = text("SELECT COUNT(*) FROM team_franchises")
        franchise_count = conn.execute(franchise_count_query).scalar()
        logger.info(f"Team Franchises: {franchise_count}")


if __name__ == "__main__":
    check_oci_orphans()
