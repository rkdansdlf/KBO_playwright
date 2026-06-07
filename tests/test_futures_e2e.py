"""
End-to-end test: Fetch Futures stats and save to database.
"""

import asyncio
import logging

from src.crawlers.futures.futures_batting import fetch_and_parse_futures_batting
from src.repositories.player_repository import PlayerRepository
from src.repositories.save_futures_batting import save_futures_batting

logger = logging.getLogger(__name__)

PLAYER_ID = "51868"  # KBO player ID (string)
PLAYER_URL = f"https://www.koreabaseball.com/Futures/Player/HitterTotal.aspx?playerId={PLAYER_ID}"


async def main():
    logger.info("=== Futures Batting E2E Test ===\n")

    # Step 1: Crawl and parse
    logger.info("Step 1: Crawling Futures stats for player %s...", PLAYER_ID)
    rows = await fetch_and_parse_futures_batting(PLAYER_ID, PLAYER_URL)
    logger.info("Parsed %d season records\n", len(rows))

    if not rows:
        logger.info("No data to save. Exiting.")
        return

    # Show sample
    for row in rows[:3]:
        logger.info("  %s: AVG=%s, G=%s, H=%s, HR=%s", row.get('season'), row.get('AVG'), row.get('G'), row.get('H'), row.get('HR'))
    logger.info("")

    # Step 2: Get or create player in database
    logger.info("Step 2: Ensuring player %s exists in database...", PLAYER_ID)
    repo = PlayerRepository()

    # Try to get existing player
    from src.parsers.player_profile_parser import PlayerProfileParsed

    player = repo.upsert_player_profile(PLAYER_ID, PlayerProfileParsed(is_active=True, player_name="고명준"))

    if not player:
        logger.info("Failed to create player record")
        return

    logger.info("Player DB ID: %s\n", player.id)

    # Step 3: Save Futures stats
    logger.info("Step 3: Saving %d Futures records to database...", len(rows))
    saved = save_futures_batting(player_id_db=player.player_basic_id, rows=rows)
    logger.info("Saved %d records\n", saved)

    # Step 4: Verify
    logger.info("Step 4: Verifying records in database...")
    from sqlalchemy import select

    from src.db.engine import SessionLocal
    from src.models.player import PlayerSeasonBatting

    with SessionLocal() as session:
        stmt = (
            select(PlayerSeasonBatting)
            .where(PlayerSeasonBatting.player_id == player.player_basic_id, PlayerSeasonBatting.league == "FUTURES")
            .order_by(PlayerSeasonBatting.season)
        )

        results = session.execute(stmt).scalars().all()

        logger.info("Found %d Futures records in database:", len(results))
        for record in results:
            logger.info("  %s: AVG=%s, G=%s, H=%s, HR=%s", record.season, record.avg, record.games, record.hits, record.home_runs)

    logger.info("\n=== Test Complete ===")


if __name__ == "__main__":
    asyncio.run(main())
