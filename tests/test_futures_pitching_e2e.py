"""End-to-end test: Fetch Futures pitching stats and save to database."""

import asyncio
import logging

from src.crawlers.futures.futures_pitching import fetch_and_parse_futures_pitching
from src.repositories.player_repository import PlayerRepository
from src.repositories.player_season_pitching_repository import save_pitching_stats_to_db

logger = logging.getLogger(__name__)

PLAYER_ID = "50030"  # KBO player ID (string) for So Hyeong-jun (pitcher)
PLAYER_URL = f"https://www.koreabaseball.com/Futures/Player/PitcherTotal.aspx?playerId={PLAYER_ID}"


async def main():
    logger.info("=== Futures Pitching E2E Test ===\n")

    # Step 1: Crawl and parse
    logger.info("Step 1: Crawling Futures pitching stats for player %s...", PLAYER_ID)
    rows = await fetch_and_parse_futures_pitching(PLAYER_ID, PLAYER_URL)
    logger.info("Parsed %d season records\n", len(rows))

    if not rows:
        logger.info("No data to save. Exiting.")
        return

    # Show sample
    for row in rows[:3]:
        logger.info(
            "  %s: ERA=%s, G=%s, W=%s, L=%s, IP=%s",
            row.get("season"),
            row.get("era"),
            row.get("games"),
            row.get("wins"),
            row.get("losses"),
            row.get("innings_pitched"),
        )
    logger.info("")

    # Step 2: Get or create player in database
    logger.info("Step 2: Ensuring player %s exists in database...", PLAYER_ID)
    repo = PlayerRepository()

    from src.parsers.player_profile_parser import PlayerProfileParsed

    player = repo.upsert_player_profile(PLAYER_ID, PlayerProfileParsed(is_active=True, player_name="소형준"))

    if not player:
        logger.info("Failed to create player record")
        return

    logger.info("Player DB ID: %s, Player Basic ID: %s\n", player.id, player.player_basic_id)

    # Step 3: Save Futures pitching stats
    logger.info("Step 3: Saving %d Futures pitching records to database...", len(rows))
    payloads = []
    for r in rows:
        payloads.append(
            {
                "player_id": int(player.player_basic_id),
                "player_name": "소형준",
                "season": r.get("season"),
                "league": "FUTURES",
                "level": "KBO2",
                "source": "PROFILE",
                "team_code": r.get("team_code"),
                "games": r.get("games"),
                "complete_games": r.get("complete_games"),
                "shutouts": r.get("shutouts"),
                "wins": r.get("wins"),
                "losses": r.get("losses"),
                "saves": r.get("saves"),
                "holds": r.get("holds"),
                "innings_pitched": r.get("innings_pitched"),
                "innings_outs": r.get("innings_outs"),
                "hits_allowed": r.get("hits_allowed"),
                "runs_allowed": r.get("runs_allowed"),
                "earned_runs": r.get("earned_runs"),
                "home_runs_allowed": r.get("home_runs_allowed"),
                "walks_allowed": r.get("walks_allowed"),
                "hit_batters": r.get("hit_batters"),
                "strikeouts": r.get("strikeouts"),
                "era": r.get("era"),
                "tbf": r.get("tbf"),
            },
        )

    saved = save_pitching_stats_to_db(payloads)
    logger.info("Saved %d records\n", saved)

    # Step 4: Verify
    logger.info("Step 4: Verifying records in database...")
    from sqlalchemy import select

    from src.db.engine import SessionLocal
    from src.models.player import PlayerSeasonPitching

    with SessionLocal() as session:
        stmt = (
            select(PlayerSeasonPitching)
            .where(PlayerSeasonPitching.player_id == player.player_basic_id, PlayerSeasonPitching.league == "FUTURES")
            .order_by(PlayerSeasonPitching.season)
        )

        results = session.execute(stmt).scalars().all()

        logger.info("Found %d Futures pitching records in database:", len(results))
        for record in results:
            logger.info(
                "  %s: ERA=%s, G=%s, W=%s, L=%s, IP=%s",
                record.season,
                record.era,
                record.games,
                record.wins,
                record.losses,
                record.innings_pitched,
            )

    logger.info("\n=== Test Complete ===")


if __name__ == "__main__":
    asyncio.run(main())
