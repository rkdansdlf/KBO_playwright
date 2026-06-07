import argparse
import asyncio
import logging
import os
import sys

# Add project root to path
sys.path.append(os.getcwd())

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.schedule_crawler import ScheduleCrawler
from src.db.engine import SessionLocal
from src.repositories.game_repository import Game
from src.services.game_collection_service import crawl_and_save_game_details
from src.services.schedule_collection_service import save_schedule_games
from src.utils.series_validation import get_available_series_by_year

logger = logging.getLogger(__name__)


async def backfill_year(year: int, series_list: list[str] = None):
    """
    Backfill game data for a single year.
    """
    logger.info("\n" + "=" * 60)
    logger.info("Starting Backfill for Year: %d", year)
    logger.info("=" * 60)

    if not series_list:
        series_list = get_available_series_by_year(year)
        # Exclude exhibition for now to save time/resources, focus on regular and postseason
        series_list = [s for s in series_list if s != "exhibition"]

    schedule_crawler = ScheduleCrawler(request_delay=1.0)
    detail_crawler = GameDetailCrawler(request_delay=1.5)

    all_game_ids = []

    # Step 1: Collect Schedules
    logger.info("Phase 1: Collecting Schedules for %d...", year)

    # Map internal series names to KBO Series IDs (based on 2010 debug)
    # 0,9,6: Regular Season
    # 3,4,5,7: Postseason (Korean Series, Playoff, Semi-playoff, Wildcard)
    # 1: Exhibition

    kbo_series_ids = []
    if any(s == "regular" for s in series_list):
        kbo_series_ids.append("0,9,6")

    if any(s in ["korean_series", "playoff", "semi_playoff", "wildcard"] for s in series_list):
        kbo_series_ids.append("3,4,5,7")

    # Remove duplicates
    kbo_series_ids = list(set(kbo_series_ids))

    for sid in kbo_series_ids:
        try:
            logger.info("Crawling Series ID: %s...", sid)
            games = await schedule_crawler.crawl_season(year, months=list(range(3, 12)), series_id=sid)
            logger.info("Found %d games for Series %s", len(games), sid)

            result = save_schedule_games(games, log=logger.info)
            if result.failed:
                logger.warning("Schedule save failed for %d rows", result.failed)
            for g in result.saved_games:
                all_game_ids.append((g["game_id"], g["game_date"]))
        except Exception as e:
            logger.error("Error collecting schedule for %d (Series %s): %s", year, sid, e)

    # Remove duplicates from all_game_ids
    unique_games = []
    seen_ids = set()
    for gid, gdate in all_game_ids:
        if gid not in seen_ids:
            unique_games.append({"game_id": gid, "game_date": gdate})
            seen_ids.add(gid)

    logger.info("Total unique games to scrape: %d", len(unique_games))

    # Step 2: Scrape Details
    logger.info("Phase 2: Scraping Game Details for %d...", year)

    # Filter out already completed games if needed, but for backfill we usually want all
    # For efficiency, we use crawl_games which handles concurrency
    success_count = 0

    # Process in smaller chunks to avoid memory/connection issues
    chunk_size = 20
    for i in range(0, len(unique_games), chunk_size):
        chunk = unique_games[i : i + chunk_size]
        logger.info(
            "Processing chunk %d/%d (%d games)...",
            i // chunk_size + 1,
            (len(unique_games) - 1) // chunk_size + 1,
            len(chunk),
        )

        pending_chunk = []
        for game in chunk:
            gid = game["game_id"]

            # Check if already has scores (skip if already done)
            with SessionLocal() as session:
                existing = session.query(Game).filter(Game.game_id == gid).one_or_none()
                if existing and existing.home_score is not None:
                    success_count += 1
                    continue
            pending_chunk.append(game)

        if pending_chunk:
            result = await crawl_and_save_game_details(
                pending_chunk,
                detail_crawler=detail_crawler,
                force=True,
                concurrency=1,
                log=logger.info,
            )
            success_count += result.detail_saved
            for item in result.items.values():
                if not item.detail_saved:
                    logger.warning("Failed to crawl/save %s (reason=%s)", item.game_id, item.failure_reason or 'unknown')

    logger.info("Finished %d: %d/%d games completed.", year, success_count, len(unique_games))


async def main():
    parser = argparse.ArgumentParser(description="Backfill KBO game data for 2010-2017")
    parser.add_argument("--year", type=int, help="Specific year to backfill")
    parser.add_argument("--start", type=int, default=2010, help="Start year (default: 2010)")
    parser.add_argument("--end", type=int, default=2017, help="End year (default: 2017)")

    args = parser.parse_args()

    if args.year:
        years = [args.year]
    else:
        years = list(range(args.start, args.end + 1))

    logger.info("Starting Historical Backfill for range: %s", years)

    for year in years:
        await backfill_year(year)
        # Optional sleep between years
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
