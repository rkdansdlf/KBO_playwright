import logging

logger = logging.getLogger(__name__)

import asyncio
import json
import os
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.services.game_collection_service import crawl_and_save_game_details


async def recover_historical_games(json_file: str, max_concurrency: int = 5):
    if not os.path.exists(json_file):
        logger.error(f"❌ File not found: {json_file}")
        return

    with open(json_file) as f:
        game_list = json.load(f)

    # Sort games by date to process chronologically
    game_list.sort(key=lambda x: x.get("game_date", ""))

    total_games = len(game_list)
    logger.info(f"🚀 Starting recovery for {total_games} games...")

    from src.db.engine import SessionLocal
    from src.services.player_id_resolver import PlayerIdResolver

    # Create session and resolver
    session = SessionLocal()
    resolver = PlayerIdResolver(session)

    # Preload seasons to avoid N+1 queries
    years = set()
    for g in game_list:
        d = str(g.get("game_date", ""))
        if len(d) >= 4:
            try:
                years.add(int(d[:4]))
            except Exception:  # noqa: BLE001
                pass

    logger.info(f"🔄 Preloading player data for {len(years)} seasons: {sorted(list(years))}")
    for year in sorted(list(years)):
        resolver.preload_season_index(year)

    # Pass resolver to crawler
    crawler = GameDetailCrawler(resolver=resolver)

    tasks = []
    for g in game_list:
        tasks.append({"game_id": g["game_id"], "game_date": g["game_date"]})

    # Process in batches
    batch_size = 50
    success_count = 0
    fail_count = 0

    logger.info(f"🚀 Processing in batches of {batch_size}...")

    for i in range(0, total_games, batch_size):
        batch = tasks[i : i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (total_games + batch_size - 1) // batch_size

        logger.info(f"📦 Batch {batch_num}/{total_batches}: Processing {len(batch)} games...")

        try:
            result = await crawl_and_save_game_details(
                batch,
                detail_crawler=crawler,
                force=True,
                concurrency=max_concurrency,
                log=print,
            )
            saved_in_batch = result.detail_saved
            success_count += result.detail_saved
            fail_count += result.detail_failed
            logger.info(f"   ✅ Saved {saved_in_batch} games in this batch.")

        except Exception as e:  # noqa: BLE001
            logger.error(f"❌ Batch {batch_num} failed: {e}")

    session.close()

    logger.info("\n✅ Recovery Summary:")
    logger.info(f"   Total Games: {total_games}")
    logger.info(f"   Successfully Saved: {success_count}")
    logger.info(f"   Failed: {fail_count}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--file", default="data/historical_game_ids.json")
    parser.add_argument("--concurrency", type=int, default=5)
    args = parser.parse_args()

    asyncio.run(recover_historical_games(args.file, args.concurrency))
