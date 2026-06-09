"""Manual live debug probe for cancelled-game parser behavior.

This file is named like a test but is an executable debug script. It does not
save to the database. Use standard CLIs for operational collection.
"""

import logging

logger = logging.getLogger(__name__)

import asyncio
import os
import sys

# Add project root to path
sys.path.insert(0, os.getcwd())

from src.crawlers.game_detail_crawler import GameDetailCrawler


async def main():
    logger.info(
        "[DEBUG] scripts/maintenance/test_cancel_detect.py performs a live parser probe only. It does not persist data."
    )
    crawler = GameDetailCrawler()
    # Mock some basic setup if needed

    game_id = "20140611SSNX0"
    game_date = "20140611"

    logger.info(f"📡 Testing cancelled game: {game_id}")

    # We need to init the crawler (launch browser)
    # Actually crawl_game does it
    data = await crawler.crawl_game(game_id, game_date)

    if data:
        logger.info(f"✅ Successfully crawled {game_id}")
        logger.info(f"Metadata: {data.get('metadata')}")
    else:
        logger.error(f"❌ Failed to crawl {game_id} (Returned None - Correct for cancelled?)")


if __name__ == "__main__":
    asyncio.run(main())
