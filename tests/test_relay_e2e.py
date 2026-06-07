"""
End-to-end test for RELAY crawler.
Tests fetching and saving play-by-play data.
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio

from src.crawlers.relay_crawler import fetch_and_parse_relay
from src.repositories.relay_repository import get_game_relay_summary, save_relay_data

logger = logging.getLogger(__name__)


async def main():
    logger.info("\n=== RELAY Crawler E2E Test ===\n")

    # Use a recent game with completed data
    game_id = "20251013SKSS0"
    game_date = "20251013"

    # Step 1: Crawl RELAY section
    logger.info("Step 1: Crawling RELAY for game %s...", game_id)
    relay_data = await fetch_and_parse_relay(game_id, game_date)

    if not relay_data:
        logger.info("No RELAY data returned. Test failed.")
        return

    innings = relay_data.get("innings", [])
    logger.info("Parsed %d innings\n", len(innings))

    # Show sample
    if innings:
        logger.info("Sample inning data:")
        sample = innings[0]
        logger.info("  Inning %s %s", sample['inning'], sample['half'])
        logger.info("  Plays: %d", len(sample['plays']))
        if sample["plays"]:
            logger.info("  First play: %s...", sample['plays'][0].get('description', '')[:50])
    logger.info("")

    # Step 2: Save to database
    logger.info("Step 2: Saving RELAY data to database...")
    saved = save_relay_data(game_id, innings)
    logger.info("Saved %d plays\n", saved)

    # Step 3: Verify
    logger.info("Step 3: Verifying saved data...")
    summary = get_game_relay_summary(game_id)

    logger.info("Game: %s", summary['game_id'])
    logger.info("Total plays: %s", summary['total_plays'])
    logger.info("Innings recorded: %s", summary['innings'])
    logger.info("\nEvent types:")
    for event_type, count in summary["event_types"].items():
        if count > 0:
            logger.info("  %s: %d", event_type, count)

    logger.info("\n=== Test Complete ===")


if __name__ == "__main__":
    asyncio.run(main())
