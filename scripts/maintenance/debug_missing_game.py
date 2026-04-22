"""Manual live debug probe for one missing historical game.

This script intentionally calls `GameDetailCrawler` directly and does not save
to the database. Use `src.cli.collect_games` or `src.cli.run_daily_update` for
operational detail collection.
"""
import asyncio
import sys
import os

# Add the project root to sys.path
sys.path.insert(0, os.getcwd())

from src.crawlers.game_detail_crawler import GameDetailCrawler

async def main():
    print(
        "[DEBUG] scripts/maintenance/debug_missing_game.py performs a live parser probe only. "
        "It does not persist data."
    )
    crawler = GameDetailCrawler()
    # crawler.init()  <-- This was causing the error
    try:
        # Try a specifically missing game
        game_id = '20100414OBHT0'
        game_date = '20100414'
        print(f"📡 Crawling {game_id}...")
        data = await crawler.crawl_game(game_id, game_date)
        if data:
            print(f"✅ Successfully crawled {game_id}")
            teams = data.get('teams', {})
            metadata = data.get('metadata', {})
            print(f"Stadium: {metadata.get('stadium')}")
            print(f"Away: {teams.get('away', {}).get('name')} ({teams.get('away', {}).get('score')}) [Code: {teams.get('away', {}).get('code')}]")
            print(f"Home: {teams.get('home', {}).get('name')} ({teams.get('home', {}).get('score')}) [Code: {teams.get('home', {}).get('code')}]")
        else:
            print(f"❌ Failed to crawl {game_id}")
    except Exception as e:
        print(f"💥 Error: {e}")
    # No crawler.close() needed here as pool closes in crawl_games if owned


if __name__ == "__main__":
    asyncio.run(main())
