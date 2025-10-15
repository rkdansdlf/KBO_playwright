"""
POC Test Script
Tests basic crawling functionality and saves results to JSON
"""
import asyncio
import json
import os
from pathlib import Path

import pytest

from src.crawlers.schedule_crawler import ScheduleCrawler
from src.crawlers.game_detail_crawler import GameDetailCrawler


pytestmark = pytest.mark.skipif(
    os.getenv("ENABLE_PLAYWRIGHT_TESTS") != "1",
    reason="Playwright-dependent integration tests skipped (set ENABLE_PLAYWRIGHT_TESTS=1 to run)",
)


@pytest.mark.asyncio
async def test_schedule_crawler():
    """Test schedule crawler"""
    print("=" * 60)
    print("TEST 1: Schedule Crawler")
    print("=" * 60)

    crawler = ScheduleCrawler()

    # Get October 2024 schedule (or current month)
    year = 2024
    month = 10

    games = await crawler.crawl_schedule(year, month)

    # Save to JSON
    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / f"schedule_{year}_{month:02d}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(games, f, ensure_ascii=False, indent=2)

    print(f"\n💾 Saved schedule to: {output_file}")
    print(f"📊 Total games: {len(games)}")

    return games


@pytest.mark.asyncio
async def test_game_detail_crawler(game_id: str = None, game_date: str = None):
    """Test game detail crawler"""
    print("\n" + "=" * 60)
    print("TEST 2: Game Detail Crawler")
    print("=" * 60)

    crawler = GameDetailCrawler()

    # Use provided game or default to a test game
    if not game_id:
        # Try to get a recent game from October 2024
        game_id = "20241013SKNC0"  # Example game
        game_date = "20241013"

    print(f"\n Testing with game_id: {game_id}")

    game_data = await crawler.crawl_game(game_id, game_date)

    if game_data:
        # Save to JSON
        output_dir = Path("data")
        output_dir.mkdir(exist_ok=True)

        output_file = output_dir / f"game_{game_id}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(game_data, f, ensure_ascii=False, indent=2)

        print(f"\n💾 Saved game data to: {output_file}")
        print(f"\n Game Data Summary:")
        print(f"  Stadium: {game_data['metadata'].get('stadium', 'N/A')}")
        print(f"  Attendance: {game_data['metadata'].get('attendance', 'N/A')}")
        print(f"  Away Hitters: {len(game_data['hitters']['away'])}")
        print(f"  Home Hitters: {len(game_data['hitters']['home'])}")
        print(f"  Away Pitchers: {len(game_data['pitchers']['away'])}")
        print(f"  Home Pitchers: {len(game_data['pitchers']['home'])}")

        return game_data
    else:
        print(f"\n❌ Failed to crawl game: {game_id}")
        return None


async def main():
    """Run POC tests"""
    print("\n" + "=" * 30)
    print("KBO Crawler POC Test")
    print("=" * 30 + "\n")

    # Test 1: Schedule Crawler
    try:
        games = await test_schedule_crawler()

        # Test 2: Game Detail Crawler
        # If we got games from schedule, use the first one
        if games:
            print(f"\n💡 Using first game from schedule: {games[0]['game_id']}")
            await test_game_detail_crawler(
                game_id=games[0]['game_id'],
                game_date=games[0]['game_date']
            )
        else:
            # Fallback to default test
            await test_game_detail_crawler()

    except Exception as e:
        print(f"\n❌ POC Test failed: {e}")
        import traceback
        traceback.print_exc()

    print("\n" + "=" * 30)
    print("POC Test Complete!")
    print("=" * 30 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
