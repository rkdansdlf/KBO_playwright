"""
Integrated script: Crawl and save to database
"""
import asyncio
from datetime import datetime

from src.crawlers.schedule_crawler import ScheduleCrawler
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.repositories.game_repository import GameRepository


async def crawl_and_save_schedule(year: int, month: int):
    """Crawl schedule and save to database"""
    print("\n" + "=" * 60)
    print("STEP 1: Crawl and Save Schedule")
    print("=" * 60)

    crawler = ScheduleCrawler()
    repo = GameRepository()

    # Crawl schedule
    games = await crawler.crawl_schedule(year, month)

    if games:
        # Save to database
        saved_count = repo.save_schedules(games)
        print(f"✅ Saved {saved_count} games to database")
        return games
    else:
        print("❌ No games found")
        return []


async def crawl_and_save_game_detail(game_id: str, game_date: str):
    """Crawl game detail and save to database"""
    print("\n" + "=" * 60)
    print(f"STEP 2: Crawl and Save Game Detail ({game_id})")
    print("=" * 60)

    crawler = GameDetailCrawler()
    repo = GameRepository()

    # Update status to 'ready'
    repo.update_crawl_status(game_id, 'ready')

    # Crawl game detail
    game_data = await crawler.crawl_game(game_id, game_date)

    if game_data:
        # Save to database
        success = repo.save_game_detail(game_data)

        if success:
            repo.update_crawl_status(game_id, 'crawled')
            print(f"✅ Successfully crawled and saved game: {game_id}")
            return game_data
        else:
            repo.update_crawl_status(game_id, 'failed', 'Error saving to database')
            print(f"❌ Failed to save game: {game_id}")
            return None
    else:
        repo.update_crawl_status(game_id, 'failed', 'Error crawling data')
        print(f"❌ Failed to crawl game: {game_id}")
        return None


async def main():
    """Main integrated workflow"""
    print("\n" + "🚀" * 30)
    print("KBO Crawler - Database Integration")
    print("🚀" * 30)

    # Step 1: Crawl schedule
    year = 2024
    month = 10
    games = await crawl_and_save_schedule(year, month)

    if not games:
        print("\n❌ No games to process")
        return

    # Step 2: Crawl first game detail
    first_game = games[0]
    print(f"\n💡 Processing first game: {first_game['game_id']}")

    await crawl_and_save_game_detail(
        game_id=first_game['game_id'],
        game_date=first_game['game_date']
    )

    # Step 3: Verify database
    print("\n" + "=" * 60)
    print("STEP 3: Verify Database")
    print("=" * 60)

    repo = GameRepository()
    pending_games = repo.get_pending_games(limit=5)

    print(f"\n📊 Database Status:")
    print(f"  Pending games: {len(pending_games)}")

    if pending_games:
        print(f"\n  Next games to crawl:")
        for game in pending_games[:3]:
            print(f"    - {game['game_id']} ({game['game_date']})")

    print("\n" + "✅" * 30)
    print("Integration Test Complete!")
    print("✅" * 30)


if __name__ == "__main__":
    asyncio.run(main())
