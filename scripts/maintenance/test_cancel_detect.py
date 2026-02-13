import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, os.getcwd())

from src.crawlers.game_detail_crawler import GameDetailCrawler

async def main():
    crawler = GameDetailCrawler()
    # Mock some basic setup if needed
    
    game_id = '20140611SSNX0'
    game_date = '20140611'
    
    print(f"📡 Testing cancelled game: {game_id}")
    
    # We need to init the crawler (launch browser)
    # Actually crawl_game does it
    data = await crawler.crawl_game(game_id, game_date)
    
    if data:
        print(f"✅ Successfully crawled {game_id}")
        print(f"Metadata: {data.get('metadata')}")
    else:
        print(f"❌ Failed to crawl {game_id} (Returned None - Correct for cancelled?)")

if __name__ == "__main__":
    asyncio.run(main())
