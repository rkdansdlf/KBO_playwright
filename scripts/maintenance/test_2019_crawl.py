
import asyncio
import os
import sys
import pytest

# Add project root to path
sys.path.append(os.getcwd())

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.db.engine import SessionLocal

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_LIVE_DEBUG_TESTS") != "1",
    reason="Live debug crawler test is disabled unless RUN_LIVE_DEBUG_TESTS=1",
)

async def test_single_game():
    game_id = '20190323SSNC0' 
    game_date = '20190323'
    
    print(f"📡 Testing crawl for {game_id}...")
    crawler = GameDetailCrawler()
    try:
        data = await crawler.crawl_game(game_id, game_date)
        if data:
            print(f"✅ Crawled successfully!")
            print(f"🏠 Home Score: {data['teams']['home']['score']}")
            print(f"🚀 Away Score: {data['teams']['away']['score']}")
        else:
            print(f"❌ Crawl returned None")
    except Exception as e:
        print(f"💥 Error: {e}")
    finally:
        await crawler.close()

if __name__ == "__main__":
    asyncio.run(test_single_game())
