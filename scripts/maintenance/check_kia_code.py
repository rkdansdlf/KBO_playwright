
"""Manual live debug probe for historical team-code parsing.

This script intentionally calls `GameDetailCrawler` directly and does not save
to the database. Use `src.cli.collect_games` or `src.cli.run_daily_update` for
operational detail collection.
"""
import asyncio
import os
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.services.player_id_resolver import PlayerIdResolver
from src.db.engine import SessionLocal

async def check_kia_code():
    print(
        "[DEBUG] scripts/maintenance/check_kia_code.py performs a live parser probe only. "
        "It does not persist data."
    )
    session = SessionLocal()
    pool = AsyncPlaywrightPool(max_pages=1)
    
    try:
        resolver = PlayerIdResolver(session)
        crawler = GameDetailCrawler(pool=pool, resolver=resolver)
        await pool.start()
        
        # 2024.03.24 Kiwoom vs KIA
        # Try both WOHT0 and WOKIA0
        
        test_ids = ["20240323HHLG0"]
        game_date = "20240323"
        
        for game_id in test_ids:
            print(f"\n🔍 Testing Game ID: {game_id}")
            try:
                data = await crawler.crawl_game(game_id, game_date)
                if data and data['hitters']['away']:
                    print(f"✅ SUCCESS for {game_id}! Found {len(data['hitters']['away'])} away hitters.")
                    # Print first player to confirm
                    print(f"   First Player: {data['hitters']['away'][0]['player_name']}")
                else:
                    print(f"❌ FAILED for {game_id} (No data/players)")
            except Exception as e:
                print(f"❌ ERROR for {game_id}: {e}")
                
    finally:
        session.close()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(check_kia_code())
