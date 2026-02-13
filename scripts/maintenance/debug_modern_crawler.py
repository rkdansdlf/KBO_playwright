
import asyncio
import os
import json
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.services.player_id_resolver import PlayerIdResolver
from src.db.engine import SessionLocal

async def debug_modern_crawler():
    pool = AsyncPlaywrightPool(max_pages=1)
    session = SessionLocal()
    try:
        resolver = PlayerIdResolver(session)
        crawler = GameDetailCrawler(pool=pool, resolver=resolver)
    
        # 2024 Opening Day Game (Hanwha vs LG)
        game_id = "20240323HHLG0"
        game_date = "20240323"
        
        print(f"🔍 Debugging Game: {game_id}")
        
        await pool.start()
        
        data = await crawler.crawl_game(game_id, game_date)
        
        if data:
            print("\n--- AWAY HITTERS SAMPLE ---")
            for h in data['hitters']['away'][:5]:
                print(f"Player: {h['player_name']}")
                print(f"Player ID: {h['player_id']}")
                print(f"Uniform: {h['uniform_no']}")
                print(f"Stats: {h['stats']}")
                print("-" * 20)
                
            total_runs = sum(h['stats'].get('runs', 0) for h in data['hitters']['away'])
            print(f"\nTotal Away Runs from Stats: {total_runs}")
            print(f"Actual Away Score: {data['teams']['away']['score']}")
        else:
            print("❌ No data returned.")
            
    finally:
        session.close()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(debug_modern_crawler())
