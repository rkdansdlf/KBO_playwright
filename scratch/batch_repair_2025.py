
import asyncio
import sys
import os
from datetime import datetime

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.repositories.game_repository import save_game_detail
from src.db.engine import SessionLocal
from sqlalchemy import text

async def batch_repair_2025_parallel():
    session = SessionLocal()
    # Find 2025 games with no pitching records
    query = text("""
        SELECT game_id, game_date 
        FROM game g
        WHERE game_date LIKE '2025%'
        AND game_status NOT IN ('경기취소', '취소')
        AND (SELECT COUNT(*) FROM game_pitching_stats WHERE game_id = g.game_id) = 0
    """)
    targets = session.execute(query).all()
    session.close()

    if not targets:
        print("No games in 2025 found with missing pitching stats.")
        return

    print(f"Found {len(targets)} games in 2025 to repair (Parallel mode).")
    
    # Prepare games list for crawler
    games_to_crawl = []
    for game_id, game_date in targets:
        date_str = game_date.replace("-", "") if hasattr(game_date, "replace") else str(game_date).replace("-", "")
        games_to_crawl.append({"game_id": game_id, "game_date": date_str})

    crawler = GameDetailCrawler(request_delay=0.8) # Slightly more delay for parallel health
    
    # crawl_games handles internal concurrency (default 3)
    results = await crawler.crawl_games(games_to_crawl)
    
    success_count = 0
    for payload in results:
        if payload and payload.get('pitchers'):
            if any(payload['pitchers'].values()):
                if save_game_detail(payload):
                    # print(f"  ✅ Successfully repaired pitching stats for {payload['game_id']}")
                    success_count += 1
                else:
                    print(f"  ❌ Failed to save detail for {payload['game_id']}")

    print(f"\nRepair Complete: {success_count}/{len(targets)} games updated successfully.")

if __name__ == "__main__":
    asyncio.run(batch_repair_2025_parallel())
