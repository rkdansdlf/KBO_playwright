import asyncio
import sys
import os
from datetime import datetime
import sqlite3

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.repositories.game_repository import save_game_detail
from src.db.engine import SessionLocal

async def repair_2025_stats():
    conn = sqlite3.connect("data/kbo_dev.db")
    cursor = conn.cursor()
    
    # 1. Identify 2025 primary games that are missing stats or have zeroed batting stats
    # Criteria: is_primary = 1 AND 2025 AND (no stats OR sum(hits) == 0)
    query = """
    SELECT g.game_id, g.game_date
    FROM game g
    WHERE strftime('%Y', g.game_date) = '2025'
      AND g.is_primary = 1
      AND g.game_status NOT IN ('경기취소', '취소', '우천취소')
      AND (
          (SELECT COUNT(*) FROM game_batting_stats b WHERE b.game_id = g.game_id) = 0
          OR 
          (SELECT SUM(hits) FROM game_batting_stats b WHERE b.game_id = g.game_id) = 0
      )
    """
    
    targets = cursor.execute(query).fetchall()
    conn.close()

    if not targets:
        print("No games in 2025 found needing repair.")
        return

    print(f"Found {len(targets)} games in 2025 to repair.")


    
    games_to_crawl = []
    for game_id, game_date in targets:
        date_str = str(game_date).replace("-", "")
        games_to_crawl.append({"game_id": game_id, "game_date": date_str})

    # To avoid overwhelming the server and handle large volume, we'll process in chunks
    chunk_size = 50
    crawler = GameDetailCrawler(request_delay=1.0)
    
    success_count = 0
    for i in range(0, len(games_to_crawl), chunk_size):
        chunk = games_to_crawl[i:i+chunk_size]
        print(f"Processing chunk {i//chunk_size + 1}/{(len(games_to_crawl)-1)//chunk_size + 1} ({len(chunk)} games)...")
        
        results = await crawler.crawl_games(chunk)
        
        for payload in results:
            if payload:
                # Check if we actually got stats in the payload
                has_hitters = payload.get('hitters') and any(payload['hitters'].values())
                if has_hitters:
                    if save_game_detail(payload):
                        success_count += 1
                    else:
                        print(f"  ❌ Failed to save detail for {payload['game_id']}")
                else:
                    print(f"  ⚠️ Crawford {payload['game_id']} but result still has no hitters.")

    print(f"\nRepair Complete: {success_count}/{len(targets)} games updated successfully.")

if __name__ == "__main__":
    asyncio.run(repair_2025_stats())
