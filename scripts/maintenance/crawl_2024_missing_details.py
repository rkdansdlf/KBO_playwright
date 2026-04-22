import asyncio
import sys
import os
from datetime import datetime
import sqlite3

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.repositories.game_repository import save_game_detail

async def crawl_2024_missing():
    conn = sqlite3.connect("data/kbo_dev.db")
    cursor = conn.cursor()
    
    # Identify 2024 games missing batting stats
    query = """
    SELECT g.game_id, g.game_date
    FROM game g
    WHERE strftime('%Y', g.game_date) = '2024'
      AND (SELECT COUNT(*) FROM game_batting_stats b WHERE b.game_id = g.game_id) = 0
      AND g.game_status NOT IN ('경기취소', '취소', '우천취소')
    """
    
    targets = cursor.execute(query).fetchall()
    conn.close()

    if not targets:
        print("No missing games found for 2024.")
        return

    print(f"Found {len(targets)} games in 2024 to crawl.")
    
    games_to_crawl = []
    for game_id, game_date in targets:
        date_str = str(game_date).replace("-", "")
        games_to_crawl.append({"game_id": game_id, "game_date": date_str})

    chunk_size = 50
    crawler = GameDetailCrawler(request_delay=1.0)
    
    success_count = 0
    for i in range(0, len(games_to_crawl), chunk_size):
        chunk = games_to_crawl[i:i+chunk_size]
        print(f"Processing 2024 chunk {i//chunk_size + 1}/{(len(games_to_crawl)-1)//chunk_size + 1} ({len(chunk)} games)...")
        
        results = await crawler.crawl_games(chunk)
        
        for payload in results:
            if payload:
                if save_game_detail(payload):
                    success_count += 1
                else:
                    print(f"  ❌ Failed to save detail for {payload['game_id']}")

    print(f"\n2024 Crawl Complete: {success_count}/{len(targets)} games saved.")

if __name__ == "__main__":
    asyncio.run(crawl_2024_missing())
