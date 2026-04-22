import asyncio
import sys
import os
import sqlite3

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.repositories.game_repository import save_game_detail

async def flawless_crawl():
    with open("scratch/flawless_crawl_list.txt", "r") as f:
        lines = f.readlines()
    
    games_to_crawl = []
    for line in lines:
        if not line.strip(): continue
        gid, gdate = line.strip().split("|")
        games_to_crawl.append({"game_id": gid, "game_date": gdate.replace("-", "")})

    print(f"🚀 Starting FLAWLESS CRAWL for {len(games_to_crawl)} games...")
    
    # Process in larger chunks with verification
    chunk_size = 30
    crawler = GameDetailCrawler(request_delay=1.2)
    
    success_count = 0
    fail_count = 0
    
    for i in range(0, len(games_to_crawl), chunk_size):
        chunk = games_to_crawl[i:i+chunk_size]
        print(f"📦 Progress: {i}/{len(games_to_crawl)} games. Current Success: {success_count}")
        
        results = await crawler.crawl_games(chunk)
        
        for payload in results:
            if payload:
                if save_game_detail(payload):
                    success_count += 1
                else:
                    fail_count += 1
            else:
                fail_count += 1

    print(f"\n✅ FLAWLESS CRAWL COMPLETE!")
    print(f"Total Success: {success_count}")
    print(f"Total Failed/Skipped (Integrity Error): {fail_count}")

if __name__ == "__main__":
    asyncio.run(flawless_crawl())
