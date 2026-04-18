#!/usr/bin/env python3
"""
Backfill missing game metadata (2001-2010).
Extracts unique game_ids that exist in stats tables but are missing from the 'game' table,
and fetches their detail/metadata from the KBO website.
"""
import asyncio
import sys
from pathlib import Path
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.repositories.game_repository import save_game_detail
from src.db.engine import SessionLocal

async def backfill_orphan_games(limit: int = 100):
    print(f"Finding orphan games (limit {limit})...")
    
    with SessionLocal() as session:
        # Get missing IDs from batting stats (most comprehensive)
        query = text("""
            SELECT DISTINCT game_id 
            FROM game_batting_stats 
            WHERE game_id NOT IN (SELECT game_id FROM game)
            ORDER BY game_id ASC
            LIMIT :limit
        """)
        missing = session.execute(query, {"limit": limit}).fetchall()
        
    if not missing:
        print("No orphan games found.")
        return

    print(f"Found {len(missing)} orphan games. Starting crawl...")
    
    crawler = GameDetailCrawler()
    success_count = 0
    
    for (game_id,) in missing:
        # Extract date from game_id (YYYYMMDD)
        date_str = game_id[:8]
        print(f"📡 Crawling {game_id} ({date_str})...")
        
        try:
            # We use lightweight=True because we mainly need metadata to fix the 'game' table relationship.
            payload = await crawler.crawl_game(game_id, date_str, lightweight=True)
            
            if payload:
                # Save to database
                if save_game_detail(payload):
                    print(f"✅ Saved metadata for {game_id}")
                    success_count += 1
                else:
                    print(f"❌ Failed to save {game_id}")
            else:
                print(f"⚠️  No payload returned for {game_id}")
                
        except Exception as e:
            print(f"❌ Error for {game_id}: {e}")
            
        # Polite delay
        await asyncio.sleep(1.0)

    print(f"\nFinished batch. Success: {success_count}/{len(missing)}")

if __name__ == "__main__":
    # Default limit to 10 for safety in this turn, can be increased later
    limit_arg = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    asyncio.run(backfill_orphan_games(limit_arg))
