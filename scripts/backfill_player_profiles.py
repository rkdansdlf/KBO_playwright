"""
Backfill Player Profiles
Collects missing photo_url, salary, draft_info, etc. for existing players in player_basic table.
Usage: python3 scripts/backfill_player_profiles.py --limit 10 --delay 2.0
"""
import asyncio
import argparse
import sys
import os

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.crawlers.player_profile_crawler import PlayerProfileCrawler
from src.repositories.player_basic_repository import PlayerBasicRepository
from src.db.engine import SessionLocal
from src.models.player import PlayerBasic
from src.utils.playwright_pool import AsyncPlaywrightPool

async def backfill(limit: int, delay: float):
    repo = PlayerBasicRepository()
    
    # 1. Target players: missing photo_url
    with SessionLocal() as session:
        query = session.query(PlayerBasic).filter(PlayerBasic.photo_url == None)
        if limit > 0:
            query = query.limit(limit)
        targets = query.all()
    
    if not targets:
        print("✅ No players need backfilling.")
        return

    print(f"🚀 Starting backfill for {len(targets)} players (delay={delay}s)...")
    
    # Reuse a single pool for efficiency
    pool = AsyncPlaywrightPool(max_pages=1)
    await pool.start()
    crawler = PlayerProfileCrawler(request_delay=delay, pool=pool)
    
    success_count = 0
    fail_count = 0
    
    try:
        for i, p in enumerate(targets):
            print(f"[{i+1}/{len(targets)}] Processing {p.name} ({p.player_id})...")
            
            try:
                profile = await crawler.crawl_player_profile(
                    str(p.player_id), 
                    position=p.position
                )
                
                if profile:
                    # Fix: upsert_players requires 'name'
                    profile['name'] = p.name
                    # Update DB
                    repo.upsert_players([profile])
                    print(f"  ✅ Updated: photo={profile['photo_url']}, salary={profile['salary_original']}")
                    success_count += 1
                else:
                    print(f"  ⚠️ No profile found for {p.player_id}")
                    fail_count += 1
            except Exception as e:
                print(f"  ❌ Error processing {p.player_id}: {e}")
                fail_count += 1
            
            # Additional safety delay (on top of crawler's internal delay if needed)
            if i < len(targets) - 1:
                await asyncio.sleep(delay)
                
    finally:
        await pool.close()

    print(f"\n✨ Backfill complete!")
    print(f"   - Success: {success_count}")
    print(f"   - Failed:  {fail_count}")

def main():
    parser = argparse.ArgumentParser(description="Backfill missing player profile details")
    parser.add_argument("--limit", type=int, default=0, help="Number of players to process (0 = all)")
    parser.add_argument("--delay", type=float, default=1.5, help="Delay between requests in seconds")
    
    args = parser.parse_args()
    
    asyncio.run(backfill(args.limit, args.delay))

if __name__ == "__main__":
    main()
