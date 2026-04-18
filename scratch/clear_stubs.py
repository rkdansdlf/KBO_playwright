
import asyncio
import sys
import os

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.crawlers.player_profile_crawler import PlayerProfileCrawler
from src.repositories.player_basic_repository import PlayerBasicRepository
from src.db.engine import SessionLocal
from src.models.player import PlayerBasic
from src.utils.playwright_pool import AsyncPlaywrightPool

async def clear_stubs():
    repo = PlayerBasicRepository()
    with SessionLocal() as session:
        # Target players in the 900000 range which are known stubs
        targets = session.query(PlayerBasic).filter(
            PlayerBasic.player_id >= 900000,
            PlayerBasic.photo_url == None
        ).all()
    
    if not targets:
        print("No stubs to clear.")
        return

    print(f"Clearing {len(targets)} stub profiles...")
    pool = AsyncPlaywrightPool(max_pages=1)
    await pool.start()
    crawler = PlayerProfileCrawler(request_delay=0.1, pool=pool)
    
    try:
        for i, p in enumerate(targets):
            print(f"[{i+1}/{len(targets)}] Checking {p.player_id}...")
            profile = await crawler.crawl_player_profile(str(p.player_id))
            if not profile:
                print(f"  -> Marking {p.player_id} as NOT_FOUND")
                repo.upsert_players([{
                    'player_id': p.player_id,
                    'name': p.name,
                    'photo_url': 'NOT_FOUND'
                }])
            else:
                profile['name'] = p.name
                repo.upsert_players([profile])
                print(f"  -> Found data for {p.player_id}")
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(clear_stubs())
