
"""
Player Profile Enrichment CLI
Identifies players with missing basic info (e.g. birth_date, debut_year) and crawls them.
"""
import asyncio
import argparse
from typing import List, Set

from sqlalchemy import select, or_

from src.db.engine import SessionLocal
from src.models.player import Player
from src.crawlers.player_profile_crawler import PlayerProfileCrawler
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.repositories.player_repository import PlayerRepository
from src.parsers.player_profile_parser import PlayerProfileParsed
from src.utils.safe_print import safe_print as print

async def collect_missing_profiles(limit: int = 100):
    session = SessionLocal()
    repo = PlayerRepository()
    pool = AsyncPlaywrightPool(max_pages=1)
    crawler = PlayerProfileCrawler(request_delay=1.2, pool=pool)

    try:
        stmt = select(Player).where(
            or_(
                Player.birth_date == None,
                Player.debut_year == None
            )
        ).limit(limit)
        
        target_players = session.execute(stmt).scalars().all()
        print(f"🎯 Found {len(target_players)} players with missing config (Limit: {limit})")
        
        if not target_players:
            print("✅ All players seem to have profiles!")
            return

        async with pool:
            for idx, player in enumerate(target_players, 1):
                pid = player.kbo_person_id
                if not pid:
                    continue

                print(f"[{idx}/{len(target_players)}] Crawling profile for {pid}")

                data = await crawler.crawl_player_profile(str(pid))
                if data and data.get('raw_text'):
                    # Use the standardized parser to extract all fields from raw_text
                    from src.parsers.player_profile_parser import parse_profile
                    
                    parsed = parse_profile(data['raw_text'])
                    parsed.player_id = int(pid) if pid.isdigit() else None
                    
                    # Store original values from crawler as well if needed
                    parsed.photo_url = data.get('photo_url')
                    parsed.salary_original = data.get('salary_original')
                    parsed.signing_bonus_original = data.get('signing_bonus_original')
                    parsed.draft_info = data.get('draft_info')

                    repo.upsert_player_profile(str(pid), parsed)
                    
                    print(f"   ✅ Saved profile for {parsed.player_name}")
                else:
                    print(f"   ⚠️  Crawl skipped or no data (Stub/Retired) for {pid}")

                if idx % 10 == 0:
                    await asyncio.sleep(1)

    except Exception as e:
        print(f"❌ Critical Error: {e}")
    finally:
        session.close()

def main():
    parser = argparse.ArgumentParser(description="Collect Missing Player Profiles")
    parser.add_argument("--limit", type=int, default=1000, help="Max players to process")
    args = parser.parse_args()
    
    asyncio.run(collect_missing_profiles(args.limit))

if __name__ == "__main__":
    main()
