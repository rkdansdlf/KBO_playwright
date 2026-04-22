
"""
Player Profile Enrichment CLI
Identifies players with missing basic info (e.g. birth_date, debut_year) and crawls them.
"""
import asyncio
import argparse
from typing import List, Optional

from sqlalchemy import select, or_

from src.db.engine import SessionLocal
from src.models.player import Player
from src.crawlers.player_profile_crawler import PlayerProfileCrawler
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.repositories.player_repository import PlayerRepository
from src.utils.safe_print import safe_print as print

async def collect_profiles(limit: int = 100, target_ids: Optional[List[str]] = None):
    session = SessionLocal()
    repo = PlayerRepository()
    pool = AsyncPlaywrightPool(max_pages=1)
    crawler = PlayerProfileCrawler(request_delay=1.5, pool=pool)

    try:
        if target_ids:
            stmt = select(Player).where(Player.kbo_person_id.in_(target_ids))
            print(f"🎯 Targeted processing for {len(target_ids)} IDs")
        else:
            stmt = select(Player).where(
                or_(
                    Player.birth_date == None,
                    Player.debut_year == None
                )
            ).limit(limit)
        
        target_players = session.execute(stmt).scalars().all()
        
        if not target_players:
            print("✅ No matching players found for profile collection.")
            return

        print(f"🎯 Processing {len(target_players)} player profiles...")

        async with pool:
            for idx, player in enumerate(target_players, 1):
                pid = player.kbo_person_id
                if not pid:
                    continue

                print(f"[{idx}/{len(target_players)}] Crawling profile for {pid} ({getattr(player, 'name_kor', 'Unknown')})")

                data = await crawler.crawl_player_profile(str(pid))
                if data:
                    print(f"   ✅ Fetched profile for {pid}")
                    # Use PlayerProfileParsed for repository compatibility
                    from src.parsers.player_profile_parser import PlayerProfileParsed
                    parsed = PlayerProfileParsed(
                        player_id=int(pid) if pid.isdigit() else None,
                        photo_url=data.get('photo_url'),
                        batting_hand=data.get('bats'),
                        throwing_hand=data.get('throws'),
                        entry_year=data.get('debut_year'),
                        salary_original=data.get('salary_original'),
                        signing_bonus_original=data.get('signing_bonus_original'),
                        draft_info=data.get('draft_info')
                    )
                    
                    # Update name if available in crawler raw data (optional)
                    # For now, repo.upsert_player_profile will handle the merge.
                    
                    repo.upsert_player_profile(str(pid), parsed)
                    print(f"   ✅ Saved profile metadata for {pid}")
                else:
                    print(f"   ⚠️  Crawl skipped or no data for {pid}")

                if idx % 5 == 0:
                    await asyncio.sleep(1)

    except Exception as e:
        print(f"❌ Critical Error: {e}")
    finally:
        session.close()

def main():
    parser = argparse.ArgumentParser(description="Collect Missing Player Profiles")
    parser.add_argument("--limit", type=int, default=1000, help="Max players to process")
    parser.add_argument("--ids", type=str, help="Comma-separated List of KBO Player IDs")
    args = parser.parse_args()
    
    target_ids = None
    if args.ids:
        target_ids = [i.strip() for i in args.ids.split(",")]
    
    asyncio.run(collect_profiles(args.limit, target_ids))

if __name__ == "__main__":
    main()
