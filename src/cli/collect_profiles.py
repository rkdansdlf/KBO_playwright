
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

                print(f"[{idx}/{len(target_players)}] Crawling profile for {pid} (Name: {player.player_name or '?'})")

                data = await crawler.crawl_player_profile(str(pid))
                if data:
                    # Map Crawler Dict -> PlayerProfileParsed
                    basic = data.get('basic_info', {})
                    phys = data.get('physical_info', {})
                    career = data.get('career_info', {})

                    parsed = PlayerProfileParsed(
                        player_id=int(pid) if pid.isdigit() else None,
                        player_name=basic.get('name'),
                        back_number=int(basic['back_number']) if basic.get('back_number') and basic['back_number'].isdigit() else None,
                        birth_date=basic.get('birth_date'), # Assuming format matches or needs parsing?
                        position=basic.get('position'),
                        height_cm=int(phys['height']) if phys.get('height') else None,
                        weight_kg=int(phys['weight']) if phys.get('weight') else None,
                        batting_hand=phys.get('bat_hand'),
                        throwing_hand=phys.get('throw_hand'),
                        entry_year=int(career['debut_year']) if career.get('debut_year') and career['debut_year'].isdigit() else None,
                        # education_or_career_path is not easily mapped from summary text in crawler
                    )

                    # Normalize birth_date if needed (Crawler returns 1999년 01월 01일?)
                    # Repository expects YYYY-MM-DD string or Date object.
                    # Repository _apply_profile_fields parses YYYY-MM-DD.
                    # Let's hope basic['birth_date'] is clean or fix it.
                    if parsed.birth_date and '년' in parsed.birth_date:
                        import re
                        match = re.search(r"(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일", parsed.birth_date)
                        if match:
                            parsed.birth_date = f"{match.group(1)}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"

                    repo.upsert_player_profile(str(pid), parsed)
                    print(f"   ✅ Saved profile for {parsed.player_name}")
                else:
                    print(f"   ❌ Crawl failed or no data")

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
