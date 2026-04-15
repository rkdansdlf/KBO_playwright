
import asyncio
import os
import sys
from datetime import datetime, timedelta
from typing import List

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.crawlers.player_movement_crawler import PlayerMovementCrawler
from src.crawlers.daily_roster_crawler import DailyRosterCrawler
from src.repositories.player_repository import PlayerRepository
from src.repositories.team_repository import TeamRepository
from src.db.engine import SessionLocal
from src.utils.playwright_pool import AsyncPlaywrightPool

async def backfill_player_movements(years: List[int]):
    print(f"🔄 Starting Player Movement Backfill for years: {years}...")
    crawler = PlayerMovementCrawler()
    repo = PlayerRepository()
    
    for year in years:
        try:
            movements = await crawler.crawl_years(year, year)
            if movements:
                count = repo.save_player_movements(movements)
                print(f"  ✅ Saved {count} movements for {year}")
        except Exception as e:
            print(f"  ❌ Error for {year}: {e}")

async def backfill_daily_rosters(start_date_str: str, end_date_str: str):
    print(f"📅 Starting Daily Roster Backfill from {start_date_str} to {end_date_str}...")
    
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    pool = AsyncPlaywrightPool(max_pages=1, headless=True)
    await pool.start()
    
    crawler = DailyRosterCrawler(pool=pool)
    
    current_date = start_date
    while current_date <= end_date:
        d_str = current_date.strftime("%Y-%m-%d")
        try:
            # We crawl one day at a time to manage persistence more reliably
            roster = await crawler.crawl_date_range(d_str, d_str)
            if roster:
                with SessionLocal() as session:
                    repo = TeamRepository(session)
                    count = repo.save_daily_rosters(roster)
                    print(f"  ✅ Saved {count} roster records for {d_str}")
        except Exception as e:
            print(f"  ❌ Error for {d_str}: {e}")
            
        current_date += timedelta(days=1)
    
    await pool.close()

async def main():
    # 1. Backfill Movements (2024-2026)
    await backfill_player_movements([2024, 2025, 2026])
    
    # 2. Backfill Daily Rosters
    # Let's start with a sample period: 2024 Regular Season (roughly March to Oct)
    # And 2026 early season (March to April)
    await backfill_daily_rosters("2024-03-23", "2024-10-31")
    await backfill_daily_rosters("2026-03-20", "2026-04-14")

if __name__ == "__main__":
    asyncio.run(main())
