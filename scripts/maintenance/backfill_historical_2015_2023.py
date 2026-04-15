
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

async def backfill_historical_movements(start_year: int, end_year: int):
    """2015-2023 선수 이동 현황 수집"""
    print(f"🔄 Starting Historical Player Movement Backfill ({start_year}-{end_year})...")
    crawler = PlayerMovementCrawler()
    repo = PlayerRepository()
    
    # 9년치 데이터를 한꺼번에 수집
    try:
        movements = await crawler.crawl_years(start_year, end_year)
        if movements:
            count = repo.save_player_movements(movements)
            print(f"  ✅ Saved {count} historical movements to SQLite.")
    except Exception as e:
        print(f"  ❌ Error during historical movements backfill: {e}")

async def backfill_historical_rosters(year: int, start_month: int = 3, end_month: int = 10):
    """특정 연도의 시즌 기간(3~10월) 엔트리 수집"""
    print(f"📅 Starting Historical Roster Backfill for {year} (Months {start_month}-{end_month})...")
    
    # 해당 연도의 대략적인 개막일부터 종료일까지
    import calendar
    last_day = calendar.monthrange(year, end_month)[1]
    start_date = date(year, start_month, 20) 
    end_date = date(year, end_month, last_day)
    
    pool = AsyncPlaywrightPool(max_pages=1, headless=True)
    await pool.start()
    
    crawler = DailyRosterCrawler(pool=pool)
    
    current_date = start_date
    while current_date <= end_date:
        d_str = current_date.strftime("%Y-%m-%d")
        try:
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

from datetime import date

async def main():
    # 1. 2015-2023 선수 이동 현황 백필
    await backfill_historical_movements(2015, 2023)
    
    # 2. 2023년 데이터 샘플 백필 (전체 9년치는 시간이 너무 오래 걸리므로 우선 최근 연도부터)
    # 실제 운영 시에는 연도별로 병렬 또는 순차적으로 실행 권장
    await backfill_historical_rosters(2023, 4, 4) # 2023년 4월 한 달 샘플 수집

if __name__ == "__main__":
    asyncio.run(main())
