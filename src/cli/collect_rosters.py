
"""
Daily Roster Collector CLI
"""
import asyncio
import argparse
from datetime import date, timedelta
from src.crawlers.daily_roster_crawler import DailyRosterCrawler
from src.repositories.team_repository import TeamRepository
from src.utils.safe_print import safe_print as print

from src.db.engine import SessionLocal

def save_chunk(chunk):
    session = SessionLocal()
    try:
        repo = TeamRepository(session)
        count = repo.save_daily_rosters(chunk)
        print(f"   💾 Saved chunk of {len(chunk)} records (New/Updated: {count})")
    except Exception as e:
        print(f"   ⚠️ Error saving chunk: {e}")
    finally:
        session.close()

async def collect_rosters(year: int, month: int = None):
    crawler = DailyRosterCrawler()
    
    # Define date range
    if month:
        start_date = date(year, month, 1)
        # End date: start of next month - 1 day
        if month == 12:
            end_date = date(year, 12, 31)
        else:
            end_date = date(year, month + 1, 1) - timedelta(days=1)
    else:
        # Full year (Regular season roughly Mar-Nov)
        start_date = date(year, 3, 1)
        end_date = date(year, 11, 30)
    
    print(f"🗓️  Collecting Daily Rosters: {start_date} ~ {end_date}")
    
    data = await crawler.crawl_date_range(
        start_date=start_date.strftime("%Y-%m-%d"), 
        end_date=end_date.strftime("%Y-%m-%d"),
        save_callback=save_chunk
    )
    
    print(f"✅ Finished Roster Collection for {year}" + (f"-{month}" if month else ""))

def main():
    parser = argparse.ArgumentParser(description="Collect Daily Rosters")
    parser.add_argument("--year", type=int, required=True, help="Target Year")
    parser.add_argument("--month", type=int, help="Target Month (Optional)")
    args = parser.parse_args()
    
    asyncio.run(collect_rosters(args.year, args.month))

if __name__ == "__main__":
    main()
