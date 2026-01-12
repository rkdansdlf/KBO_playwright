"""
Script to collect daily 1st team roster status.
Usage:
    python collect_daily_rosters.py --start 2024-03-23 --end 2024-10-01
    python collect_daily_rosters.py --date 20240520
"""
import argparse
import asyncio
from datetime import datetime, timedelta

from src.db.engine import SessionLocal as get_db_session
from src.repositories.team_repository import TeamRepository
from src.crawlers.daily_roster_crawler import DailyRosterCrawler

async def collect(start_date: str, end_date: str):
    session = get_db_session()
    repo = TeamRepository(session)
    crawler = DailyRosterCrawler()
    
    print(f"🚀 Starting Roster Collection: {start_date} ~ {end_date}")
    
    # Define callback for incremental saving
    def save_chunk(roster_data):
        if roster_data:
            count = repo.save_daily_rosters(roster_data)
            print(f"   Saved {count} records.", flush=True)
            
    try:
        data = await crawler.crawl_date_range(start_date, end_date, save_callback=save_chunk)
        print(f"✅ Roster Collection Finished. Total records processed: {len(data)}")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        session.close()

def main():
    parser = argparse.ArgumentParser(description="Collect KBO Daily Roster")
    parser.add_argument("--start", type=str, help="Start Date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End Date (YYYY-MM-DD)")
    parser.add_argument("--date", type=str, help="Single Date (YYYYMMDD or YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    s_date = None
    e_date = None
    
    if args.date:
        d_str = args.date.replace("-", "")
        # format: YYYYMMDD -> YYYY-MM-DD for crawler input
        formatted = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
        s_date = formatted
        e_date = formatted
    elif args.start:
        s_date = args.start
        e_date = args.end if args.end else args.start
    else:
        # Default to yesterday
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        s_date = yesterday
        e_date = yesterday
        
    asyncio.run(collect(s_date, e_date))

if __name__ == "__main__":
    main()
