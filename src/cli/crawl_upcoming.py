"""
Upcoming Schedule Crawler
Crawls schedule for the current month and the next month to ensure future games are populated in the DB.
"""
import argparse
import asyncio
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List, Dict

from src.crawlers.schedule_crawler import ScheduleCrawler
from src.repositories.game_repository import save_schedule_game
from src.utils.safe_print import safe_print as print

async def crawl_upcoming(args: argparse.Namespace) -> None:
    """Crawl upcoming months."""
    crawler = ScheduleCrawler(request_delay=args.delay)
    
    # Determine months to crawl
    now = datetime.now()
    targets = []
    
    if args.year and args.months:
        # Manual mode
        y = int(args.year)
        ms = [int(m.strip()) for m in str(args.months).split(',')]
        for m in ms:
            targets.append((y, m))
    else:
        # Auto mode: Current month + Next month
        targets.append((now.year, now.month))
        
        next_month_date = now + relativedelta(months=1)
        targets.append((next_month_date.year, next_month_date.month))
    
    print(f"🚀 Starting upcoming schedule crawl for: {targets}")
    
    total_new = 0
    
    for year, month in targets:
        print(f"\\n📅 Crawling {year}-{month:02d}...")
        games = await crawler.crawl_schedule(year, month)
        
        # Filter for future or today?
        # Actually, for schedule updates, we might as well save everything returned 
        # to ensure time/stadium changes are reflected even for near-past games if needed.
        # But user request focused on "upcoming".
        
        # Let's save all found games for the month. Upsert handles duplicates.
        for game in games:
            saved = save_schedule_game(game)
            if saved:
                total_new += 1
                
        print(f"   => Processed {len(games)} games for {month}월")

    print(f"\\n✅ Completed. Processed {total_new} updates (upserts).")


def main():
    parser = argparse.ArgumentParser(description="KBO Upcoming Schedule Crawler")
    parser.add_argument("--year", type=int, help="Target Year")
    parser.add_argument("--months", type=str, help="Target Months (comma separated)")
    parser.add_argument("--delay", type=float, default=1.2, help="Request delay")
    
    args = parser.parse_args()
    asyncio.run(crawl_upcoming(args))

if __name__ == "__main__":
    main()
