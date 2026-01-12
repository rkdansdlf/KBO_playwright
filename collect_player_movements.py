
import asyncio
import argparse
from datetime import datetime

from src.crawlers.player_movement_crawler import PlayerMovementCrawler
from src.repositories.player_repository import PlayerRepository
from src.utils.safe_print import safe_print as print

async def main():
    parser = argparse.ArgumentParser(description="Collect KBO Player Movements (Trade, FA, etc.)")
    parser.add_argument("--start", type=int, default=2017, help="Start Year (default: 2017)")
    parser.add_argument("--end", type=int, default=datetime.now().year + 1, help="End Year (default: Next Year)")
    
    args = parser.parse_args()
    
    print(f"🚀 Starting Player Movement Collection: {args.start} ~ {args.end}")
    
    crawler = PlayerMovementCrawler()
    repo = PlayerRepository()
    
    # Iterate year by year to save incrementally
    for year in range(args.start, args.end + 1):
        try:
            data = await crawler.crawl_years(year, year)
            if data:
                saved = repo.save_player_movements(data)
                print(f"💾 Saved {saved} records for {year}.")
            else:
                print(f"ℹ️ No records found for {year}.")
        except Exception as e:
            print(f"❌ Error collecting {year}: {e}")
            
    print("✅ Collection Finished.")

if __name__ == "__main__":
    asyncio.run(main())
