
import asyncio
import json
import os
import sys
from typing import List, Dict

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.crawlers.schedule_crawler import ScheduleCrawler

async def main():
    crawler = ScheduleCrawler(request_delay=1.0)
    all_games = []
    
    # 2002 to 2007
    years = range(2002, 2008) 
    
    for year in years:
        print(f"\n📅 Crawling Schedule for {year}...")
        # Regular Season usually March to October/November
        # Let's crawl 3 to 11
        # Pass series_id="0,9,6" to ensure Regular Season games are found (especially for pre-2010)
        games = await crawler.crawl_season(year, months=list(range(3, 12)), series_id="0,9,6")
        print(f"   found {len(games)} games for {year}")
        all_games.extend(games)
        
    print(f"\n✅ Total games found: {len(all_games)}")
    
    output_path = 'data/historical_game_ids_02_07.json'
    os.makedirs('data', exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_games, f, ensure_ascii=False, indent=2)
        
    print(f"📝 Saved to {output_path}")

if __name__ == "__main__":
    asyncio.run(main())
